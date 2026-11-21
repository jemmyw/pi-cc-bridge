/** Isolated, opt-in live provider smoke. Never sends to an unrelated session. */
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { createRequire } from "node:module";
import { BrokerClient, type BrokerEvent } from "../src/intercom/client.ts";
import { ccSessionsDir } from "../src/cc/paths.ts";
import { readRecordFile, isPidAlive } from "../src/cc/registry.ts";
import { ClaudeJobs } from "../src/claude-jobs/provider.ts";
import { loadProviderApi, registerClaudeJobs } from "../extensions/claude-jobs.ts";

const nonce = randomUUID();
const root = mkdtempSync(join(tmpdir(), "claude-jobs-live-"));
const fixture = join(root, "fixture");
const parentId = randomUUID();
const funnelName = `pi-job-smoke-${nonce}`;
const children: ChildProcess[] = [];
const parent = new BrokerClient(root);
let jobs: ClaudeJobs | undefined;
let dispose: (() => void) | undefined;
let handleId: string | undefined;
const messages: Extract<BrokerEvent, { type: "message" }>[] = [];
const req = createRequire(import.meta.url);
const tsx = req.resolve("tsx");
const intercom = process.env.PI_INTERCOM_DIR;
if (!intercom) throw new Error("Set PI_INTERCOM_DIR to the installed pi-intercom directory");
async function wait(label: string, predicate: () => boolean, ms = 15_000) {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    if (predicate()) return;
    await sleep(100);
  }
  throw new Error(`Timed out: ${label}`);
}
const launch = (args: string[]) => {
  const child = spawn(process.execPath, args, { cwd: fixture,
    env: { ...process.env, PI_CODING_AGENT_DIR: root }, stdio: "ignore" });
  children.push(child);
  return child;
};
try {
  mkdirSync(fixture);
  // A new private Git fixture is not a worktree of the shared implementation checkout.
  const git = spawn("git", ["init", "-q", fixture], { stdio: "ignore" });
  await new Promise<void>((done, reject) => { git.once("error", reject); git.once("exit", code => code === 0 ? done() : reject(new Error("git init failed"))); });
  mkdirSync(join(root, "pi-cc-bridge"));
  writeFileSync(join(root, "pi-cc-bridge/config.json"), JSON.stringify({ peerName: funnelName, pollMs: 500,
    expose: [], maxHolders: 1, logLevel: "quiet" }), { mode: 0o600 });
  launch(["--import", tsx, join(intercom!, "broker/broker.ts")]);
  await wait("isolated broker", () => existsSync(join(root, "intercom/broker.sock")));
  const daemon = launch(["--import", tsx, resolve("src/cli.ts")]);
  await wait("verified funnel", () => readRecordFile(join(ccSessionsDir(), `${daemon.pid}.json`))?.name === funnelName);
  parent.on("error", () => {});
  parent.on("message", message => messages.push(message));
  await parent.connectAndRegister(parentId, { cwd: fixture, pid: process.pid, model: "provider-live-parent",
    startedAt: Date.now(), lastActivity: Date.now() });
  jobs = new ClaudeJobs({ id: parentId, cwd: fixture, agentDir: root, parentTarget: parentId });
  const profile = process.env.PI_CODING_AGENT_DIR;
  if (!profile) throw new Error("Run from an explicit active Pi profile (PI_CODING_AGENT_DIR)");
  dispose = registerClaudeJobs(await loadProviderApi(profile), jobs);
  const question = `DECISION ${nonce}`;
  const answer = `APPROVED ${nonce}`;
  const output = join(fixture, "owned-proof.txt");
  const prompt = `Only work in this private fixture ${fixture}. Do not edit any other path. First ask the bound parent via the funnel exactly '${question}: may I write the proof file?'. Do not write until the parent replies '${answer}'. End the waiting turn using the prescribed WAIT protocol. After receiving the actual cross-session answer, write owned-proof.txt containing exactly '${answer}', read it back and complete using the prescribed DONE protocol. This smoke must use native SendMessage for the question and a native cross-session reply for the answer, not stdin steering. Ignore any unrelated messages.`;
  const handle = await jobs.start({ sessionId: parentId, prompt, promptDigest: createHash("sha256").update(prompt).digest("hex"),
    cwd: fixture, runId: randomUUID(), stepIndex: 0, agent: "claude-job-worker", options: { scope: fixture, timeoutMs: 180_000 } });
  handleId = handle.providerJobId;
  assert.equal(handle.state, "running", JSON.stringify(handle));
  const expectedName = `claude:claude-job-${handleId}`;
  await wait("native worker question to exact parent before first outbound contact", () => messages.some(message =>
    message.from.name === expectedName && message.from.id.startsWith("cc-") && message.message.content.text.includes(question)), 120_000);
  const questionEvent = messages.find(message => message.from.name === expectedName && message.message.content.text.includes(question))!;
  assert.equal(existsSync(output), false, "worker mutated before parent decision");
  assert.equal(jobs.status(handleId).state, "running", "intermediate result must not complete job");
  parent.send(questionEvent.from.id, { id: randomUUID(), timestamp: Date.now(), content: { text: answer } });
  await wait("continued native answer and explicit completion", () => jobs!.status(handleId!).state !== "running", 120_000);
  assert.equal(jobs.status(handleId).state, "completed", JSON.stringify(jobs.status(handleId)));
  assert.equal(readFileSync(output, "utf8").trim(), answer);
  console.log("PASS native question -> exact parent answer -> retained-context edit -> explicit provider completion; expose=[]; no stdin answer");
} finally {
  jobs?.close(); dispose?.(); parent.close();
  // Allow the provider guardian to reap its own process group before stopping the bridge.
  await sleep(700);
  for (const child of children.reverse()) {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    await wait("owned child exit", () => child.exitCode !== null || child.signalCode !== null, 5000);
    const recordPath = join(ccSessionsDir(), `${child.pid}.json`);
    const record = readRecordFile(recordPath);
    if (record && !isPidAlive(record.pid) && record.bridgeOwner === "pi-cc-bridge" && record.name === funnelName) rmSync(recordPath);
  }
  rmSync(root, { recursive: true, force: true });
}
