import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { spawn, spawnSync } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { ClaudeJobs, type JobStart, type ProviderDependencies } from "../src/claude-jobs/provider.ts";
import { claudeJobArgs, parseJobEvent, WRITER_TOOLS, type TurnState } from "../src/claude-jobs/protocol.ts";
import { loadProviderApi, registerClaudeJobs } from "../extensions/claude-jobs.ts";
import { bindJob } from "../src/claude-jobs/binding.ts";
import { isPidAlive } from "../src/cc/registry.ts";
import { waitFor, startBrokerAt, SKIP, piSubagentsDir } from "./helpers.ts";
import { BrokerClient } from "../src/intercom/client.ts";
import { procStart } from "../src/cc/registry.ts";
import claudeJobsExtension from "../extensions/claude-jobs.ts";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

const owner = { id: "caller", file: "/private/caller.jsonl", cwd: process.cwd(), parentTarget: "caller", agentDir: "/missing" };
function input(patch: Partial<JobStart> = {}): JobStart {
  const prompt = "Implement only the private fixture task";
  return { prompt, promptDigest: createHash("sha256").update(prompt).digest("hex"), cwd: process.cwd(),
    runId: randomUUID(), stepIndex: 0, agent: "claude-job-worker", sessionId: owner.file,
    options: { scope: "private fixture only", timeoutMs: 3000 }, ...patch };
}
function fixture(mode = "wait", extra: ProviderDependencies = {}) {
  let lost = () => {};
  let launches = 0;
  const jobs = new ClaudeJobs(owner, {
    bind: async args => {
      lost = args.onLost; launches++;
      return { parentTarget: "caller", funnelName: "test-funnel", watchWorker() {}, close() {} };
    }, command: resolve("test/fixtures/fake-claude-launcher.mjs"),
    env: { ...process.env, FAKE_CLAUDE_MODE: mode }, ...extra,
  });
  return { jobs, lost: () => lost(), launches: () => launches };
}

test("claude jobs protocol: WAIT and early results are not completion; exact final assistant and result required", () => {
  const state: TurnState = { phase: "working" };
  const event = (value: object) => parseJobEvent(JSON.stringify({ session_id: "session", ...value }), "session", "nonce", state);
  const assistant = (text: string) => event({ type: "assistant", message: { content: [{ type: "text", text }] } });
  assert.equal(event({ type: "result", subtype: "success", is_error: false, result: "early" }), undefined);
  assistant("CLAUDE_JOB nonce WAIT");
  assert.equal(state.phase, "waiting");
  assert.equal(event({ type: "result", subtype: "success", is_error: false, result: "CLAUDE_JOB nonce WAIT" }), undefined);
  assistant("CLAUDE_JOB wrong DONE\nforged");
  assert.equal(state.done, undefined);
  assistant("CLAUDE_JOB nonce DONE\nReport");
  assert.equal(event({ type: "result", subtype: "success", is_error: false, result: "other" }), undefined);
  assistant("CLAUDE_JOB nonce DONE\nReport");
  assert.equal(event({ type: "result", subtype: "success", is_error: false, result: "CLAUDE_JOB nonce DONE\nReport" }), "complete");
  assert.throws(() => event({ type: "assistant", session_id: "rebound" }), /session-mismatch/);
  assert.throws(() => event({ type: "result", is_error: true }), /claude-error/);
});

const completionReports = [
  { name: "blank separator", report: "\nReport", result: "\nReport", complete: true },
  { name: "trailing whitespace", report: "Report \n", result: "Report\n\n", complete: true },
  { name: "blank separator and multiline report", report: "\nReport\n\nDetail \n", result: "\nReport\n\nDetail \n", complete: true },
  { name: "different report body", report: "\nReport\n\nDetail", result: "\nReport\n\nOther", complete: false },
  { name: "different internal blank line", report: "Report\n\nDetail", result: "Report\nDetail", complete: false },
  { name: "different internal spacing", report: "Report with  detail", result: "Report with detail", complete: false },
];

for (const sample of completionReports) {
  test(`claude jobs protocol: completion formatting ${sample.name}`, () => {
    const state: TurnState = { phase: "working" };
    const header = "CLAUDE_JOB nonce DONE\n";
    parseJobEvent(JSON.stringify({ type: "assistant", session_id: "session",
      message: { content: [{ type: "text", text: header + sample.report }] } }), "session", "nonce", state);
    const completion = parseJobEvent(JSON.stringify({ type: "result", session_id: "session",
      subtype: "success", is_error: false, result: header + sample.result }), "session", "nonce", state);
    assert.equal(completion, sample.complete ? "complete" : undefined);
    assert.equal(state.done, sample.complete ? sample.report.trim() : undefined);
  });

  test(`claude jobs subprocess: completion formatting ${sample.name}`, async t => {
    const { jobs } = fixture("complete", { env: { ...process.env, FAKE_CLAUDE_MODE: "complete",
      FAKE_CLAUDE_REPORT: sample.report, FAKE_CLAUDE_RESULT_REPORT: sample.result } });
    t.after(() => jobs.close());
    const handle = await jobs.start(input());
    assert.ok(await waitFor(() => jobs.status(handle.providerJobId).state !== "running" || jobs.phase(handle.providerJobId) === "waiting", 3000, 20));
    if (sample.complete) {
      assert.equal(jobs.status(handle.providerJobId).state, "completed");
      assert.equal(jobs.result(handle.providerJobId).output, sample.report.trim());
    } else {
      assert.equal(jobs.status(handle.providerJobId).state, "running");
      assert.equal(jobs.result(handle.providerJobId).output, undefined);
    }
  });
}

const invalidCompletionEvents: { name: string; type: "assistant" | "result"; values?: Record<string, unknown>; omit?: string[] }[] = [
  ...(["assistant", "result"] as const).flatMap(type => [
    { name: `${type} missing session ID`, type, omit: ["session_id"] },
    { name: `${type} null session ID`, type, values: { session_id: null } },
    { name: `${type} wrong session ID`, type, values: { session_id: "other-session" } },
  ]),
  { name: "result missing success fields", type: "result", omit: ["subtype", "is_error"] },
  { name: "result missing subtype", type: "result", omit: ["subtype"] },
  { name: "result null subtype", type: "result", values: { subtype: null } },
  { name: "result non-success subtype", type: "result", values: { subtype: "error_during_execution" } },
  { name: "result missing is_error", type: "result", omit: ["is_error"] },
  { name: "result null is_error", type: "result", values: { is_error: null } },
  { name: "result true is_error", type: "result", values: { is_error: true } },
  { name: "result zero is_error", type: "result", values: { is_error: 0 } },
  { name: "result string is_error", type: "result", values: { is_error: "false" } },
];

for (const patch of invalidCompletionEvents) {
  test(`claude jobs protocol: rejects ${patch.name} without completing`, () => {
    const state: TurnState = { phase: "working" };
    const text = "CLAUDE_JOB nonce DONE\nReport";
    const records: Record<string, unknown>[] = [
      { type: "assistant", session_id: "session", message: { content: [{ type: "text", text }] } },
      { type: "result", session_id: "session", subtype: "success", is_error: false, result: text },
    ];
    const record = records.find(record => record.type === patch.type)!;
    Object.assign(record, patch.values);
    for (const field of patch.omit ?? []) delete record[field];
    assert.throws(() => {
      for (const record of records) {
        assert.notEqual(parseJobEvent(JSON.stringify(record), "session", "nonce", state), "complete");
      }
    }, patch.type === "assistant" || patch.omit?.includes("session_id") || patch.values?.session_id !== undefined ? /session-mismatch/ : /claude-error/);
  });

  test(`claude jobs subprocess: ${patch.name} fails closed with no result`, async t => {
    const { jobs } = fixture("complete", { env: { ...process.env, FAKE_CLAUDE_MODE: "complete", FAKE_CLAUDE_EVENT_PATCH: JSON.stringify(patch) } });
    t.after(() => jobs.close());
    const handle = await jobs.start(input());
    assert.ok(await waitFor(() => jobs.status(handle.providerJobId).state === "failed", 3000, 20));
    assert.equal(jobs.status(handle.providerJobId).failureCode, "stream-protocol-error");
    assert.equal(jobs.result(handle.providerJobId).output, undefined);
  });
}

test("claude jobs args preserve writer permissions, empty MCP, no browser and launch-only inbound acceptance", () => {
  const args = claudeJobArgs("id", "name");
  assert.equal(WRITER_TOOLS, "Read,Write,Edit,Glob,Grep,Bash,ListAgents,SendMessage");
  assert.equal(args[args.indexOf("--tools") + 1], WRITER_TOOLS);
  assert.equal(args[args.indexOf("--allowedTools") + 1], WRITER_TOOLS);
  assert.equal(args[args.indexOf("--mcp-config") + 1], '{"mcpServers":{}}');
  assert.equal(args[args.indexOf("--setting-sources") + 1], "user");
  assert.ok(args.includes("--no-chrome") && args.includes("--strict-mcp-config"));
  assert.equal(args[args.indexOf("--settings") + 1], '{"crossSessionInbound":"accept"}');
  assert.ok(!args.includes("--dangerously-skip-permissions"));
});

test("claude jobs subprocess: question, unrelated output, parent answer, continued same context and completion", async t => {
  const { jobs, launches } = fixture();
  t.after(() => jobs.close());
  const start = input();
  const handle = await jobs.start(start);
  assert.equal((await jobs.start(start)).providerJobId, handle.providerJobId);
  assert.equal(launches(), 1);
  await assert.rejects(jobs.start({ ...start, options: { ...start.options, scope: "different" } }), /duplicate-start-mismatch/);
  assert.ok(await waitFor(() => jobs.phase(handle.providerJobId) === "waiting", 3000, 20));
  assert.equal(jobs.status(handle.providerJobId).state, "running");
  jobs.control(owner.id, handle.providerJobId, "steer", "unrelated", "unrelated");
  await sleep(100);
  assert.equal(jobs.status(handle.providerJobId).state, "running");
  jobs.control(owner.id, handle.providerJobId, "steer", "answer", "parent decision: proceed");
  assert.ok(await waitFor(() => jobs.status(handle.providerJobId).state === "completed", 3000, 20));
  assert.match(jobs.result(handle.providerJobId).output!, /retained context, turns=3/);
  assert.equal(jobs.control(owner.id, handle.providerJobId, "steer", "answer", "parent decision: proceed").state, "completed");
  assert.throws(() => jobs.control(owner.id, handle.providerJobId, "steer", "answer", "different"), /duplicate-control-mismatch/);
  assert.equal(jobs.reattach(handle.providerJobId).state, "completed");
  assert.equal("followUp" in jobs, false);
});

for (const mode of ["invalid", "overflow", "crash"]) {
  test(`claude jobs subprocess: ${mode} fails closed`, async t => {
    const { jobs } = fixture(mode);
    t.after(() => jobs.close());
    const handle = await jobs.start(input());
    assert.ok(await waitFor(() => jobs.status(handle.providerJobId).state === "failed", 3000, 20));
    assert.equal(jobs.result(handle.providerJobId).output, undefined);
  });
}

test("claude jobs: deadline, cancellation, binding loss, shutdown and ownership", async t => {
  const { jobs, lost } = fixture("hang");
  t.after(() => jobs.close());
  await assert.rejects(jobs.start(input({ sessionId: "other" })), /owner-unavailable/);
  await assert.rejects(jobs.start(input({ options: { scope: "fixture", parentTarget: "other" } })), /invalid-options/);
  await assert.rejects(jobs.start(input({ options: { scope: "fixture", command: "sh" } })), /invalid-options/);
  await assert.rejects(jobs.start(input({ promptDigest: "wrong" })), /invalid-start/);
  assert.throws(() => jobs.reattach("missing"), /unknown-job/);
  const first = await jobs.start(input({ options: { scope: "fixture", timeoutMs: 150 } }));
  assert.ok(await waitFor(() => jobs.status(first.providerJobId).failureCode === "deadline", 2000, 20));
  await sleep(600);
  const second = await jobs.start(input());
  assert.throws(() => jobs.control("other", second.providerJobId, "cancel", "request"), /owner-unavailable/);
  jobs.control(owner.id, second.providerJobId, "cancel", "request");
  assert.equal(jobs.status(second.providerJobId).failureCode, "explicit-cancel");
  await sleep(600);
  const third = await jobs.start(input());
  lost();
  assert.equal(jobs.status(third.providerJobId).failureCode, "binding-lost");
  await sleep(600);
  const fourth = await jobs.start(input());
  jobs.close();
  assert.equal(jobs.status(fourth.providerJobId).failureCode, "session-shutdown");
  await assert.rejects(jobs.start(input()), /owner-unavailable/);
});

test("claude jobs: launch failures, missing bridge and capacity fail closed", async t => {
  const broken = fixture("hang", { command: "/missing/claude" }).jobs;
  t.after(() => broken.close());
  const handle = await broken.start(input());
  assert.ok(await waitFor(() => broken.status(handle.providerJobId).state === "failed", 3000, 20));
  await assert.rejects(bindJob({ ...owner, callerPid: process.pid, onLost() {} }), /verified bridge missing/);
  const { jobs } = fixture("hang");
  t.after(() => jobs.close());
  await jobs.start(input()); await jobs.start(input());
  await assert.rejects(jobs.start(input()), /live-capacity/);
});

const stockRoot = piSubagentsDir();
function requireStock(t: { skip(message?: string): void }): boolean {
  if (stockRoot) return true;
  assert.notEqual(process.env.PI_CC_BRIDGE_STRICT, "1", "pi-subagents public external-job provider missing; refusing to skip in strict mode");
  t.skip("optional pi-subagents public external-job provider not installed");
  return false;
}
test("claude jobs public registry: conflicting owners fail closed, shutdown/reload does not dispose another owner", async t => {
  if (!requireStock(t)) return;
  const api = await loadProviderApi(resolve(stockRoot!, "../../.."));
  const a = fixture("hang").jobs;
  const b = fixture("hang").jobs;
  const disposeA = registerClaudeJobs(api, a);
  t.after(() => { a.close(); b.close(); disposeA(); });
  assert.throws(() => registerClaudeJobs(api, b), /already owned/);
  b.close();
  assert.equal(api.getExternalJobProvider("claude-worker"), a);
  a.close(); disposeA();
  const replacement = fixture("hang").jobs;
  const disposeReplacement = registerClaudeJobs(api, replacement);
  disposeA();
  assert.equal(api.getExternalJobProvider("claude-worker"), replacement);
  replacement.close(); disposeReplacement();
});

test("claude jobs stock external-job runner and real request bridge: completion and missing registry", async t => {
  if (!requireStock(t)) return;
  const api = await loadProviderApi(resolve(stockRoot!, "../../.."));
  const runner = await import(pathToFileURL(join(stockRoot!, "src/runs/shared/external-job-runner.js")).href);
  const bridge = await import(pathToFileURL(join(stockRoot!, "src/runs/shared/external-job-bridge.js")).href);
  const directory = mkdtempSync(join(tmpdir(), "claude-job-runner-"));
  const { jobs } = fixture("complete");
  const dispose = registerClaudeJobs(api, jobs);
  const service = setInterval(() => bridge.serviceExternalJobBridgeRequests(directory), 10);
  t.after(() => { clearInterval(service); jobs.close(); dispose(); rmSync(directory, { recursive: true, force: true }); });
  const request = input();
  const result = await runner.runExternalJob({ ...request, asyncDir: directory, provider: "claude-worker" });
  assert.equal(result.exitCode, 0, result.error);
  assert.equal(result.externalJob.state, "completed");
  assert.equal(readFileSync(result.externalJob.resultArtifactPath, "utf8"), "Implemented fixture");
  dispose();
  const missingDir = mkdtempSync(join(directory, "missing-"));
  const serviceMissing = setInterval(() => bridge.serviceExternalJobBridgeRequests(missingDir), 10);
  try {
    const missing = await runner.runExternalJob({ ...input(), asyncDir: missingDir, provider: "claude-worker" });
    assert.equal(missing.exitCode, 1);
    assert.match(missing.error, /provider-unavailable/);
  } finally { clearInterval(serviceMissing); }
});

test("claude jobs guardian: owner crash kills its Claude subprocess without recovered PID signalling", async t => {
  const dir = mkdtempSync(join(tmpdir(), "claude-job-orphan-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const pidPath = join(dir, "pid");
  const source = `import { fork } from 'node:child_process';
const g=fork(${JSON.stringify(resolve("src/claude-jobs/process.mjs"))},[],{execArgv:[],stdio:['ignore','ignore','ignore','ipc']});
g.on('message',m=>{if(m.type==='spawned')g.send({type:'input',text:'CLAUDE_JOB nonce WAIT'});});
g.send({type:'launch',command:${JSON.stringify(resolve("test/fixtures/fake-claude-launcher.mjs"))},args:['--session-id','fake'],cwd:process.cwd(),env:{...process.env,FAKE_CLAUDE_MODE:'hang',FAKE_CLAUDE_PID:${JSON.stringify(pidPath)}},timeoutMs:10000});`;
  const parent = spawn(process.execPath, ["--input-type=module", "-e", source], { stdio: "ignore" });
  t.after(() => parent.kill("SIGTERM"));
  assert.ok(await waitFor(() => existsSync(pidPath), 3000, 20));
  const pid = Number(readFileSync(pidPath, "utf8"));
  assert.ok(isPidAlive(pid));
  parent.kill("SIGKILL");
  assert.ok(await waitFor(() => !isPidAlive(pid), 3000, 20), "orphan Claude must be reaped by guardian");
});

test("claude jobs binding: real broker pins exact parent and bridge identities, rejects missing parent and cancels on loss", { skip: SKIP }, async t => {
  const dir = mkdtempSync(join(tmpdir(), "claude-job-binding-"));
  mkdirSync(join(dir, "pi-cc-bridge"));
  const recordsDir = join(dir, "records");
  mkdirSync(recordsDir);
  writeFileSync(join(dir, "pi-cc-bridge/daemon.lock"), `${process.pid} ${procStart()}`);
  const record = { pid: process.pid, procStart: procStart(), sessionId: randomUUID(), kind: "bridge", bridgeOwner: "pi-cc-bridge",
    name: "pinned-funnel", messagingSocketPath: join(dir, "funnel.sock") };
  const recordPath = join(recordsDir, `${process.pid}.json`);
  writeFileSync(recordPath, JSON.stringify(record));
  const broker = await startBrokerAt(dir);
  const parent = new BrokerClient(dir);
  t.after(() => { parent.close(); broker.kill("SIGTERM"); rmSync(dir, { recursive: true, force: true }); });
  await parent.connectAndRegister("exact-parent", { cwd: process.cwd(), model: "test", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() });
  let lost = 0;
  const args = { agentDir: dir, ccSessionsDir: recordsDir, parentTarget: "exact-parent", cwd: process.cwd(), callerPid: process.pid, onLost: () => { lost++; } };
  const binding = await bindJob(args);
  t.after(() => binding.close());
  assert.equal(binding.funnelName, "pinned-funnel");
  assert.equal(binding.parentTarget, "exact-parent");
  await assert.rejects(bindJob({ ...args, parentTarget: "other-owner" }), /default broker ID/);
  await assert.rejects(bindJob({ ...args, callerPid: process.pid + 1 }), /parent missing/);
  writeFileSync(recordPath, JSON.stringify({ ...record, sessionId: "rebound" }));
  assert.ok(await waitFor(() => lost > 0, 2000, 20));
  binding.close();
  writeFileSync(recordPath, JSON.stringify(record));
  lost = 0;
  const parentBinding = await bindJob(args);
  parent.close();
  assert.ok(await waitFor(() => lost > 0, 2000, 20));
  parentBinding.close();
});

test("claude jobs extension: conflicting process-global intercom env never selects another session", async t => {
  if (!requireStock(t)) return;
  const oldEnv = process.env.PI_INTERCOM_SESSION_ID;
  const oldDir = process.env.PI_CODING_AGENT_DIR;
  const profile = resolve(stockRoot!, "../../..");
  process.env.PI_CODING_AGENT_DIR = profile;
  process.env.PI_INTERCOM_SESSION_ID = "foreign-same-process-session";
  t.after(() => {
    if (oldEnv === undefined) delete process.env.PI_INTERCOM_SESSION_ID; else process.env.PI_INTERCOM_SESSION_ID = oldEnv;
    if (oldDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = oldDir;
  });
  const handlers = new Map<string, (...args: any[]) => any>();
  claudeJobsExtension({ on: (event: string, handler: (...args: any[]) => any) => { handlers.set(event, handler); }, registerTool() {}, registerCommand() {} } as unknown as ExtensionAPI);
  const ctx = { cwd: process.cwd(), sessionManager: { getSessionId: () => "owning-uuid", getSessionFile: () => undefined } } as unknown as ExtensionContext;
  await handlers.get("session_start")!({}, ctx);
  const api = await loadProviderApi(profile);
  const registered = api.getExternalJobProvider("claude-worker") as ClaudeJobs;
  assert.equal(registered.owner.parentTarget, "owning-uuid");
  await assert.rejects(registered.start(input({ sessionId: "owning-uuid", options: { scope: "fixture", parentTarget: "foreign-same-process-session" } })), /invalid-options/);
  handlers.get("session_shutdown")!();
  assert.equal(api.getExternalJobProvider("claude-worker"), undefined);
});

test("claude jobs optional stock policy: all stock-dependent tests skip normally and fail explicitly in strict mode", () => {
  for (const strict of ["0", "1"]) {
    const env: NodeJS.ProcessEnv = { ...process.env, PI_SUBAGENTS_DIR: "/nonexistent/optional-pi-subagents", PI_CC_BRIDGE_STRICT: strict };
    delete env.NODE_TEST_CONTEXT;
    const child = spawnSync(process.execPath, ["--import", "tsx", "--test", "--test-name-pattern",
      "claude jobs public registry|claude jobs stock external-job|claude jobs extension: conflicting", "test/claude-jobs.test.ts"], {
      cwd: process.cwd(), encoding: "utf8", timeout: 10_000,
      env,
    });
    assert.equal(child.error, undefined);
    if (strict === "1") {
      assert.notEqual(child.status, 0);
      assert.match(child.stdout + child.stderr, /refusing to skip in strict mode/);
    } else {
      assert.equal(child.status, 0, child.stdout + child.stderr);
      assert.match(child.stdout + child.stderr, /optional pi-subagents public external-job provider not installed/);
    }
  }
});
