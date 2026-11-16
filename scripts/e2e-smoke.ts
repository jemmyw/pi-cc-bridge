/**
 * Manual round-trip through an isolated broker and a dedicated Claude probe.
 * Run with tsx scripts/e2e-smoke.ts. Never treats Claude stdout as delivery.
 */
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, writeFileSync, readFileSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { createRequire } from "node:module";
import { BrokerClient, type BrokerEvent, type BrokerSessionInfo } from "../src/intercom/client.ts";
import { BRIDGE_OWNER, ccSessionsDir } from "../src/cc/paths.ts";
import { isPidAlive, procStart } from "../src/cc/registry.ts";

const PKG_DIR = fileURLToPath(new URL("..", import.meta.url));
const requireFromPkg = createRequire(join(PKG_DIR, "package.json"));
type ReceivedMessage = Extract<BrokerEvent, { type: "message" }>;

export function isProbeMessage(event: ReceivedMessage, peer: { id: string; name: string }, text: string): boolean {
  return event.from.id === peer.id && event.from.name === peer.name &&
    event.message.content.text === `Message from Claude Code session ${peer.name.slice("claude:".length)}:\n${text}`;
}

export function assertNoPrematureAck(messages: ReceivedMessage[], peer: { id: string; name: string }, ack: string): void {
  if (messages.some(event => isProbeMessage(event, peer, ack))) throw new Error("premature ACK before pi to CC ping");
}

function intercomDir(): string {
  if (process.env.PI_INTERCOM_DIR?.trim()) return process.env.PI_INTERCOM_DIR.trim();
  try {
    return dirname(requireFromPkg.resolve("pi-intercom/package.json"));
  } catch {
    return join(homedir(), ".pi", "agent", "npm", "node_modules", "pi-intercom");
  }
}

function tsxLoader(intercom: string): string {
  if (process.env.TSX_LOADER?.trim()) return process.env.TSX_LOADER.trim();
  try {
    return requireFromPkg.resolve("tsx");
  } catch {
    return join(intercom, "..", "tsx", "dist", "loader.mjs");
  }
}

async function stop(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null || !child.pid) return;
  const exited = new Promise<void>(resolveExit => child.once("exit", () => resolveExit()));
  child.kill("SIGTERM");
  await Promise.race([exited, sleep(3_000)]);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGKILL");
    await Promise.race([exited, sleep(3_000)]);
  }
  if (child.exitCode === null && child.signalCode === null) throw new Error(`child ${child.pid} did not exit`);
}

/** Remove only a stopped child's record whose identity still matches this run. */
function cleanRecord(pid: number, name: string, bridge: boolean): void {
  if (isPidAlive(pid)) throw new Error(`refusing record cleanup for live pid ${pid}`);
  const dir = ccSessionsDir();
  const path = join(dir, `${pid}.json`);
  try {
    const record = JSON.parse(readFileSync(path, "utf-8"));
    if (record.pid !== pid || record.name !== name) throw new Error(`record identity changed for pid ${pid}`);
    if (bridge && record.bridgeOwner !== BRIDGE_OWNER) throw new Error(`record ownership changed for pid ${pid}`);
    rmSync(path);
    for (const file of readdirSync(dir)) {
      if (new RegExp(`^${pid}\\.[a-f0-9]{64}\\.key$`).test(file)) rmSync(join(dir, file));
    }
    if (typeof record.messagingSocketPath === "string" && record.messagingSocketPath.endsWith(`/${pid}.sock`)) {
      rmSync(record.messagingSocketPath, { force: true });
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

export async function main(): Promise<void> {
  const nonce = randomUUID();
  const peerName = `pi-bridge-probe-${nonce}`;
  const targetName = `e2e-target-${nonce}`;
  const probeName = `ccbridge-probe-${nonce}`;
  const hello = `CC-TO-PI ${nonce}`;
  const nativeHello = `NATIVE-CC-TO-PI ${nonce}`;
  const holderName = `pi-${targetName}`;
  const ping = `PI-TO-CC ${nonce}`;
  const ack = `ACK ${nonce}`;
  const agentDir = mkdtempSync(join(tmpdir(), "ccbridge-e2e-"));
  const children: ChildProcess[] = [];
  const records: { child: ChildProcess; name: string; bridge: boolean }[] = [];
  const diagnostics = new Map<ChildProcess, { role: string; stdout: Buffer; stderr: Buffer }>();
  const holders = new Map<number, string>();
  let daemon: ChildProcess | undefined;
  let succeeded = false;
  let target: BrokerClient | undefined;
  let interrupted = false;
  let childFailure: Error | undefined;
  const interrupt = () => { interrupted = true; };
  process.on("SIGINT", interrupt);
  process.on("SIGTERM", interrupt);
  const launch = (role: string, command: string, args: string[], env = process.env): ChildProcess => {
    const child = spawn(command, args, { cwd: agentDir, env, stdio: ["pipe", "pipe", "pipe"] });
    children.push(child);
    const tails = { role, stdout: Buffer.alloc(0), stderr: Buffer.alloc(0) };
    diagnostics.set(child, tails);
    for (const stream of ["stdout", "stderr"] as const) {
      child[stream]!.on("data", (chunk: Buffer) => {
        tails[stream] = Buffer.concat([tails[stream], chunk]).subarray(-64 * 1024);
      });
    }
    child.once("error", error => { childFailure = new Error(`${role} spawn failed: ${error.message}`); });
    return child;
  };
  const findHolder = (): boolean => {
    if (!daemon?.pid) return false;
    for (const file of readdirSync(ccSessionsDir())) {
      if (!file.endsWith(".json")) continue;
      let record;
      try { record = JSON.parse(readFileSync(join(ccSessionsDir(), file), "utf-8")); } catch { continue; }
      if (record.name !== holderName || record.bridgeOwner !== BRIDGE_OWNER || record.kind !== "bridge") continue;
      if (!Number.isSafeInteger(record.pid) || record.pid <= 0 || !isPidAlive(record.pid)) continue;
      let args: string;
      try {
        args = execFileSync("/bin/ps", ["-p", String(record.pid), "-o", "args="], { encoding: "utf-8" });
      } catch (error) {
        if (!isPidAlive(record.pid)) continue;
        throw error;
      }
      if (!args.includes(`"parentPid":${daemon.pid},`) || !args.includes(`"piSessionId":"${targetName}"`) || !args.includes(agentDir)) continue;
      const start = procStart(record.pid);
      if (!start) throw new Error("cannot verify owned holder identity");
      holders.set(record.pid, start);
      return true;
    }
    return false;
  };
  const waitFor = async (label: string, predicate: () => boolean, timeout = 15_000) => {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      if (interrupted) throw new Error("smoke interrupted");
      if (childFailure) throw childFailure;
      const exited = children.find(child => child.exitCode !== null || child.signalCode !== null);
      if (exited) throw new Error(`${diagnostics.get(exited)!.role} pid ${exited.pid} exited code=${exited.exitCode} signal=${exited.signalCode} while waiting for ${label}`);
      if (predicate()) return;
      await sleep(100);
    }
    throw new Error(`timed out waiting for ${label}; ` + children.map(child =>
      `${diagnostics.get(child)!.role} pid=${child.pid} code=${child.exitCode} signal=${child.signalCode}`,
    ).join(", "));
  };

  try {
    const intercom = intercomDir();
    const tsx = tsxLoader(intercom);
    const stateDir = join(agentDir, "pi-cc-bridge");
    mkdirSync(stateDir, { mode: 0o700 });
    writeFileSync(join(stateDir, "config.json"), JSON.stringify({
      enabled: true, peerName, pollMs: 500, logLevel: "normal",
      piSenderMode: "bypass", ccInbound: "hold-bypass", expose: [targetName], maxHolders: 1,
    }), { mode: 0o600 });
    const env = { ...process.env, PI_CODING_AGENT_DIR: agentDir, NODE_NO_WARNINGS: "1" };
    launch("broker", process.execPath, ["--import", tsx, join(intercom, "broker", "broker.ts")], env);
    await waitFor("isolated broker", () => existsSync(join(agentDir, "intercom", "broker.sock")));
    daemon = launch("daemon", process.execPath, ["--import", tsx, join(PKG_DIR, "src", "cli.ts")], env);
    records.push({ child: daemon, name: peerName, bridge: true });
    await waitFor("dedicated funnel", () => {
      try {
        return JSON.parse(readFileSync(join(ccSessionsDir(), `${daemon!.pid}.json`), "utf-8")).name === peerName;
      } catch { return false; }
    });

    target = new BrokerClient(agentDir);
    const messages: ReceivedMessage[] = [];
    const sessions = new Map<string, BrokerSessionInfo>();
    target.on("message", (event: ReceivedMessage) => {
      messages.push(event);
      target!.receipt(event.message.id);
    });
    target.on("error", (event: { error: string }) => { childFailure = new Error(event.error); });
    target.on("session_joined", (event: Extract<BrokerEvent, { type: "session_joined" }>) => sessions.set(event.session.id, event.session));
    target.on("sessions", (event: Extract<BrokerEvent, { type: "sessions" }>) => {
      for (const session of event.sessions) sessions.set(session.id, session);
    });
    await target.connectAndRegister(targetName, {
      name: targetName, cwd: agentDir, model: "e2e", pid: process.pid,
      startedAt: Date.now(), lastActivity: Date.now(), status: "idle",
      acknowledgesReceipts: true, livenessIntervalMs: 5_000,
    });

    const cc = launch("claude", "claude", [
      "-p", "--name", probeName, "--model", "haiku", "--safe-mode", "--no-session-persistence",
      "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}', "--setting-sources", "user", "--no-chrome",
      "--settings", '{"crossSessionInbound":"accept"}',
      "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
      "--tools", "ListAgents,SendMessage", "--allowedTools", "ListAgents,SendMessage",
    ]);
    records.push({ child: cc, name: probeName, bridge: false });
    const prompt = `Use ListAgents to find the session named "${peerName}". Message only "${peerName}" and "${holderName}". ` +
      `Use SendMessage to send exactly "To: ${targetName}\\n${hello}" to it (replace \\n with a newline). ` +
      `Then use ListAgents to find "${holderName}" (retry discovery if not visible yet) and send exactly "${nativeHello}" to it. ` +
      `When you receive a cross-session message from "${holderName}" whose text is exactly "${ping}", ` +
      `use SendMessage to send exactly "${ack}" to "${holderName}". Do not send the ACK before receiving that message.`;
    cc.stdin!.on("error", error => { childFailure = error; });
    cc.stdin!.write(JSON.stringify({ type: "user", message: { role: "user", content: prompt } }) + "\n");

    target.list();
    await waitFor("dedicated Claude registry record", () => {
      try {
        return JSON.parse(readFileSync(join(ccSessionsDir(), `${cc.pid}.json`), "utf-8")).name === probeName;
      } catch { return false; }
    }, 60_000);
    const record = JSON.parse(readFileSync(join(ccSessionsDir(), `${cc.pid}.json`), "utf-8"));
    const expected = { id: `cc-${record.sessionId}`, name: `claude:${probeName}` };
    await waitFor("Claude peer on isolated broker", () => sessions.get(expected.id)?.name === expected.name);
    await waitFor("owned native holder record", findHolder);
    await waitFor("CC to pi nonce", () => messages.some(event => isProbeMessage(event, expected, hello)), 120_000);
    console.log("PASS CC to pi: exact nonce received from dedicated Claude peer");
    await waitFor("native CC to pi nonce", () => messages.some(event => isProbeMessage(event, expected, nativeHello)), 120_000);
    console.log("PASS native holder discovery and CC to pi delivery");
    assertNoPrematureAck(messages, expected, ack);
    console.log("PASS no premature ACK before pi to CC ping");
    const beforeReply = messages.length;
    target.send(expected.id, { id: randomUUID(), timestamp: Date.now(), content: { text: ping } });
    await waitFor("pi to CC acknowledgement", () => messages.slice(beforeReply).some(event => isProbeMessage(event, expected, ack)), 120_000);
    console.log("PASS native pi to CC: exact ACK received through broker from dedicated Claude peer");
    succeeded = true;
  } finally {
    const errors: unknown[] = [];
    try { findHolder(); } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") errors.push(error);
    }
    target?.close();
    for (const child of [...children].reverse()) {
      try { await stop(child); } catch (error) { errors.push(error); }
    }
    for (const [pid, start] of holders) {
      try {
        for (const signal of ["SIGTERM", "SIGKILL"] as const) {
          if (!isPidAlive(pid)) break;
          if (procStart(pid) !== start) throw new Error(`holder pid ${pid} identity changed`);
          process.kill(pid, signal);
          const deadline = Date.now() + 3_000;
          while (isPidAlive(pid) && Date.now() < deadline) await sleep(100);
        }
        cleanRecord(pid, holderName, true);
        if (existsSync(join(ccSessionsDir(), `${pid}.json`))) throw new Error(`holder record ${pid} remains`);
      } catch (error) { errors.push(error); }
    }
    for (const record of records) {
      if (!record.child.pid) continue;
      try { cleanRecord(record.child.pid, record.name, record.bridge); } catch (error) { errors.push(error); }
    }
    try {
      for (const file of readdirSync(ccSessionsDir())) {
        if (!file.endsWith(".json")) continue;
        let record;
        try { record = JSON.parse(readFileSync(join(ccSessionsDir(), file), "utf-8")); } catch { continue; }
        if (record.name === holderName && record.bridgeOwner === BRIDGE_OWNER) throw new Error(`owned holder record remains: ${file}`);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") errors.push(error);
    }
    for (const [child, tails] of diagnostics) {
      try {
        for (const stream of ["stdout", "stderr"] as const) {
          writeFileSync(join(agentDir, `${tails.role}.${stream}.tail`), tails[stream], { mode: 0o600 });
        }
        writeFileSync(join(agentDir, `${tails.role}.exit.json`), JSON.stringify({ pid: child.pid, code: child.exitCode, signal: child.signalCode }), { mode: 0o600 });
      } catch (error) { errors.push(error); }
    }
    process.off("SIGINT", interrupt);
    process.off("SIGTERM", interrupt);
    if (errors.length) throw new AggregateError(errors, `cleanup incomplete; private diagnostics retained at ${agentDir}`);
    if (succeeded) {
      rmSync(agentDir, { recursive: true, force: true });
      console.log("PASS owned children and native holder records removed");
    } else {
      console.error(`Private smoke diagnostics retained at ${agentDir}`);
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    console.error("smoke failed:", (error as Error).message);
    process.exitCode = 1;
  });
}
