import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import { startBrokerAt, resolveTsx, waitFor, SKIP, SKIP_REASON } from "./helpers.ts";
import { BrokerClient, type BrokerSessionInfo } from "../src/intercom/client.ts";
import net from "node:net";

const PKG_DIR = new URL("..", import.meta.url).pathname;
const skip = SKIP ? SKIP_REASON : false;

function freshFixture(agentDir: string): { sessionsDir: string; socketDir: string; mySocketPath: string; myServer: net.Server; myLines: string[] } {
  const sessionsDir = join(agentDir, "cc-sessions");
  const socketDir = join(agentDir, "cc-run");
  mkdirSync(sessionsDir, { recursive: true, mode: 0o700 });
  mkdirSync(socketDir, { recursive: true, mode: 0o700 });
  const mySocketPath = join(socketDir, `${process.pid}.sock`);
  const myLines: string[] = [];
  const myServer = net.createServer(socket => {
    let buf = Buffer.alloc(0);
    socket.on("data", chunk => {
      buf = Buffer.concat([buf, chunk]);
      let idx: number;
      while ((idx = buf.indexOf(10)) !== -1) {
        const line = buf.subarray(0, idx).toString("utf-8").trim();
        buf = buf.subarray(idx + 1);
        if (line) myLines.push(line);
      }
    });
  });
  writeFileSync(
    join(sessionsDir, `${process.pid}.json`),
    JSON.stringify({
      pid: process.pid,
      sessionId: randomUUID(),
      cwd: process.cwd(),
      startedAt: Date.now(),
      procStart: "1",
      version: "2.1.296",
      peerProtocol: 1,
      peerFeatures: ["reply_across_default_dirs"],
      kind: "interactive",
      entrypoint: "cli",
      pidDomain: "linux:test:pid:[test]",
      messagingSocketPath: mySocketPath,
      name: "test-cc",
      nameSource: "user",
      nameSince: Date.now(),
      updatedAt: Date.now(),
      status: "idle",
      statusUpdatedAt: Date.now(),
    }),
  );
  return { sessionsDir, socketDir, mySocketPath, myServer, myLines };
}

function startDaemon(agentDir: string): ChildProcess {
  const tsx = resolveTsx()!;
  return spawn(process.execPath, ["--import", tsx, join(PKG_DIR, "src", "cli.ts")], {
    env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, NODE_NO_WARNINGS: "1" },
    stdio: ["ignore", "ignore", "pipe"],
  });
}

function registerPlanner(agentDir: string): Promise<BrokerClient> {
  return new Promise(async (resolve, reject) => {
    const client = new BrokerClient(agentDir);
    try {
      await client.connectAndRegister("planner-id", {
        name: "planner",
        cwd: process.cwd(),
        model: "test",
        pid: process.pid,
        startedAt: Date.now(),
        lastActivity: Date.now(),
        status: "busy",
        livenessIntervalMs: 5_000,
      });
      resolve(client);
    } catch (error) {
      reject(error);
    }
  });
}

test("lifecycle: broker restart reconnects the mirror without replacing the single bridge", { skip }, async t => {
  const agentDir = mkdtempSync(join(tmpdir(), "ccbridge-life-"));
  const { sessionsDir, mySocketPath, myServer } = freshFixture(agentDir);
  await new Promise<void>(resolve => myServer.listen(mySocketPath, resolve));
  const stateDir = join(agentDir, "pi-cc-bridge");
  mkdirSync(stateDir, { recursive: true, mode: 0o700 });
  writeFileSync(
    join(stateDir, "config.json"),
    JSON.stringify({ enabled: true, pollMs: 250, ccSessionsDir: sessionsDir, ccSocketDir: join(agentDir, "cc-run") }),
  );

  let broker = await startBrokerAt(agentDir);

  // Two daemons race: exactly one owns the guard (P0-3).
  const d1 = startDaemon(agentDir);
  const d2 = startDaemon(agentDir);
  t.after(async () => {
    for (const child of [d1, d2, broker]) {
      child.kill("SIGTERM");
      await waitFor(() => child.exitCode !== null || child.signalCode !== null);
    }
    myServer.close();
    rmSync(agentDir, { recursive: true, force: true });
  });
  let exits = 0;
  const onExit = () => exits++;
  d1.on("exit", onExit);
  d2.on("exit", onExit);
  await sleep(2_500);
  const lockPid = Number(readFileSync(join(stateDir, "daemon.lock"), "utf-8").trim().split(" ")[0]);
  const survivors = [d1, d2].filter(d => d.pid === lockPid && d.exitCode === null);
  assert.equal(exits, 1, "exactly one daemon exits under lock contention");
  assert.equal(survivors.length, 1, "lock owner survives");
  const daemon = survivors[0];

  const bridgeFile = join(sessionsDir, `${daemon.pid}.json`);
  async function assertMirror() {
    const planner = await registerPlanner(agentDir);
    try {
      let mirrored = false;
      planner.on("sessions", event => { mirrored ||= event.sessions.some((session: BrokerSessionInfo) => session.name === "claude:test-cc"); });
      planner.on("session_joined", event => { mirrored ||= event.session.name === "claude:test-cc"; });
      planner.list();
      assert.ok(await waitFor(() => mirrored, 20_000), "Claude peer registers on the broker");
    } finally {
      planner.close();
    }
  }
  await assertMirror();
  const initial = readFileSync(bridgeFile, "utf-8");
  broker.kill("SIGTERM");
  assert.ok(await waitFor(() => broker.exitCode !== null || broker.signalCode !== null));
  broker = await startBrokerAt(agentDir);
  await assertMirror();
  assert.equal(readFileSync(bridgeFile, "utf-8"), initial);
  assert.equal(daemon.exitCode, null);
});
