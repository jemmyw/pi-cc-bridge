import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync, unlinkSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import net from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import { startIsolatedBroker, resolveTsx, waitFor, BROKER_ENTRY, SKIP, SKIP_REASON } from "./helpers.ts";
import { BrokerClient, type BrokerEvent } from "../src/intercom/client.ts";
import { buildOutboundLine, parseInboundLine } from "../src/cc/protocol.ts";

/**
 * Native-entries integration: a fully SYNTHETIC CC↔pi loop.
 *
 * The test process fakes one CC session (registry record + inbox socket), a
 * fake pi session "planner" registers on the isolated broker, and the daemon
 * under test must:
 *   1. discover the fake CC session and register peer claude:test-cc,
 *   2. spawn a holder child that exposes pi-planner in the CC registry,
 *   3. relay CC → pi with attribution (fake CC posts to the holder socket),
 *   4. relay pi → CC (planner sends to claude:test-cc; daemon posts to the
 *      fake CC socket).
 *
 * Skipped when pi-intercom is not installed at the standard path.
 */

const PKG_DIR = new URL("..", import.meta.url).pathname;

interface Fixture {
  agentDir: string;
  sessionsDir: string;
  socketDir: string;
  mySocketPath: string;
  myServer: net.Server;
  myLines: string[];
  cleanup: () => void;
}

async function setupFixture(): Promise<Fixture> {
  const { agentDir, stop: stopBroker } = await startIsolatedBroker();
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
    socket.on("error", () => socket.destroy());
  });
  await new Promise<void>(resolve => myServer.listen(mySocketPath, () => resolve()));

  // Fake CC session record: the daemon must treat this test process as a CC session.
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
      peerFeatures: ["notify_idle", "reply_across_default_dirs", "artifact_yield"],
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

  const stateDir = join(agentDir, "pi-cc-bridge");
  mkdirSync(stateDir, { recursive: true });
  writeFileSync(
    join(stateDir, "config.json"),
    JSON.stringify({ enabled: true, pollMs: 250, expose: ["*"], maxHolders: 8, ccSessionsDir: sessionsDir }),
  );
  // socketDir override goes through the same key the config loader reads:
  const cfg = JSON.parse(readFileSync(join(stateDir, "config.json"), "utf-8"));
  cfg.ccSocketDir = socketDir;
  writeFileSync(join(stateDir, "config.json"), JSON.stringify(cfg));

  const cleanup = () => {
    try {
      unlinkSync(join(sessionsDir, `${process.pid}.json`));
    } catch {
      /* already gone */
    }
    myServer.close();
    stopBroker();
    rmSync(agentDir, { recursive: true, force: true }); // P3-9: no test temp leaks
  };
  return { agentDir, sessionsDir, socketDir, mySocketPath, myServer, myLines, cleanup };
}

function startDaemon(agentDir: string): ReturnType<typeof spawn> {
  const tsx = resolveTsx();
  if (!tsx) throw new Error("tsx loader not found");
  return spawn(process.execPath, ["--import", tsx, join(PKG_DIR, "src", "cli.ts")], {
    env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, NODE_NO_WARNINGS: "1" },
    stdio: ["ignore", "ignore", "pipe"],
  });
}

test("native entries: holder child exposes pi sessions to CC; both relays work", { skip: SKIP_REASON }, async t => {
  const fixture = await setupFixture();
  t.after(() => fixture.cleanup());

  // Fake pi session on the isolated broker.
  const planner = new BrokerClient(fixture.agentDir);
  t.after(() => planner.close());
  const fromCc = new Promise<{ text: string; fromId: string }>(resolve => {
    planner.on("message", event => {
      resolve({ text: event.message.content.text, fromId: event.from.id });
      planner.receipt(event.message.id);
    });
  });
  await planner.connectAndRegister("planner-id", {
    name: "planner",
    cwd: process.cwd(),
    model: "test",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
    status: "busy",
    livenessIntervalMs: 5_000,
  });

  // Daemon under test.
  const daemon = startDaemon(fixture.agentDir);
  t.after(() => {
    try {
      daemon.kill("SIGTERM");
    } catch {
      /* gone */
    }
  });

  // 1. masquerade record appears
  const masqueradeOk = await waitFor(() => existsSync(join(fixture.sessionsDir, readDaemonPid(fixture.agentDir) + ".json")));
  assert.ok(masqueradeOk, "daemon masquerade record must exist");

  // 2. holder record for pi-planner appears
  const holderOk = await waitFor(() => readHolderPidFor(fixture.agentDir, "pi-planner") !== undefined, 15_000);
  assert.ok(holderOk, "holder record pi-planner must exist");
  const holderRecord = join(fixture.sessionsDir, `${readHolderPidFor(fixture.agentDir, "pi-planner")!}.json`);
  const holderRecordJson = JSON.parse(readFileSync(holderRecord, "utf-8"));
  assert.equal(holderRecordJson.name, "pi-planner");
  assert.equal(holderRecordJson.status, "busy"); // mirrored from planner's registration
  assert.notEqual(holderRecordJson.procStart, "0", "procStart must be the real field 22, not itrealvalue");
  assert.equal(holderRecordJson.bridgeOwner, "pi-cc-bridge");
  const holderSocket = holderRecordJson.messagingSocketPath as string;
  assert.ok(existsSync(holderSocket), "holder socket must exist");

  // 3. CC → pi relay: post as the fake CC session to the holder socket.
  const ccLine = buildOutboundLine({
    senderSocketPath: fixture.mySocketPath,
    senderName: "test-cc",
    senderMode: "prompting",
    text: "NATIVE-CC-TO-PI-1",
    msgId: "native-1",
  });
  await new Promise<void>((resolve, reject) => {
    const s = net.createConnection(holderSocket);
    s.on("connect", () => {
      s.write(ccLine + "\n");
      setTimeout(() => {
        s.end();
        resolve();
      }, 300);
    });
    s.on("error", reject);
  });
  const got = await Promise.race([fromCc, sleep(10_000).then(() => null)]);
  assert.ok(got, "planner must receive the relayed CC message");
  assert.equal(got.fromId, "cc-" + JSON.parse(readFileSync(join(fixture.sessionsDir, `${process.pid}.json`), "utf-8")).sessionId);
  assert.match(got.text, /Message from Claude Code session test-cc:\nNATIVE-CC-TO-PI-1/);

  // 4. pi → CC relay: planner sends to the synthetic peer by name.
  planner.send("claude:test-cc", { id: "native-2", timestamp: Date.now(), content: { text: "NATIVE-PI-TO-CC-1" } });
  const ccGot = await waitFor(() => fixture.myLines.some(l => l.includes("NATIVE-PI-TO-CC-1")), 10_000);
  assert.ok(ccGot, "fake CC socket must receive the pi message");
  const inbound = JSON.parse(fixture.myLines.find(l => l.includes("NATIVE-PI-TO-CC-1"))!);
  assert.equal(inbound.type, "user");
  assert.match(inbound.message.content, /from-name="pi-planner"/, "sender identity must be the pi session's holder entry");
  assert.equal(inbound.from, holderSocket, "sender socket must be the holder's socket so replies route back");

  // 5. cleanup: daemon SIGTERM removes holder record + socket.
  const holderPid = holderRecordJson.pid;
  daemon.kill("SIGTERM");
  const cleaned = await waitFor(() => !existsSync(holderRecord) && !existsSync(holderSocket) && !isAlive(holderPid), 10_000);
  assert.ok(cleaned, "holder record, socket, and process must be cleaned up");
});

test("funnel: unaddressed message with two Pi sessions is held and only the sender is notified", { skip: SKIP_REASON }, async t => {
  const fixture = await setupFixture();
  const sessions = ["planner", "worker"].map(name => ({ name, client: new BrokerClient(fixture.agentDir) }));
  const relayed: { to: string; id: string }[] = [];
  let daemon: ReturnType<typeof startDaemon> | undefined;
  t.after(async () => {
    for (const { client } of sessions) client.close();
    if (daemon) {
      daemon.kill("SIGTERM");
      await waitFor(() => daemon!.exitCode !== null || daemon!.signalCode !== null);
    }
    fixture.cleanup();
  });
  for (const { name, client } of sessions) {
    client.on("message", event => {
      relayed.push({ to: name, id: event.message.id });
      client.receipt(event.message.id);
    });
    await client.connectAndRegister(`${name}-id`, {
      name, cwd: process.cwd(), model: "test", pid: process.pid,
      startedAt: Date.now(), lastActivity: Date.now(), status: "idle", livenessIntervalMs: 5000,
    });
  }
  daemon = startDaemon(fixture.agentDir);
  assert.ok(await waitFor(() => sessions.every(({ name }) => readHolderPidFor(fixture.agentDir, `pi-${name}`))));
  const { messagingSocketPath: inbox } = JSON.parse(readFileSync(join(fixture.sessionsDir, `${daemon.pid}.json`), "utf-8"));
  const text = "Please review the patch";
  const msgId = "unaddressed-1";
  const line = JSON.parse(buildOutboundLine({ senderSocketPath: fixture.mySocketPath, senderName: "test-cc", text, msgId }));
  delete line.from; // Exercise the uds: sender address from the envelope.
  await new Promise<void>((resolve, reject) => {
    const socket = net.createConnection(inbox);
    socket.once("connect", () => socket.end(`${JSON.stringify(line)}\n`));
    socket.once("error", reject);
    socket.once("close", () => resolve());
  });

  assert.ok(await waitFor(() => fixture.myLines.length > 0), "sender receives the notice");
  await sleep(350);
  assert.equal(fixture.myLines.length, 1);
  const notice = parseInboundLine(fixture.myLines[0]);
  assert.ok(notice.kind === "user");
  assert.equal(notice.line.from, inbox);
  const [reason, heading, ...entries] = notice.text.split("\n");
  assert.equal(reason, "Not delivered: no To: line and 2 Pi sessions connected. Message saved in held/.");
  assert.equal(heading, "Reachable Pi sessions:");
  assert.equal(entries.pop(), "Resend directly to pi-<alias> or put To: <alias> on the first line.");
  assert.deepEqual(entries.sort(), ["- pi-planner", "- pi-worker"]);
  const heldDir = join(fixture.agentDir, "pi-cc-bridge", "held");
  const files = readdirSync(heldDir);
  assert.equal(files.length, 1);
  const { at, ...held } = JSON.parse(readFileSync(join(heldDir, files[0]), "utf-8"));
  assert.equal(typeof at, "number");
  assert.deepEqual(held, { reason: "funnel-unrouted", fromName: "test-cc", text, msgId });
  assert.deepEqual(relayed, [], "neither Pi session receives a relay");
});

function readDaemonPid(agentDir: string): string {
  try {
    return readFileSync(join(agentDir, "pi-cc-bridge", "daemon.lock"), "utf-8").trim().split(" ")[0];
  } catch {
    return "0";
  }
}

function readHolderPidFor(agentDir: string, name: string): string | undefined {
  const dir = join(agentDir, "cc-sessions");
  try {
    for (const file of readdirSync(dir)) {
      if (!file.endsWith(".json")) continue;
      const rec = JSON.parse(readFileSync(join(dir, file), "utf-8"));
      if (rec.name === name) return file.replace(/\.json$/, "");
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

for (const native of [true, false]) {
  test(`ask correlation: ${native ? "native" : "funnel"} routes use explicit tokens and broker replyTo`, { skip: SKIP_REASON }, async t => {
    const fixture = await setupFixture();
    const configPath = join(fixture.agentDir, "pi-cc-bridge", "config.json");
    const config = JSON.parse(readFileSync(configPath, "utf-8"));
    writeFileSync(configPath, JSON.stringify({ ...config, maxHolders: native ? 1 : 0, relayBurst: 32, relayPerMin: 120 }));
    const planner = new BrokerClient(fixture.agentDir);
    const messages: Extract<BrokerEvent, { type: "message" }>[] = [];
    let peerReady = false;
    planner.on("session_joined", event => { if (event.session.name === "claude:test-cc") peerReady = true; });
    planner.on("message", event => { messages.push(event); planner.receipt(event.message.id); });
    await planner.connectAndRegister("planner-id", {
      name: "planner", cwd: process.cwd(), model: "test", pid: process.pid,
      startedAt: Date.now(), lastActivity: Date.now(), status: "idle", livenessIntervalMs: 5000,
    });
    const daemon = startDaemon(fixture.agentDir);
    t.after(async () => {
      planner.close();
      daemon.kill("SIGTERM");
      await waitFor(() => daemon.exitCode !== null || daemon.signalCode !== null);
      fixture.cleanup();
    });
    assert.ok(await waitFor(() => peerReady));
    if (native) assert.ok(await waitFor(() => readHolderPidFor(fixture.agentDir, "pi-planner") !== undefined));
    const daemonRecord = JSON.parse(readFileSync(join(fixture.sessionsDir, `${daemon.pid}.json`), "utf-8"));
    const holderPid = readHolderPidFor(fixture.agentDir, "pi-planner");
    const inbox = native
      ? JSON.parse(readFileSync(join(fixture.sessionsDir, `${holderPid}.json`), "utf-8")).messagingSocketPath
      : daemonRecord.messagingSocketPath;
    const post = async (text: string, msgId: string) => {
      const line = buildOutboundLine({ senderSocketPath: fixture.mySocketPath, senderName: "renamed-cc-label",
        senderMode: "prompting", text: native ? text : `To: planner\n${text}`, msgId });
      await new Promise<void>((resolve, reject) => {
        const socket = net.createConnection(inbox);
        socket.once("connect", () => socket.end(`${line}\n`));
        socket.once("error", reject);
        socket.once("close", () => resolve());
      });
    };
    const tokens: string[] = [];
    for (let n = 1; n <= 2; n++) {
      planner.send("claude:test-cc", { id: `ask-${n}`, expectsReply: true, timestamp: Date.now(), content: { text: `question-${n}` } });
      assert.ok(await waitFor(() => fixture.myLines.some(line => line.includes(`question-${n}`))));
      const frame = parseInboundLine(fixture.myLines.find(line => line.includes(`question-${n}`))!);
      assert.equal(frame.kind, "user");
      if (frame.kind !== "user") return;
      const token = /Bridge-Reply: ([0-9a-f-]{36})/.exec(frame.text)?.[1];
      assert.ok(token);
      tokens.push(token);
      assert.equal(frame.line.from, native ? inbox : daemonRecord.messagingSocketPath);
    }
    assert.notEqual(tokens[0], tokens[1]);
    await post("unrelated notification", "notification");
    assert.ok(await waitFor(() => messages.some(event => event.message.id === "notification")));
    assert.equal(messages.find(event => event.message.id === "notification")!.message.replyTo, undefined);
    await post(`Bridge-Reply: ${tokens[1]}\nsecond answer`, "reply-2");
    await post(`Bridge-Reply: ${tokens[0]}\nfirst answer`, "reply-1");
    assert.ok(await waitFor(() => messages.filter(event => event.message.replyTo).length === 2));
    assert.equal(messages.find(event => event.message.id === "reply-2")!.message.replyTo, "ask-2");
    assert.equal(messages.find(event => event.message.id === "reply-1")!.message.replyTo, "ask-1");
    assert.match(messages.find(event => event.message.id === "reply-1")!.message.content.text, /\nfirst answer$/);
    assert.ok(!messages.find(event => event.message.id === "reply-1")!.message.content.text.includes("Bridge-Reply:"));
    await post(`Bridge-Reply: ${tokens[1]}\nduplicate`, "duplicate");
    assert.ok(await waitFor(() => readdirSync(join(fixture.agentDir, "pi-cc-bridge", "held")).some(file => file.includes("invalid-ask-reply"))));
    assert.equal(messages.some(event => event.message.id === "duplicate"), false);

    let cancelledDeliveryAcks = 0;
    planner.on("delivered", event => { if (event.messageId === "cancelled-ask") cancelledDeliveryAcks++; });
    planner.send("claude:test-cc", { id: "cancelled-ask", expectsReply: true, timestamp: Date.now(), content: { text: "cancelled question" } });
    assert.ok(await waitFor(() => fixture.myLines.some(line => line.includes("cancelled question")) && cancelledDeliveryAcks >= 1));
    const cancelledFrame = parseInboundLine(fixture.myLines.find(line => line.includes("cancelled question"))!);
    assert.equal(cancelledFrame.kind, "user");
    if (cancelledFrame.kind !== "user") return;
    const cancelledToken = /Bridge-Reply: ([0-9a-f-]{36})/.exec(cancelledFrame.text)![1];
    // Exercise the real 0.18.1 broker control frame without exposing a new public API.
    planner["rawSend"]({ type: "cancel_message", messageId: "cancelled-ask" });
    assert.ok(await waitFor(() => cancelledDeliveryAcks >= 2));
    await sleep(100);
    const beforeCancelledReply = readdirSync(join(fixture.agentDir, "pi-cc-bridge", "held")).length;
    await post(`Bridge-Reply: ${cancelledToken}\nlate cancelled reply`, "cancelled-reply");
    assert.ok(await waitFor(() => readdirSync(join(fixture.agentDir, "pi-cc-bridge", "held")).length > beforeCancelledReply));
    assert.equal(messages.some(event => event.message.replyTo === "cancelled-ask"), false);

    await new Promise<void>(resolve => fixture.myServer.close(() => resolve()));
    planner.send("claude:test-cc", { id: "failed-ask", expectsReply: true, timestamp: Date.now(), content: { text: "transport failure" } });
    assert.ok(await waitFor(() => messages.some(event => event.message.replyTo === "failed-ask")));
    assert.match(messages.find(event => event.message.replyTo === "failed-ask")!.message.content.text, /Delivery failed/);
  });
}
