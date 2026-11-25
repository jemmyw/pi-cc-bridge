import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync, readdirSync, rmSync, symlinkSync } from "node:fs";
import { createFrameReader, writeFrame } from "../src/intercom/framing.ts";
import { procStart } from "../src/cc/registry.ts";
import { tmpdir } from "node:os";
import { join } from "node:path";
import net from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import { buildSync } from "esbuild";
import { startIsolatedBroker, waitFor, SKIP_REASON } from "./helpers.ts";
import { BrokerClient, type BrokerEvent } from "../src/intercom/client.ts";
import { buildOutboundLine, parseInboundLine } from "../src/cc/protocol.ts";

const PKG_DIR = new URL("..", import.meta.url).pathname;
const BRIDGE_NAME = "test-bridge";
const DISCOVERY_HINT = '[pi-cc-bridge] Send /list to the bridge peer "test-bridge" to find reachable Pi sessions. Send messages to this same peer with a first-line To: address.';

interface Fixture {
  agentDir: string;
  sessionsDir: string;
  socketDir: string;
  mySocketPath: string;
  myServer: net.Server;
  myLines: string[];
  cleanup: () => void;
}

async function setupFixture(config = {}, startBroker = startIsolatedBroker): Promise<Fixture> {
  const { agentDir, stop: stopBroker } = await startBroker();
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
      procStart: procStart(),
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
    JSON.stringify({ enabled: true, peerName: BRIDGE_NAME, pollMs: 250, ccSessionsDir: sessionsDir, ccSocketDir: socketDir, ...config }),
  );
  const cleanup = () => {
    myServer.close();
    stopBroker();
    rmSync(agentDir, { recursive: true, force: true });
  };
  return { agentDir, sessionsDir, socketDir, mySocketPath, myServer, myLines, cleanup };
}

function startDaemon(agentDir: string): ReturnType<typeof spawn> {
  const outfile = join(agentDir, "cli.mjs");
  symlinkSync(join(PKG_DIR, "node_modules"), join(agentDir, "node_modules"), "dir");
  buildSync({ entryPoints: [join(PKG_DIR, "src", "cli.ts")], external: ["fs-ext"], bundle: true, platform: "node", format: "esm", outfile });
  return spawn(process.execPath, [outfile], {
    env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, NODE_NO_WARNINGS: "1" },
    stdio: ["ignore", "ignore", "pipe"],
  });
}

async function writeCcLine(socketPath: string, line: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    socket.once("connect", () => socket.end(`${line}\n`));
    socket.once("error", reject);
    socket.once("close", () => resolve());
  });
}

function bridgeRecord(fixture: Fixture, pid: number | undefined) {
  return JSON.parse(readFileSync(join(fixture.sessionsDir, `${pid}.json`), "utf-8"));
}

function findFrame(fixture: Fixture, match: (frame: Extract<ReturnType<typeof parseInboundLine>, { kind: "user" }>) => boolean) {
  return fixture.myLines.map(parseInboundLine).find(frame => frame.kind === "user" && match(frame)) as
    Extract<ReturnType<typeof parseInboundLine>, { kind: "user" }> | undefined;
}

function post(fixture: Fixture, daemon: ReturnType<typeof startDaemon>, text: string, msgId: string,
  senderMode = "prompting", senderSocketPath = fixture.mySocketPath) {
  const record = bridgeRecord(fixture, daemon.pid);
  return writeCcLine(record.messagingSocketPath, buildOutboundLine({ senderSocketPath, senderName: "claimed-name",
    text, msgId, senderMode, targetSessionId: record.sessionId }));
}

const listLines = (text: string) => text.split("\n").filter(line => line.startsWith("To: "));

test("single bridge: cold discovery, exact round trips and no implicit or synthetic routes", { skip: SKIP_REASON }, async t => {
  const fixture = await setupFixture({ routeTo: "planner", expose: ["*"], maxHolders: 8, relayBurst: 2, relayPerMin: 1 });
  const clients: BrokerClient[] = [];
  const relayed: { recipient: string; event: Extract<BrokerEvent, { type: "message" }> }[] = [];
  let peerId = "";
  let daemon: ReturnType<typeof startDaemon>;
  t.after(async () => {
    for (const client of clients) client.close();
    if (daemon) {
      daemon.kill("SIGTERM");
      assert.ok(await waitFor(() => daemon.exitCode !== null));
      assert.equal(existsSync(join(fixture.sessionsDir, `${daemon.pid}.json`)), false);
      assert.equal(existsSync(join(fixture.socketDir, `${daemon.pid}.sock`)), false);
    }
    fixture.cleanup();
  });
  const register = async (id: string, name: string) => {
    const client = new BrokerClient(fixture.agentDir);
    clients.push(client);
    client.on("session_joined", event => { if (event.session.name === "claude:test-cc") peerId = event.session.id; });
    client.on("message", event => { relayed.push({ recipient: id, event }); client.receipt(event.message.id); });
    await client.connectAndRegister(id, { name, cwd: "/private-pi-cwd", model: "private-pi-model", pid: process.pid,
      startedAt: Date.now(), lastActivity: Date.now(), acknowledgesReceipts: true });
    return client;
  };
  const planner = await register("planner-id", "planner");
  daemon = startDaemon(fixture.agentDir);
  assert.ok(await waitFor(() => Boolean(peerId)));
  const list = async (id: string, mode = "bypass") => {
    await post(fixture, daemon, "/list", id, mode);
    const get = () => findFrame(fixture, frame => frame.line.msg_id === `list-${id}`);
    assert.ok(await waitFor(() => Boolean(get())));
    const frame = get()!;
    assert.equal(frame.meta.fromName, BRIDGE_NAME);
    assert.equal(frame.line.from, bridgeRecord(fixture, daemon.pid).messagingSocketPath);
    assert.doesNotMatch(frame.text, /private-pi|claude:test-cc|cc-[0-9a-f]{8}-/);
    return frame.text;
  };
  const reject = async (text: string, reason: string, mode = "prompting") => {
    const count = fixture.myLines.length;
    await post(fixture, daemon, text, randomUUID(), mode);
    assert.ok(await waitFor(() => fixture.myLines.length > count));
    const frame = parseInboundLine(fixture.myLines[count]);
    assert.ok(frame.kind === "user");
    assert.match(frame.text, /Not delivered:.*Message saved in held\//);
    assert.ok(frame.text.includes(reason), frame.text);
  };

  const firstList = await list("cold");
  assert.ok(firstList.startsWith(`${DISCOVERY_HINT}\n\n`));
  assert.deepEqual(listLines(firstList), ["To: planner"]);
  await reject("no header with sole session", "missing or invalid first-line");
  await reject("To: absent\nmiss with sole session", "matches no reachable Pi session");
  await reject("To: planner\ntrust hold", "held by policy", "bypass");

  const unvalidatedPath = join(fixture.socketDir, "unregistered.sock");
  const unexpectedReplies: Buffer[] = [];
  const unvalidated = net.createServer(socket => socket.on("data", chunk => unexpectedReplies.push(chunk)));
  await new Promise<void>(resolve => unvalidated.listen(unvalidatedPath, resolve));
  t.after(() => unvalidated.close());
  for (const [id, text] of [["untrusted-list", "/list"], ["untrusted-relay", "To: planner\nnot trusted"]]) {
    await post(fixture, daemon, text, id, "prompting", unvalidatedPath);
  }
  const heldDir = join(fixture.agentDir, "pi-cc-bridge", "held");
  assert.ok(await waitFor(() => readdirSync(heldDir).filter(file => file.includes("unknown-sender")).length === 2));
  assert.deepEqual(unexpectedReplies, []);
  assert.equal(relayed.length, 0);

  const duplicateId = 'worker "quoted"';
  const worker = await register(duplicateId, "PLANNER");
  const secondList = await list("duplicates", "unknown");
  const addresses = listLines(secondList);
  assert.deepEqual(addresses, ['To: "planner-id"', `To: ${JSON.stringify(duplicateId)}`]);
  await reject("To: planner\nambiguous alias", "is ambiguous");
  await reject(`To: ${peerId}\nnever relay to a mirror`, "matches no reachable Pi session");
  assert.equal(relayed.length, 0);

  for (const [i, address] of addresses.entries()) {
    const body = `exact payload ${i}\nnewline, trailing space `;
    await post(fixture, daemon, `${address}\n${body}`, `in-${i}`);
    assert.ok(await waitFor(() => relayed.length === i + 1));
    const { recipient, event } = relayed[i];
    assert.equal(recipient, i === 0 ? "planner-id" : duplicateId);
    assert.equal(event.from.id, peerId);
    assert.equal(event.from.name, "claude:test-cc");
    assert.equal(event.message.content.text, `Message from Claude Code session test-cc:\n${body}`);
  }
  for (const [i, client] of [planner, worker].entries()) {
    const body = `original payload ${i}\nTo: this-is-content\n `;
    client.send(peerId, { id: `out-${i}`, timestamp: Date.now(), content: { text: body } });
    const get = () => findFrame(fixture, f => f.line.msg_id === `out-${i}`);
    assert.ok(await waitFor(() => Boolean(get())));
    const frame = get()!;
    assert.equal(frame.meta.fromName, BRIDGE_NAME);
    assert.equal(frame.meta.fromMode, "bypass");
    assert.equal(frame.line.from, bridgeRecord(fixture, daemon.pid).messagingSocketPath);
    const [instruction, message] = frame.text.split("\n\nPi message:\n");
    assert.ok(instruction.includes(`send to "${BRIDGE_NAME}" with this exact first line:`));
    assert.deepEqual(listLines(instruction), [addresses[i]]);
    assert.equal(message, body);
    await post(fixture, daemon, `${listLines(instruction)[0]}\nreply ${i}`, `reply-${i}`);
    assert.ok(await waitFor(() => relayed.some(({ event }) => event.message.id === `reply-${i}`)));
    assert.equal(relayed.find(({ event }) => event.message.id === `reply-${i}`)!.recipient, i === 0 ? "planner-id" : duplicateId);
  }

  await reject(`${addresses[1]}\nthird delivery to the worker`, "bridge relay rate limit reached");
  planner.close();
  const replacement = await register("alias-reuse", "planner-id");
  await reject('To: "planner-id"\nnever use the replacement alias', "matches no reachable Pi session");
  assert.equal(relayed.length, 4);
  assert.ok(replacement.isRegistered);
  assert.deepEqual(readdirSync(fixture.sessionsDir).sort(), [`${daemon.pid}.json`, `${process.pid}.json`].sort());
  assert.deepEqual(readdirSync(fixture.socketDir).sort(), [`${daemon.pid}.sock`, `${process.pid}.sock`, "unregistered.sock"].sort());
  assert.equal(existsSync(join(fixture.agentDir, "pi-cc-bridge", "holders-ipc.sock")), false);
  const children = execFileSync("/bin/ps", ["-axo", "ppid="], { encoding: "utf-8" }).split(/\s+/);
  assert.ok(!children.includes(String(daemon.pid)), "daemon runs no helper processes");
});

test("ask correlation through the bridge: explicit tokens and broker replyTo", { skip: SKIP_REASON }, async t => {
  const fixture = await setupFixture({ relayBurst: 32, relayPerMin: 120 });
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
  const daemonRecord = JSON.parse(readFileSync(join(fixture.sessionsDir, `${daemon.pid}.json`), "utf-8"));
  const inbox = daemonRecord.messagingSocketPath;
  const post = async (text: string, msgId: string) => {
    const line = buildOutboundLine({ senderSocketPath: fixture.mySocketPath, senderName: "renamed-cc-label",
      senderMode: "prompting", text: `To: "planner-id"\n${text}`, msgId });
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
    assert.equal(frame.line.from, inbox);
    assert.equal(frame.meta.fromName, BRIDGE_NAME);
    assert.ok(frame.text.startsWith(n === 1 ? `${DISCOVERY_HINT}\n\nFrom Pi session ` : "From Pi session "));
    assert.match(frame.text, /\nTo: "planner-id"\n\[Bridge ask: /);
    assert.ok(frame.text.endsWith(`\n\nPi message:\nquestion-${n}`));
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

for (const exact of [false, true]) {
  test(exact ? "bridge holds the original payload and notifies on broker endpoint failure"
    : "legacy broker: /list and Pi to Claude work, Claude relay holds without exact-send", async t => {
    const pi = { id: "pi-id", endpointEpoch: "pi-epoch", name: "planner", model: "test", cwd: "/test", pid: process.pid };
    const attempted: Record<string, unknown>[] = [];
    const fixture = await setupFixture({}, async () => {
      const agentDir = mkdtempSync(join(tmpdir(), "ccbridge-protocol-"));
      mkdirSync(join(agentDir, "intercom"));
      const broker = net.createServer(socket => {
        let firstList = true;
        socket.on("data", createFrameReader(raw => {
          const frame = JSON.parse(raw);
          if (frame.type === "register") writeFrame(socket, { type: "registered", sessionId: frame.sessionId, features: exact ? ["exact-send-v1"] : [] });
          if (frame.type === "list") {
            writeFrame(socket, { type: "sessions", requestId: frame.requestId, sessions: [pi] });
            if (firstList) writeFrame(socket, { type: "message", from: pi, message: { id: "pi-post", timestamp: Date.now(), content: { text: "payload" } } });
            firstList = false;
          }
          if (frame.type === "send") {
            attempted.push(frame);
            writeFrame(socket, { type: "delivery_failed", messageId: frame.message.id, reason: "Target endpoint changed before delivery" });
          }
        }, error => socket.destroy(error)));
      });
      await new Promise<void>(resolve => broker.listen(join(agentDir, "intercom", "broker.sock"), resolve));
      return { agentDir, stop: () => broker.close() };
    });
    const daemon = startDaemon(fixture.agentDir);
    t.after(async () => {
      daemon.kill("SIGTERM");
      await waitFor(() => daemon.exitCode !== null);
      fixture.cleanup();
    });
    assert.ok(await waitFor(() => Boolean(findFrame(fixture, frame => frame.line.msg_id === "pi-post"))));
    await post(fixture, daemon, "/list", "query", "bypass");
    const list = () => findFrame(fixture, frame => frame.line.msg_id === "list-query");
    assert.ok(await waitFor(() => Boolean(list())));
    assert.deepEqual(listLines(list()!.text), ["To: planner"]);
    const original = 'To: "pi-id"\nMust hold';
    await post(fixture, daemon, original, "failed-relay");
    const error = exact ? "Target endpoint changed before delivery" : "does not support exact endpoint delivery";
    assert.ok(await waitFor(() => Boolean(findFrame(fixture, frame => frame.text.includes(error)))));
    const dir = join(fixture.agentDir, "pi-cc-bridge", "held");
    const reason = exact ? "delivery_failed" : "exact-send-unavailable";
    const hold = readdirSync(dir).find(file => file.includes(reason));
    assert.ok(hold);
    assert.equal(JSON.parse(readFileSync(join(dir, hold), "utf-8")).text, original);
    assert.equal(attempted.length, exact ? 1 : 0);
    if (exact) {
      assert.equal(attempted[0].targetId, pi.id);
      assert.equal(attempted[0].targetEpoch, pi.endpointEpoch);
    }
  });
}
