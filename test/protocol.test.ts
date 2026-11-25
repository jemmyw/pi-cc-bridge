import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import net from "node:net";
import { join } from "node:path";
import { CcDiscovery } from "../src/cc/discovery.ts";
import { parseInboundLine, buildOutboundLine, unwrapCrossSessionMessage, buildAuthLine } from "../src/cc/protocol.ts";

/**
 * Fixtures: the exact line a real Claude Code 2.1.296 session sent to our
 * masqueraded inbox during the mirror probe (2026-10-11), plus a synthesized
 * outbound round trip. Update fixtures only from a new live capture.
 */

const REAL_CC_SENDER_LINE = JSON.stringify({
  msgV: 1,
  msg_id: "d2f9384c-b448-4c76-8fb1-24afb084de2f",
  type: "user",
  message: {
    role: "user",
    content:
      '<cross-session-message from="uds:/run/user/1000/cc-socks/1920100.sock" from-name="systemfix-3c" from-mode="prompting">\nBRIDGE-HELLO-42\n</cross-session-message>',
  },
  priority: "next",
  from: "/run/user/1000/cc-socks/1920100.sock",
});

test("parses a real CC sender line: text, meta, envelope", () => {
  const parsed = parseInboundLine(REAL_CC_SENDER_LINE);
  assert.equal(parsed.kind, "user");
  if (parsed.kind !== "user") return;
  assert.equal(parsed.text, "BRIDGE-HELLO-42");
  assert.equal(parsed.line.msg_id, "d2f9384c-b448-4c76-8fb1-24afb084de2f");
  assert.equal(parsed.line.priority, "next");
  assert.equal(parsed.meta.from, "uds:/run/user/1000/cc-socks/1920100.sock");
  assert.equal(parsed.meta.fromName, "systemfix-3c");
  assert.equal(parsed.meta.fromMode, "prompting");
});

test("unwrapper leaves plain text untouched", () => {
  const { text, meta } = unwrapCrossSessionMessage("just words");
  assert.equal(text, "just words");
  assert.deepEqual(meta, {});
});

test("buildOutboundLine matches the captured CC shape and round-trips", () => {
  const line = buildOutboundLine({
    senderSocketPath: "/run/user/1000/cc-socks/999.sock",
    senderName: "pi-bridge",
    senderMode: "prompting",
    text: "hello from pi",
    msgId: "m-123",
    targetSessionId: "41f88b21-ee42-406e-bacd-f0efedc4a15b",
  });
  const raw = JSON.parse(line);
  assert.equal(raw.msgV, 1);
  assert.equal(raw.type, "user");
  assert.equal(raw.priority, "next");
  assert.equal(raw.from, "/run/user/1000/cc-socks/999.sock");
  assert.equal(raw.session_id, "41f88b21-ee42-406e-bacd-f0efedc4a15b");
  assert.match(raw.message.content, /^<cross-session-message from="uds:\/run\/user\/1000\/cc-socks\/999\.sock" from-name="pi-bridge" from-mode="prompting">\nhello from pi\n<\/cross-session-message>$/);

  const parsed = parseInboundLine(line);
  assert.equal(parsed.kind, "user");
  if (parsed.kind !== "user") return;
  assert.equal(parsed.text, "hello from pi");
  assert.equal(parsed.meta.fromName, "pi-bridge");
  assert.equal(parsed.line.session_id, "41f88b21-ee42-406e-bacd-f0efedc4a15b");
});

test("rejections mirror CC's inbox reasons", () => {
  assert.equal(parseInboundLine("{}").kind, "unknown"); // no type field
  assert.equal(parseInboundLine('{"type":"user"}').kind, "unknown"); // missing content
  const noId = parseInboundLine('{"type":"user","message":{"role":"user","content":"hi"}}');
  assert.equal(noId.kind, "user"); // CC synthesizes ids; so do we
  const oversized = parseInboundLine(
    JSON.stringify({ type: "user", msg_id: "y".repeat(200), message: { role: "user", content: "x" } }),
  );
  assert.equal(oversized.kind, "user"); // falls back to a synthesized id
  const action = parseInboundLine('{"action":"notify_when_idle","from":"uds:/x","msg_id":"z"}');
  assert.equal(action.kind, "action");
  if (action.kind === "action") assert.equal(action.action, "notify_when_idle");
  assert.equal(parseInboundLine('{"type":"auth","token":"t"}').kind, "auth");
});

test("discovery retries the hint after a failed post and tracks sessions, not sockets", async t => {
  const dir = mkdtempSync("/tmp/cc-discovery-");
  const socketPath = join(dir, "inbox.sock");
  const texts: string[] = [];
  const server = net.createServer(socket => {
    let line = "";
    socket.setEncoding("utf-8");
    socket.on("data", chunk => { line += chunk; });
    socket.on("end", () => {
      const frame = parseInboundLine(line);
      if (frame.kind === "user") texts.push(frame.text);
    });
  });
  t.after(async () => {
    if (server.listening) await new Promise<void>(resolve => server.close(() => resolve()));
    rmSync(dir, { recursive: true, force: true });
  });
  const discovery = new CcDiscovery("pi-bridge");
  const post = (sessionId: string, text: string) => discovery.post(sessionId, {
    socketPath, senderSocketPath: join(dir, "sender.sock"), senderName: "pi-planner", text, msgId: text, settleMs: 0,
  });
  await assert.rejects(post("cc-a", "not delivered"), { code: "ENOENT" });
  await new Promise<void>((resolve, reject) => server.once("error", reject).listen(socketPath, resolve));
  await post("cc-a", "first delivery");
  await post("cc-a", "second delivery");
  await post("cc-b", "new session");
  await new Promise<void>(resolve => server.close(() => resolve()));
  assert.equal(texts.length, 3);
  assert.match(texts[0], /^\[pi-cc-bridge\].+\n\nfirst delivery$/);
  assert.equal(texts[1], "second delivery");
  assert.match(texts[2], /^\[pi-cc-bridge\].+\n\nnew session$/);
});

test("auth line shape", () => {
  assert.deepEqual(JSON.parse(buildAuthLine("t")), { type: "auth", token: "t" });
});
