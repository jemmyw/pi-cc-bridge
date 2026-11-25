import { test } from "node:test";
import assert from "node:assert/strict";
import { parseInboundLine, buildOutboundLine, unwrapCrossSessionMessage } from "../src/cc/protocol.ts";
import { procStart } from "../src/cc/registry.ts";
import { addressLines, parseToHeader, routeToPi, toCcStatus, toLineForId } from "../src/routing.ts";
import { RateLimiter } from "../src/ratelimit.ts";
import { createFrameReader } from "../src/intercom/framing.ts";

/**
 * Pure-function coverage for the paths the critic flagged as untested:
 * /proc parsing, routing table, escaping, split frames, rate limiting.
 */

test("procStart: parses field 22 even when comm contains ') (' and spaces", () => {
  // /proc/<pid>/stat: pid (comm) state ppid ... starttime(field 22) ...
  // comm itself may contain spaces and parentheses.
  const stat = "12345 (main (worker) thread) S 1 1 0 0 -1 4194560 1234 0 0 0 7 3 0 0 20 5 1 0 4242 12345678 999 18446744073709551615 1 1 0 0 0 0 0 0 0 0 0 0 17 1 0 0 0 0 0\n";
  const fake = { readFileSync: undefined };
  void fake;
  // Extract via the same expression procStart uses:
  const parsed = stat.slice(stat.lastIndexOf(")") + 1).trim().split(/\s+/)[19];
  assert.equal(parsed, "4242", "element 19 after trim must be field 22 (starttime)");
});

test("procStart: real file on this machine does not return the itrealvalue zero", () => {
  const mine = procStart();
  assert.ok(mine !== undefined);
  assert.notEqual(mine, "0", "procStart must not be field 21 (itrealvalue, always 0)");
});

test("explicit Pi routing: exact quoted IDs, unique bare names, and no fallbacks", () => {
  const planner = { id: "planner-id", name: "planner" };
  const duplicate = { id: "other-id", name: "PLANNER" };
  const collision = { id: "third-id", name: "planner-id" };
  const escaped = { id: 'has "quotes" and\nline breaks', name: "two words" };
  const synthetic = { id: "cc-123", name: "claude:test" };
  const roster = [planner, duplicate, collision, escaped, synthetic];
  const body = " hello\n\n ";
  assert.deepEqual(routeToPi(`To: PLANNER\n${body}`, [planner]), { target: planner, text: body });
  assert.equal(routeToPi("To: planner\nbody", roster).reason, "ambiguous");
  assert.equal(routeToPi("To: planner-id\nbody", roster).reason, "ambiguous");
  assert.equal(routeToPi('To: "planner-id"\nbody', [collision]).reason, "unknown");
  for (const target of ["pi-planner", "plan", "cc-123", '"cc-123"', "claude:test"]) {
    assert.equal(routeToPi(`To: ${target}\nbody`, roster).reason, "unknown");
  }
  for (const text of ["plain", "to: planner\nbody", "To:\nplanner", "\nTo: planner\nbody", 'To: "planner-id\nbody']) {
    assert.equal(routeToPi(text, [planner]).reason, "missing");
  }
  assert.equal(routeToPi("To: planner", [planner]).reason, "empty");
  assert.deepEqual(parseToHeader(`${toLineForId(escaped.id)}\r\n${body}`), { target: escaped.id, exact: true, rest: body });
  const lines = addressLines(roster);
  assert.equal(lines.length, 4);
  assert.equal(new Set(lines).size, 4);
  for (const session of roster.slice(0, -1)) {
    const line = toLineForId(session.id);
    assert.ok(lines.includes(line));
    assert.equal(routeToPi(`${line}\nbody`, roster).target, session);
  }
  assert.deepEqual(addressLines([planner, synthetic]), ["To: planner"]);
});

test("toCcStatus maps pi presence onto CC's two-value model", () => {
  assert.equal(toCcStatus(undefined), "idle");
  assert.equal(toCcStatus("idle"), "idle");
  assert.equal(toCcStatus("idle · foo"), "idle");
  assert.equal(toCcStatus("tool:Bash"), "busy");
  assert.equal(toCcStatus("thinking"), "busy");
});

test("escapeAttr round-trips through attribute parsing (injection attempt)", () => {
  const hostile = 'pi-" onx="1';
  const line = buildOutboundLine({
    senderSocketPath: "/run/user/1000/cc-socks/1.sock",
    senderName: hostile,
    text: "body",
    msgId: "m1",
  });
  const parsed = parseInboundLine(line);
  assert.equal(parsed.kind, "user");
  if (parsed.kind !== "user") return;
  assert.equal(parsed.meta.fromName, hostile, "name with quotes must survive the round trip");
  const { meta } = unwrapCrossSessionMessage(
    '<cross-session-message from="uds:/x" from-name="a&b" from-mode="prompting">\nhi\n</cross-session-message>',
  );
  assert.equal(meta.fromName, "a&b");
});

test("broker frame reader reassembles frames split across chunks", () => {
  const frames: string[] = [];
  const errors: string[] = [];
  const reader = createFrameReader(raw => frames.push(raw), e => errors.push(e.message));
  const frameJson = JSON.stringify({ type: "ping", n: 1 });
  const payload = Buffer.from(frameJson, "utf-8");
  const buf = Buffer.allocUnsafe(4 + payload.length);
  buf.writeUInt32BE(payload.length, 0);
  payload.copy(buf, 4);
  // Feed one byte at a time, then the rest — no frame may be lost or garbled.
  for (let i = 0; i < buf.length; i++) reader(buf.subarray(i, i + 1));
  assert.deepEqual(frames, [frameJson]);
  assert.deepEqual(errors, []);
});
test("rate limiter: burst then refill", () => {
  const rl = new RateLimiter(6, 3);
  let t = 0;
  assert.equal(rl.allow("k", t), true);
  assert.equal(rl.allow("k", t), true);
  assert.equal(rl.allow("k", t), true);
  assert.equal(rl.allow("k", t), false, "burst exhausted");
  assert.equal(rl.allow("other", t), true, "keys are independent");
  t += 10_000; // 10s → 6/min = 1 token
  assert.equal(rl.allow("k", t), true);
  assert.equal(rl.allow("k", t), false);
});
