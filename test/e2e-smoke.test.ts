import { test } from "node:test";
import assert from "node:assert/strict";
import { assertNoPrematureAck, isProbeMessage } from "../scripts/e2e-smoke.ts";
import type { BrokerEvent } from "../src/intercom/client.ts";

test("e2e smoke: only exact broker delivery from the dedicated peer satisfies the nonce", () => {
  const peer = { id: "cc-dedicated-session", name: "claude:dedicated-probe" };
  const nonce = "ACK unique-nonce";
  const event: Extract<BrokerEvent, { type: "message" }> = {
    type: "message",
    from: { ...peer, cwd: "/test", model: "test", pid: 123 },
    message: { id: "message-1", timestamp: 1, content: { text: `Message from Claude Code session dedicated-probe:\n${nonce}` } },
  };
  assert.equal(isProbeMessage(event, peer, nonce), true);
  assert.doesNotThrow(() => assertNoPrematureAck([], peer, nonce));
  assert.throws(() => assertNoPrematureAck([event], peer, nonce), /premature ACK/);
  assert.doesNotThrow(() => assertNoPrematureAck([event], peer, "ACK another-nonce"));
  assert.equal(isProbeMessage({ ...event, from: { ...event.from, id: "cc-other-session" } }, peer, nonce), false);
  assert.equal(isProbeMessage({ ...event, from: { ...event.from, name: "claude:other-probe" } }, peer, nonce), false);
  for (const text of [nonce, `prompt mentions ${nonce}`, `${event.message.content.text} extra`, event.message.content.text.replace("unique-nonce", "old-nonce")]) {
    assert.equal(isProbeMessage({ ...event, message: { ...event.message, content: { text } } }, peer, nonce), false);
  }
});
