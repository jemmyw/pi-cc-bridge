import { test } from "node:test";
import assert from "node:assert/strict";
import { AskReplies, askInstructions, askTimeoutMs } from "../src/ask-replies.ts";

const route = { ccSessionId: "cc-session", ccIdentity: "pid/start/socket", piSessionId: "pi-session" };

test("AskReplies: overlapping asks resolve only the explicit token and route", () => {
  const asks = new AskReplies(1000);
  const first = asks.add(route, "ask-1", 0);
  const second = asks.add(route, "ask-2", 0);
  assert.match(askInstructions(first), new RegExp(first));
  assert.equal(asks.match(route, "unrelated notification", 1), undefined);
  for (const other of [{ ...route, ccSessionId: "wrong" }, { ...route, ccIdentity: "reused-pid" }, { ...route, piSessionId: "wrong" }]) {
    assert.equal(asks.match(other, `Bridge-Reply: ${first}\nanswer`, 1), undefined);
  }
  assert.equal(asks.match(route, `Bridge-Reply: ${second}\nsecond`, 1)?.replyTo, "ask-2");
  asks.cancel(second);
  assert.equal(asks.match(route, `Bridge-Reply: ${second}\nsecond duplicate`, 1), undefined);
  assert.equal(asks.match(route, `Bridge-Reply: ${first}\nfirst`, 1)?.replyTo, "ask-1");
  assert.equal(asks.match(route, `Bridge-Reply: ${first}\nfirst late`, 1000), undefined);
  const next = asks.add(route, "ask-3", 1001);
  assert.notEqual(next, first);
  assert.equal(asks.match(route, `Bridge-Reply: ${first}\nlate`, 1002), undefined);
  assert.equal(asks.match(route, `Bridge-Reply: ${next}\nnew`, 1002)?.replyTo, "ask-3");
});

test("AskReplies: cancellation, lifecycle cleanup and capacity never evict another ask", () => {
  const asks = new AskReplies(1000, 2);
  const token = asks.add(route, "ask-1", 0);
  const other = asks.add({ ...route, ccSessionId: "other", piSessionId: "other" }, "ask-2", 0);
  assert.throws(() => asks.add(route, "ask-3", 0), /limit/);
  assert.equal(asks.match(route, `Bridge-Reply: ${token}\nanswer`, 1)?.replyTo, "ask-1");
  asks.clearCc(route.ccSessionId);
  assert.equal(asks.match(route, `Bridge-Reply: ${token}\nanswer`, 1), undefined);
  asks.clearPi("other");
  assert.equal(asks.match({ ...route, ccSessionId: "other", piSessionId: "other" }, `Bridge-Reply: ${other}\nanswer`, 1), undefined);
  const cancelled = asks.add(route, "failed transport", 0);
  asks.cancel(cancelled);
  assert.equal(asks.match(route, `Bridge-Reply: ${cancelled}\nanswer`, 1), undefined);
  const controlled = asks.add(route, "cancel-message", 0);
  asks.cancelMessage(route.ccSessionId, "wrong sender", "cancel-message");
  assert.ok(asks.match(route, `Bridge-Reply: ${controlled}\nanswer`, 1));
  asks.cancelMessage(route.ccSessionId, route.piSessionId, "cancel-message");
  assert.equal(asks.match(route, `Bridge-Reply: ${controlled}\nanswer`, 1), undefined);
  const stopped = asks.add(route, "stopped", 0);
  asks.clear();
  assert.equal(asks.match(route, `Bridge-Reply: ${stopped}\nanswer`, 1), undefined);
});

test("askTimeoutMs: matches pi-intercom default and environment validation", t => {
  const old = process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
  t.after(() => {
    if (old === undefined) delete process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
    else process.env.PI_INTERCOM_ASK_TIMEOUT_MS = old;
  });
  delete process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
  assert.equal(askTimeoutMs(), 600_000);
  process.env.PI_INTERCOM_ASK_TIMEOUT_MS = "2500";
  assert.equal(askTimeoutMs(), 2500);
  for (const raw of ["0", "-1", "NaN", "1.5"]) {
    process.env.PI_INTERCOM_ASK_TIMEOUT_MS = raw;
    assert.throws(askTimeoutMs, /positive integer/);
  }
});
