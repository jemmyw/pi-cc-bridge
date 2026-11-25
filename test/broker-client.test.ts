import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { BrokerClient } from "../src/intercom/client.ts";
import { startIsolatedBroker, resolveTsx, waitFor, BROKER_ENTRY, SKIP, SKIP_REASON } from "./helpers.ts";

/**
 * Integration: run a real (isolated) pi-intercom broker under a temp agent dir
 * and verify our client's register/send/receive/receipt/list flow. Skipped when
 * pi-intercom is not installed at the standard path.
 */

function registration(name: string) {
  return {
    name,
    cwd: process.cwd(),
    model: "test",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
    status: "idle",
    acknowledgesReceipts: true,
    livenessIntervalMs: 5_000,
  };
}

test("broker client: register, list, send, receive, receipt, delivered", { skip: SKIP_REASON }, async t => {
  const { agentDir, stop } = await startIsolatedBroker();
  t.after(() => stop());

  const alpha = new BrokerClient(agentDir);
  const beta = new BrokerClient(agentDir);
  t.after(() => {
    alpha.close();
    beta.close();
  });

  await alpha.connectAndRegister("alpha-id", registration("alpha"));
  await beta.connectAndRegister("beta-id", registration("beta"));

  const received = new Promise<{ fromId: string; text: string; messageId: string }>(resolve => {
    beta.on("message", event => {
      resolve({ fromId: event.from.id, text: event.message.content.text, messageId: event.message.id });
      beta.receipt(event.message.id);
    });
  });

  const delivered = new Promise<string>(resolve => {
    alpha.on("delivered", event => resolve(event.messageId));
  });

  // Give the broker a beat to process both registrations.
  await sleep(200);
  alpha.send("beta", { id: "msg-1", timestamp: Date.now(), content: { text: "ping" } });

  const got = await Promise.race([received, sleep(5_000).then(() => null)]);
  assert.ok(got, "beta should receive the message");
  assert.equal(got.fromId, "alpha-id");
  assert.equal(got.text, "ping");
  const delivery = await Promise.race([delivered, sleep(5_000).then(() => null)]);
  assert.equal(delivery, "msg-1");
});

test("exact broker sends never fall back to an alias or a rebound endpoint", { skip: SKIP_REASON }, async t => {
  const { agentDir, stop } = await startIsolatedBroker();
  const [sender, oldTarget, rebound, mirror] = Array.from({ length: 4 }, () => new BrokerClient(agentDir));
  t.after(() => { for (const client of [sender, oldTarget, rebound, mirror]) client.close(); stop(); });
  await sender.connectAndRegister("sender", registration("sender"));
  await mirror.connectAndRegister("cc-mirror", registration("pi-id"));
  const seen = new Promise<import("../src/intercom/client.ts").BrokerSessionInfo>(resolve => {
    sender.on("session_joined", event => { if (event.session.id === "pi-id") resolve(event.session); });
  });
  await oldTarget.connectAndRegister("pi-id", registration("target"));
  const target = await seen;
  assert.equal(sender.supportsExactSend, true);
  assert.ok(target.endpointEpoch);
  const failures: string[] = [];
  const unexpected: string[] = [];
  sender.on("delivery_failed", event => failures.push(event.reason));
  for (const client of [mirror, rebound]) client.on("message", event => unexpected.push(event.message.id));
  const departed = new Promise<void>(resolve => sender.once("session_left", () => resolve()));
  oldTarget.close();
  await departed;
  const send = (id: string) => sender.sendExact(target, { id, timestamp: Date.now(), content: { text: "must not arrive" } });
  send("departed");
  assert.ok(await waitFor(() => failures.length === 1));
  assert.match(failures[0], /not found/i);
  await rebound.connectAndRegister("pi-id", registration("target"));
  send("rebound");
  assert.ok(await waitFor(() => failures.length === 2));
  assert.match(failures[1], /endpoint changed/i);
  assert.deepEqual(unexpected, []);
});
