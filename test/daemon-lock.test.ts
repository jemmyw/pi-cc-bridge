import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { tryLock } from "../src/daemon-lock.ts";

for (const { name, stored, actual, alive, expected } of [
  { name: "matching Darwin timestamp with spaces", stored: "Thu Nov 12 23:18:35 2026", actual: "Thu Nov 12 23:18:35 2026", alive: true, expected: false },
  { name: "matching Linux ticks", stored: "4242", actual: "4242", alive: true, expected: false },
  { name: "unknown stored and unavailable actual identity", stored: "unknown", actual: undefined, alive: true, expected: false },
  { name: "known stored and unavailable actual identity", stored: "4242", actual: undefined, alive: true, expected: false },
  { name: "unknown stored and known actual identity", stored: "unknown", actual: "4242", alive: true, expected: false },
  { name: "reused pid", stored: "4242", actual: "4243", alive: true, expected: true },
  { name: "dead owner", stored: "unknown", actual: undefined, alive: false, expected: true },
]) {
  test(`tryLock: ${name}`, t => {
    const dir = mkdtempSync(join(tmpdir(), "ccbridge-lock-"));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const path = join(dir, "daemon.lock");
    const original = `123 ${stored}`;
    writeFileSync(path, original, { mode: 0o600 });
    assert.equal(tryLock(path, () => {}, { isPidAlive: () => alive, procStart: () => actual }), expected);
    assert.equal(readFileSync(path, "utf-8"), expected ? `${process.pid} ${actual ?? "unknown"}` : original);
    assert.equal(statSync(path).mode & 0o777, 0o600);
  });
}

test("tryLock: acquires once and refuses a second live owner", t => {
  const dir = mkdtempSync(join(tmpdir(), "ccbridge-lock-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "daemon.lock");
  assert.equal(tryLock(path, () => {}), true);
  assert.equal(tryLock(path, () => {}), false);
});

test("tryLock: malformed locks are not evidence of a dead owner", t => {
  const dir = mkdtempSync(join(tmpdir(), "ccbridge-lock-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "daemon.lock");
  for (const content of ["", "0 unknown", "-1 unknown", "not-a-pid unknown", "123"]) {
    writeFileSync(path, content);
    assert.equal(tryLock(path, () => {}), false);
    assert.equal(readFileSync(path, "utf-8"), content);
  }
});
