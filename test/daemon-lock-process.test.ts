import { test } from "node:test";
import assert from "node:assert/strict";
import { fork, type ChildProcess } from "node:child_process";
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import { releaseLock, tryLock } from "../src/daemon-lock.ts";
import { resolveTsx, waitFor } from "./helpers.ts";

for (const stale of [false, true]) {
  test(`daemon flock: deterministic competing processes, ${stale ? "stale" : "fresh"} metadata`, { timeout: 20_000 }, async t => {
    const dir = mkdtempSync(join(tmpdir(), "ccbridge-flock-"));
    const path = join(dir, "daemon.lock");
    const children: ChildProcess[] = [];
    t.after(async () => {
      for (const child of children) {
        if (child.exitCode !== null || child.signalCode !== null) continue;
        const exited = once(child, "exit");
        child.kill("SIGKILL");
        await exited;
      }
      rmSync(dir, { recursive: true, force: true });
    });
    if (stale) writeFileSync(path, "99999999 unknown", { mode: 0o600 });
    const child = async (pause = false) => {
      const process = fork(new URL("./fixtures/daemon-lock-child.ts", import.meta.url), [path, pause ? "pause-after-read" : ""], {
        execArgv: ["--import", resolveTsx()!], stdio: ["ignore", "ignore", "pipe", "ipc"],
      });
      children.push(process);
      assert.deepEqual((await once(process, "message"))[0], { ready: true });
      return process;
    };
    const contenders = await Promise.all([child(stale), child(stale), child(stale), child(stale)]);
    const inspected = new Set<ChildProcess>();
    const results = new Map<ChildProcess, boolean>();
    for (const process of contenders) {
      process.on("message", (message: { inspected?: boolean; acquired?: boolean }) => {
        if (message.inspected) inspected.add(process);
        if (typeof message.acquired === "boolean") results.set(process, message.acquired);
      });
      process.send("acquire");
    }
    if (stale) {
      assert.ok(await waitFor(() => inspected.size + results.size === contenders.length));
      // Reclaimers that read stale metadata must resume after the first winner publishes.
      // Without flock, this schedule deterministically steals the first winner's lock.
      for (const process of inspected) {
        writeFileSync(`${path}.resume.${process.pid}`, "resume");
        assert.ok(await waitFor(() => results.has(process)));
      }
    }
    assert.ok(await waitFor(() => results.size === contenders.length));
    assert.equal([...results.values()].filter(Boolean).length, 1);
    const winner = contenders.find(process => results.get(process))!;
    assert.ok(readFileSync(path, "utf-8").startsWith(`${winner.pid} `));
    const inode = statSync(`${path}.guard`).ino;

    // SIGKILL cannot run JS cleanup. The kernel must release the permanent guard.
    const crashed = once(winner, "exit");
    winner.kill("SIGKILL");
    await crashed;
    const successor = await child();
    const result = once(successor, "message");
    successor.send("acquire");
    assert.deepEqual((await result)[0], { acquired: true });
    const released = once(successor, "exit");
    successor.send("release");
    await released;
    assert.equal(existsSync(path), false);
    assert.equal(statSync(`${path}.guard`).ino, inode, "never unlink the guard inode");
    const normalSuccessor = await child();
    const next = once(normalSuccessor, "message");
    normalSuccessor.send("acquire");
    assert.deepEqual((await next)[0], { acquired: true });
  });
}

test("daemon flock: refuses symlink and non-private guard without changing target", t => {
  const dir = mkdtempSync(join(tmpdir(), "ccbridge-flock-private-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "daemon.lock");
  const target = join(dir, "target");
  writeFileSync(target, "unchanged", { mode: 0o600 });
  symlinkSync(target, `${path}.guard`);
  assert.throws(() => tryLock(path, () => {}));
  assert.equal(readFileSync(target, "utf-8"), "unchanged");
  rmSync(`${path}.guard`);
  writeFileSync(`${path}.guard`, "", { mode: 0o600 });
  chmodSync(`${path}.guard`, 0o644);
  assert.throws(() => tryLock(path, () => {}), /non-private/);
  assert.equal(existsSync(path), false);
});

test("daemon flock: cleanup does not unlink replaced owner metadata", t => {
  const dir = mkdtempSync(join(tmpdir(), "ccbridge-flock-cleanup-"));
  const path = join(dir, "daemon.lock");
  t.after(() => { releaseLock(path); rmSync(dir, { recursive: true, force: true }); });
  assert.equal(tryLock(path, () => {}), true);
  writeFileSync(path, "123 changed identity");
  releaseLock(path);
  assert.equal(readFileSync(path, "utf-8"), "123 changed identity");
});

test("daemon flock: refuses foreign-owned guard", t => {
  const dir = mkdtempSync(join(tmpdir(), "ccbridge-flock-owner-"));
  const path = join(dir, "daemon.lock");
  const original = fs.fstatSync;
  t.after(() => {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    rmSync(dir, { recursive: true, force: true });
  });
  t.mock.method(fs, "fstatSync", ((fd: number) => {
    const stat = original(fd);
    stat.uid = (process.getuid?.() ?? 0) + 1;
    return stat;
  }) as typeof fs.fstatSync);
  syncBuiltinESMExports();
  assert.throws(() => tryLock(path, () => {}), /non-private/);
  assert.equal(existsSync(path), false);
});
