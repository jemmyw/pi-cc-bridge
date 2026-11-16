import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import { ccSocketDir } from "../src/cc/paths.ts";
import { pidDomainFor, procStart } from "../src/cc/registry.ts";

for (const platform of ["darwin", "linux"]) {
  test(`CC identity and socket paths: ${platform}`, t => {
    const originalPlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const originalRuntime = process.env.XDG_RUNTIME_DIR;
    const originalTimezone = process.env.TZ;
    Object.defineProperty(process, "platform", { value: platform });
    t.after(() => {
      Object.defineProperty(process, "platform", originalPlatform);
      if (originalRuntime === undefined) delete process.env.XDG_RUNTIME_DIR;
      else process.env.XDG_RUNTIME_DIR = originalRuntime;
      if (originalTimezone === undefined) delete process.env.TZ;
      else process.env.TZ = originalTimezone;
      t.mock.restoreAll();
      syncBuiltinESMExports();
    });
    process.env.XDG_RUNTIME_DIR = "/test/runtime";
    assert.equal(ccSocketDir(), platform === "darwin" ? "/tmp/cc-socks" : "/test/runtime/cc-socks");
    assert.equal(ccSocketDir({ socketDir: "/custom/sockets" }), "/custom/sockets");
    delete process.env.XDG_RUNTIME_DIR;
    assert.equal(ccSocketDir(), platform === "darwin" ? "/tmp/cc-socks" : `/tmp/cc-socks-${process.getuid?.() ?? 0}`);

    if (platform === "darwin") {
      process.env.TZ = "Pacific/Auckland";
      let output: string | Error = "Thu Nov 12 23:18:35 2026\n";
      const ps = t.mock.method(childProcess, "execFileSync", (() => {
        if (output instanceof Error) throw output;
        return output;
      }) as unknown as typeof childProcess.execFileSync);
      syncBuiltinESMExports();
      assert.equal(procStart(123), "Thu Nov 12 23:18:35 2026");
      const args = ps.mock.calls[0].arguments;
      assert.equal(args[0], "/bin/ps");
      assert.deepEqual(args[1], ["-p", "123", "-o", "lstart="]);
      assert.equal((args[2] as { env: NodeJS.ProcessEnv }).env.LC_ALL, "C");
      assert.equal((args[2] as { env: NodeJS.ProcessEnv }).env.TZ, "UTC");
      assert.equal(process.env.TZ, "Pacific/Auckland");
      assert.equal(pidDomainFor([]), "darwin");
      output = "";
      assert.equal(procStart(123), undefined);
      output = new Error("process gone");
      assert.equal(procStart(123), undefined);
    } else {
      t.mock.method(fs, "readFileSync", (path: unknown) => path === "/etc/machine-id" ? "machine\n" : "123 (main (worker) thread) S 1 1 0 0 -1 4194560 1234 0 0 0 7 3 0 0 20 5 1 0 4242 12345678\n");
      t.mock.method(fs, "readlinkSync", () => "pid:[42]");
      syncBuiltinESMExports();
      assert.equal(procStart(123), "4242");
      assert.match(pidDomainFor([]), /^linux:[0-9a-f]{32}:pid:\[pid:\[42\]\]$/);
      const record = { pid: 123, sessionId: "cc", messagingSocketPath: "/cc.sock", pidDomain: "linux:native:pid:[pid:[42]]" };
      assert.equal(pidDomainFor([{ record }]), record.pidDomain);
    }
    assert.equal(procStart(0), undefined);
    assert.equal(procStart(-1), undefined);
  });
}

test("Darwin procStart: UTC identity is independent of a non-UTC parent timezone", { skip: process.platform !== "darwin" }, t => {
  const originalTimezone = process.env.TZ;
  t.after(() => {
    if (originalTimezone === undefined) delete process.env.TZ;
    else process.env.TZ = originalTimezone;
  });
  process.env.TZ = "Pacific/Auckland";
  const expected = childProcess.execFileSync("/bin/ps", ["-p", String(process.pid), "-o", "lstart="], {
    encoding: "utf-8", env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
  }).trim();
  const local = childProcess.execFileSync("/bin/ps", ["-p", String(process.pid), "-o", "lstart="], {
    encoding: "utf-8", env: { ...process.env, LC_ALL: "C" },
  }).trim();
  assert.notEqual(local, expected);
  assert.equal(procStart(), expected);
  assert.equal(process.env.TZ, "Pacific/Auckland");
});
