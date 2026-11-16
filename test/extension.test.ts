import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import piCcBridgeExtension from "../extensions/index.ts";

for (const platform of ["darwin", "linux"]) {
  test(`extension: ${platform} status, start, stop and supervision verify the full lock identity`, async t => {
    const agentDir = fs.mkdtempSync(join(tmpdir(), "ccbridge-extension-"));
    const stateDir = join(agentDir, "pi-cc-bridge");
    fs.mkdirSync(stateDir);
    const originalAgentDir = process.env.PI_CODING_AGENT_DIR;
    const originalPlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
    process.env.PI_CODING_AGENT_DIR = agentDir;
    Object.defineProperty(process, "platform", { value: platform });
    t.after(() => {
      t.mock.restoreAll();
      syncBuiltinESMExports();
      Object.defineProperty(process, "platform", originalPlatform);
      if (originalAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
      else process.env.PI_CODING_AGENT_DIR = originalAgentDir;
      fs.rmSync(agentDir, { recursive: true, force: true });
    });
    const start = platform === "darwin" ? "Thu Nov 12 23:18:35 2026" : "4242";
    let actualStart: string | undefined = start;
    const readFile = fs.readFileSync;
    t.mock.method(fs, "readFileSync", ((path: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(path) === "/proc/123/stat") {
        if (!actualStart) throw new Error("identity unavailable");
        return `123 (daemon) S ${Array(18).fill("0").join(" ")} ${actualStart}`;
      }
      return Reflect.apply(readFile, fs, [path, ...args]);
    }) as typeof fs.readFileSync);
    t.mock.method(childProcess, "execFileSync", (() => {
      if (!actualStart) throw new Error("identity unavailable");
      return `${actualStart}\n`;
    }) as unknown as typeof childProcess.execFileSync);
    const spawn = t.mock.method(childProcess, "spawn", () => { throw new Error("unexpected daemon spawn"); });
    let alive = true;
    const kill = t.mock.method(process, "kill", (pid: number, signal?: string | number) => {
      assert.equal(pid, 123, "must never signal a process group or invalid pid");
      assert.ok(signal === 0 || signal === "SIGTERM");
      if (!alive) throw Object.assign(new Error("process gone"), { code: "ESRCH" });
      return true;
    });
    let supervise: (() => void) | undefined;
    t.mock.method(globalThis, "setInterval", ((callback: () => void) => {
      supervise = callback;
      return { unref: () => {} };
    }) as unknown as typeof setInterval);
    syncBuiltinESMExports();
    const lock = join(stateDir, "daemon.lock");
    fs.writeFileSync(lock, `123 ${start}`);
    fs.writeFileSync(join(stateDir, "config.json"), '{"enabled":true}');
    let handler: Parameters<ExtensionAPI["registerCommand"]>[1]["handler"] | undefined;
    piCcBridgeExtension({
      registerCommand: (name, options) => {
        assert.equal(name, "cc-bridge");
        handler = options.handler;
      },
    } as Pick<ExtensionAPI, "registerCommand"> as ExtensionAPI);
    assert.ok(handler);
    const notices: string[] = [];
    const ctx = { ui: { notify: (message: string) => notices.push(message) } } as unknown as ExtensionCommandContext;
    await handler("status", ctx);
    assert.match(notices.at(-1)!, /running \(pid 123\)/);
    await handler("start", ctx);
    assert.match(notices.at(-1)!, /already running \(pid 123\)/);
    supervise!();
    assert.equal(spawn.mock.callCount(), 0);
    await handler("stop", ctx);
    assert.equal(kill.mock.calls.filter(call => call.arguments[1] === "SIGTERM").length, 1);

    fs.writeFileSync(join(stateDir, "config.json"), '{"enabled":false}');
    for (const content of ["123 unknown", "0 unknown", "-1 unknown", "1.5 known", "9007199254740992 known", "123", `123 ${start} changed`]) {
      fs.writeFileSync(lock, content);
      await handler("status", ctx);
      assert.match(notices.at(-1)!, /daemon: not running/);
      await handler("stop", ctx);
      assert.equal(kill.mock.calls.filter(call => call.arguments[1] === "SIGTERM").length, 1, content);
    }
    fs.writeFileSync(lock, `123 ${start}`);
    for (const actual of [undefined, "unknown", "changed identity"]) {
      actualStart = actual;
      await handler("stop", ctx);
      assert.match(notices.at(-1)!, /daemon not running/);
      assert.equal(kill.mock.calls.filter(call => call.arguments[1] === "SIGTERM").length, 1);
    }
    actualStart = start;
    alive = false;
    await handler("stop", ctx);
    assert.match(notices.at(-1)!, /daemon not running/);
    assert.equal(kill.mock.calls.filter(call => call.arguments[1] === "SIGTERM").length, 1);
  });
}
