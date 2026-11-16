import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { releaseLock, tryLock } from "../../src/daemon-lock.ts";

const path = process.argv[2];
if (process.argv[3] === "pause-after-read") {
  const read = fs.readFileSync;
  fs.readFileSync = ((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    const content = Reflect.apply(read, fs, [file, ...args]);
    if (file === path) {
      process.send?.({ inspected: true });
      const deadline = Date.now() + 10_000;
      while (!fs.existsSync(`${path}.resume.${process.pid}`)) {
        if (Date.now() > deadline) throw new Error("test read barrier timed out");
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
      }
    }
    return content;
  }) as typeof fs.readFileSync;
  syncBuiltinESMExports();
}
process.on("message", message => {
  if (message === "acquire") process.send?.({ acquired: tryLock(path, () => {}) });
  if (message === "release") {
    releaseLock(path);
    process.exit(0);
  }
});
process.send?.({ ready: true });
