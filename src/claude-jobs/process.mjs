import { spawn } from "node:child_process";

let child;
let timer;
let closing = false;
let pending = 0;
let buffer = "";
let bytes = 0;
const send = value => {
  if (!process.connected || pending >= 16) return stop("ipc-overflow");
  pending++;
  process.send(value, error => {
    pending--;
    if (error) stop("ipc-error");
  });
};
function stop(reason) {
  if (closing) return;
  closing = true;
  clearTimeout(timer);
  if (process.connected) process.send({ type: "stopping", reason });
  const pid = child?.pid;
  const kill = signal => {
    if (!pid) return;
    try { process.kill(-pid, signal); } catch (error) {
      if (error.code !== "ESRCH") process.exitCode = 1;
    }
  };
  kill("SIGTERM");
  setTimeout(() => {
    kill("SIGKILL");
    process.exit(0);
  }, 500);
}
process.on("disconnect", () => stop("owner-disconnected"));
process.on("SIGTERM", () => stop("guardian-signal"));
process.on("SIGINT", () => stop("guardian-signal"));
process.on("message", message => {
  if (closing) return;
  if (message.type === "launch") {
    if (child) return stop("duplicate-launch");
    clearTimeout(timer);
    timer = setTimeout(() => stop("deadline"), message.timeoutMs);
    try {
      child = spawn(message.command, message.args, {
        cwd: message.cwd, env: message.env, detached: true, stdio: ["pipe", "pipe", "pipe"],
      });
      child.once("error", () => stop("launch-error"));
      child.once("spawn", () => send({ type: "spawned", pid: child.pid }));
      child.once("exit", () => stop("process-exit"));
      child.stdin.on("error", () => stop("stdin-error"));
      child.stderr.resume();
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", chunk => {
        bytes += Buffer.byteLength(chunk);
        if (bytes > 8 * 1024 * 1024) return stop("output-budget");
        buffer += chunk;
        let newline;
        while ((newline = buffer.indexOf("\n")) !== -1) {
          const line = buffer.slice(0, newline);
          buffer = buffer.slice(newline + 1);
          if (Buffer.byteLength(line) > 1024 * 1024) return stop("frame-limit");
          if (line.trim()) send({ type: "line", line });
        }
        if (Buffer.byteLength(buffer) > 1024 * 1024) stop("frame-limit");
      });
    } catch { stop("launch-error"); }
  } else if (message.type === "input") {
    if (!child || child.stdin.writableLength > 128 * 1024 || typeof message.text !== "string" || Buffer.byteLength(message.text) > 96 * 1024) return stop("input-limit");
    child.stdin.write(JSON.stringify({ type: "user", message: { role: "user", content: message.text } }) + "\n");
  } else if (message.type === "cancel") stop("cancelled");
});
// An owner that never supplies launch must not leave a guardian behind.
timer = setTimeout(() => stop("launch-timeout"), 5000);
