import { appendFileSync, readFileSync, renameSync, statSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { Bridge } from "./bridge.ts";
import { bridgeStateDir, loadConfig } from "./config.ts";
import { tryLock } from "./daemon-lock.ts";

const MAX_LOG_BYTES = 16 * 1024 * 1024; // rotate to daemon.log.1; keeps runaway logging bounded

/** CLI entry: run the bridge until signalled. */
export async function runDaemon(argv: string[] = process.argv.slice(2)): Promise<void> {
  // P3-1: explicit flags only; a bare positional once created `./--help/`.
  let agentDirOverride: string | undefined;
  for (const arg of argv) {
    if (arg.startsWith("--agent-dir=")) agentDirOverride = arg.slice("--agent-dir=".length);
    else {
      process.stderr.write(`pi-cc-bridge: unknown argument ${arg} (usage: pi-cc-bridge [--agent-dir=<path>])\n`);
      process.exit(2);
    }
  }
  const stateDir = bridgeStateDir(agentDirOverride);
  const config = loadConfig(stateDir);
  const logFile = join(stateDir, "daemon.log");
  let stdoutBroken = false;
  const log = (msg: string) => {
    // Logging must never throw; an EPIPE on a dead parent's stdout pipe once
    // looped through uncaughtException and wrote 19GB/hour to disk.
    const line = `${new Date().toISOString()} ${msg}`;
    try {
      let size = 0;
      try {
        size = statSync(logFile).size; // missing file is fine: append creates it
      } catch {
        size = 0;
      }
      if (size > MAX_LOG_BYTES) {
        try {
          unlinkSync(`${logFile}.1`);
        } catch {
          /* first rotation */
        }
        renameSync(logFile, `${logFile}.1`); // rotate: keep one generation, stay bounded
      }
      appendFileSync(logFile, `${line}\n`);
    } catch {
      /* disk full or gone: drop the line, keep running */
    }
    if (!stdoutBroken && config.logLevel !== "quiet") {
      try {
        process.stdout.write(`${line}\n`);
      } catch {
        stdoutBroken = true; // pipe died: stop attempting, never throw
      }
    }
  };

  const lockPath = join(stateDir, "daemon.lock");
  if (!tryLock(lockPath, log)) {
    log("another daemon owns the lock; exiting");
    process.exit(0);
  }
  process.on("exit", () => {
    try {
      if (readFileSync(lockPath, "utf-8").startsWith(`${process.pid} `)) unlinkSync(lockPath);
    } catch {
      /* best effort */
    }
  });

  if (!config.enabled) {
    log("disabled in config; exiting");
    process.exit(0);
  }

  const bridge = new Bridge(config, log, {
    ccSessionsDir: config.ccSessionsDir,
    socketDir: config.ccSocketDir,
  });
  const shutdown = (signal: string) => {
    log(`received ${signal}`);
    void bridge.stop().then(() => process.exit(0));
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("uncaughtException", error => {
    // One line, best-effort; must never recurse through logging. Then exit:
    // running on in an unknown state is worse than a supervised restart (P3-10).
    try {
      log(`uncaught: ${error.stack ?? error.message}`);
    } catch {
      /* drop */
    }
    log("shutting down after uncaught exception; the extension will restart this daemon");
    void bridge.stop().then(() => process.exit(1));
  });

  try {
    await bridge.start();
    log("bridge running");
  } catch (error) {
    log(`start failed: ${(error as Error).message}`);
    void bridge.stop().catch(() => {});
    process.exit(1);
  }
}
