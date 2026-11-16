import { closeSync, constants, type Stats, fstatSync, lstatSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { isPidAlive, procStart } from "./cc/registry.ts";

interface ProcessIdentity {
  isPidAlive: (pid: number) => boolean;
  procStart: (pid?: number) => string | undefined;
}

export function parseDaemonLock(content: string): { pid: number; procStart: string } | undefined {
  const match = /^(\d+) (.+)$/.exec(content.trim());
  if (!match) return undefined;
  const pid = Number(match[1]);
  if (!Number.isSafeInteger(pid) || pid <= 0) return undefined;
  return { pid, procStart: match[2] };
}

/** Signalling requires a known, matching process identity, not just a live pid. */
export function verifiedLockPid(
  lockPath: string,
  identity: ProcessIdentity = { isPidAlive, procStart },
): number | undefined {
  try {
    const owner = parseDaemonLock(readFileSync(lockPath, "utf-8"));
    if (!owner || owner.procStart === "unknown" || !identity.isPidAlive(owner.pid)) return undefined;
    const actualStart = identity.procStart(owner.pid);
    return actualStart && actualStart !== "unknown" && actualStart === owner.procStart ? owner.pid : undefined;
  } catch {
    return undefined;
  }
}

const heldLocks = new Map<string, { fd: number; content: string }>();

function assertPrivateFile(stat: Stats, path: string): void {
  if (!stat.isFile() || stat.uid !== process.getuid?.() || (stat.mode & 0o077) !== 0) {
    throw new Error(`refusing non-private lock file ${path}`);
  }
}

/** The guard inode is permanent. Removing it would allow two independent flock owners. */
export function tryLock(
  lockPath: string,
  log: (message: string) => void,
  identity: ProcessIdentity = { isPidAlive, procStart },
): boolean {
  if (heldLocks.has(lockPath)) return false;
  // Resolve lazily: extension status/stop only need the identity helpers above.
  const { flockSync } = createRequire(import.meta.url)("fs-ext") as { flockSync(fd: number, flags: string): void };
  const guardPath = `${lockPath}.guard`;
  const fd = openSync(guardPath, constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW, 0o600);
  let acquired = false;
  try {
    const guard = fstatSync(fd);
    assertPrivateFile(guard, guardPath);
    try {
      flockSync(fd, "exnb");
    } catch (error) {
      if (["EAGAIN", "EWOULDBLOCK"].includes((error as NodeJS.ErrnoException).code ?? "")) return false;
      throw error;
    }
    const current = lstatSync(guardPath);
    if (current.dev !== guard.dev || current.ino !== guard.ino) throw new Error("daemon guard inode changed");
    let previous: string | undefined;
    try {
      assertPrivateFile(lstatSync(lockPath), lockPath);
      previous = readFileSync(lockPath, "utf-8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    if (previous !== undefined) {
      const owner = parseDaemonLock(previous);
      if (!owner) return false;
      if (identity.isPidAlive(owner.pid)) {
        const actual = identity.procStart(owner.pid);
        if (!actual || actual === "unknown" || owner.procStart === "unknown" || actual === owner.procStart) return false;
      }
    }
    const content = `${process.pid} ${identity.procStart() ?? "unknown"}`;
    const temp = `${lockPath}.${randomUUID()}.tmp`;
    try {
      writeFileSync(temp, content, { flag: "wx", mode: 0o600 });
      renameSync(temp, lockPath);
    } finally {
      try { unlinkSync(temp); } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
    }
    heldLocks.set(lockPath, { fd, content });
    acquired = true;
    if (previous !== undefined) log("took over stale daemon lock (previous owner dead or identity changed)");
    return true;
  } finally {
    if (!acquired) closeSync(fd);
  }
}

/** Remove only our metadata, while still holding the guard; close releases flock. */
export function releaseLock(lockPath: string): void {
  const held = heldLocks.get(lockPath);
  if (!held) return;
  try {
    if (readFileSync(lockPath, "utf-8") === held.content) unlinkSync(lockPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  } finally {
    heldLocks.delete(lockPath);
    closeSync(held.fd);
  }
}
