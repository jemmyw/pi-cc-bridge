import { readFileSync, renameSync, writeFileSync } from "node:fs";
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

/** Unknown identity is not evidence that a live owner's lock is stale. */
export function tryLock(
  lockPath: string,
  log: (message: string) => void,
  identity: ProcessIdentity = { isPidAlive, procStart },
): boolean {
  const content = `${process.pid} ${identity.procStart() ?? "unknown"}`;
  try {
    writeFileSync(lockPath, content, { flag: "wx", mode: 0o600 });
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    try {
      const owner = parseDaemonLock(readFileSync(lockPath, "utf-8"));
      if (!owner) return false;
      if (identity.isPidAlive(owner.pid)) {
        const actualStart = identity.procStart(owner.pid);
        if (!actualStart || actualStart === "unknown" || owner.procStart === "unknown" || actualStart === owner.procStart) return false;
      }
    } catch {
      return false;
    }
    try {
      renameSync(lockPath, `${lockPath}.stale.${process.pid}`);
    } catch {
      return false;
    }
    try {
      writeFileSync(lockPath, content, { flag: "wx", mode: 0o600 });
      log("took over stale daemon lock (previous owner dead or identity changed)");
      return true;
    } catch {
      return false;
    }
  }
}
