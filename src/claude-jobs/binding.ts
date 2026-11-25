import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { BrokerClient, type BrokerSessionInfo } from "../intercom/client.ts";
import { verifiedLockPid } from "../daemon-lock.ts";
import { readRecordFile, procStart, listRecordsSync } from "../cc/registry.ts";
import { BRIDGE_OWNER, ccSessionsDir, type CcRecord } from "../cc/paths.ts";

export interface JobBinding {
  parentTarget: string;
  bridgeName: string;
  watchWorker(sessionId: string, pid: number): void;
  close(): void;
}

export async function bindJob(input: {
  agentDir: string; parentTarget: string; cwd: string; callerPid: number;
  ccSessionsDir?: string; onLost(): void;
}): Promise<JobBinding> {
  const client = new BrokerClient(input.agentDir);
  const roster = new Map<string, BrokerSessionInfo>();
  let closing = false;
  let parent: BrokerSessionInfo | undefined;
  let worker: { sessionId: string; pid: number; epoch?: string; ready: boolean; until: number } | undefined;
  let timer: NodeJS.Timeout | undefined;
  const lost = () => { if (!closing) input.onLost(); };
  const lock = join(input.agentDir, "pi-cc-bridge", "daemon.lock");
  const recordsDir = ccSessionsDir({ ccSessionsDir: input.ccSessionsDir });
  function daemon(): CcRecord {
    const pid = verifiedLockPid(lock);
    const record = pid ? readRecordFile(join(recordsDir, `${pid}.json`)) : undefined;
    if (!record || record.bridgeOwner !== BRIDGE_OWNER || record.kind !== "bridge" ||
        !record.name || record.procStart !== procStart(pid)) throw new Error("verified bridge missing");
    return record;
  }
  const bridge = daemon();
  if (listRecordsSync({ ccSessionsDir: recordsDir }).filter(({ record }) => record.name === bridge.name).length !== 1) {
    throw new Error("ambiguous bridge recipient name");
  }
  function check(): void {
    const current = daemon();
    if (["pid", "procStart", "sessionId", "messagingSocketPath", "name"].some(key =>
      current[key as keyof CcRecord] !== bridge[key as keyof CcRecord])) throw new Error("bridge rebound");
    const live = roster.get(input.parentTarget);
    if (!live || live.pid !== input.callerPid || live.cwd !== input.cwd || !live.endpointEpoch ||
        (parent && live.endpointEpoch !== parent.endpointEpoch)) throw new Error("parent missing or rebound; Claude jobs requires the default broker ID matching the owning Pi session UUID");
    parent = live;
    if (worker) {
      const peer = roster.get(`cc-${worker.sessionId}`);
      if (!peer) {
        if (worker.ready || Date.now() >= worker.until) throw new Error("worker route missing");
      } else {
        if (peer.pid !== bridge.pid || !peer.endpointEpoch || (worker.epoch && worker.epoch !== peer.endpointEpoch)) throw new Error("worker route rebound");
        const record = readRecordFile(join(recordsDir, `${worker.pid}.json`));
        if (!record || record.sessionId !== worker.sessionId || record.procStart !== procStart(worker.pid)) throw new Error("worker record mismatch");
        worker.ready = true;
        worker.epoch = peer.endpointEpoch;
      }
    }
  }
  client.on("error", lost);
  client.on("disconnected", lost);
  client.on("sessions", (event: { sessions: BrokerSessionInfo[] }) => {
    roster.clear();
    for (const peer of event.sessions) roster.set(peer.id, peer);
  });
  client.on("session_joined", (event: { session: BrokerSessionInfo }) => roster.set(event.session.id, event.session));
  client.on("presence_update", (event: { session: BrokerSessionInfo }) => roster.set(event.session.id, event.session));
  client.on("session_left", (event: { sessionId: string }) => {
    roster.delete(event.sessionId);
    if (event.sessionId === input.parentTarget || (worker?.ready && event.sessionId === `cc-${worker.sessionId}`)) lost();
  });
  const close = () => {
    closing = true;
    clearInterval(timer);
    client.close();
  };
  try {
    await client.connectAndRegister(`claude-job-monitor-${randomUUID()}`, {
      cwd: input.cwd, model: "claude-job-monitor", pid: process.pid,
      startedAt: Date.now(), lastActivity: Date.now(),
    });
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => { client.off("sessions", receive); reject(new Error("parent roster timeout")); }, 3000);
      const receive = () => { clearTimeout(timeout); resolve(); };
      client.once("sessions", receive);
      client.list();
    });
    check();
    // Own resource liveness, independent of stock subagent monitoring or status reads.
    timer = setInterval(() => {
      try { check(); client.list(); } catch { lost(); }
    }, 1000);
    timer.unref();
    return {
      parentTarget: input.parentTarget, bridgeName: bridge.name!,
      watchWorker(sessionId, pid) { worker = { sessionId, pid, ready: false, until: Date.now() + 30_000 }; },
      close,
    };
  } catch (error) { close(); throw error; }
}
