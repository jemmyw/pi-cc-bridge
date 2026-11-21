import { spawn, ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, unlinkSync, writeFileSync } from "node:fs";
import net from "node:net";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { CcRecord, isTargetableRecord, ownSocketPath, ccSocketDir, CcPathsOptions, ensurePrivateDir, ensureSessionsDir } from "./cc/paths.ts";
import { gcOwnStaleRecords, listRecordsSync, procStart, selfCheckProcStart, pidDomainFor, writeOwnRecord, removeOwnRecord, isPidAlive } from "./cc/registry.ts";
import { startCcInbox, CcInboxServer } from "./cc/inbox.ts";
import { postToCcInbox } from "./cc/poster.ts";
import { ParsedInbound } from "./cc/protocol.ts";
import { BrokerClient, IntercomMessage, BrokerSessionInfo } from "./intercom/client.ts";
import { BridgeConfig, bridgeStateDir } from "./config.ts";
import { randomUUID as _uuid } from "node:crypto";
void _uuid;
import { allocateHolderAlias, aliasExposed, derivedCcName, isFallbackAlias, matchTarget, parseToHeader, routeFunnel, toCcStatus } from "./routing.ts";
import { RateLimiter } from "./ratelimit.ts";
import { AskReplies, askInstructions } from "./ask-replies.ts";
export { tryLock } from "./daemon-lock.ts";
export { runDaemon } from "./daemon.ts";

/**
 * The daemon: discovery loop + relay + holder supervision.
 *
 * pi → CC: every live CC session becomes a synthetic intercom peer (own broker
 *   connection, sessionId `cc-<ccSessionId>`, name `claude:<name>`), capped by
 *   maxPeers. Connection loss is remembered and retried with backoff (P0-2).
 * CC → pi: two paths.
 *   - Native (default on, contact-driven): a holder child per exposed pi
 *     session (config `expose`) — plus lazily for any pi session that messages
 *     CC first — binds its own CC inbox socket and registry record
 *     `pi-<alias>`, so CC addresses pi sessions directly. Children are dumb
 *     socket-holders; the daemon owns routing, trust policy, and relays via
 *     the sending CC session's own peer connection (P0-4, P1-13).
 *   - Funnel fallback: the daemon itself masquerades as one CC session
 *     (`peerName`); messages route via a leading `To: <alias>` line, `routeTo`,
 *     or a single-session roster. Unroutable traffic is HELD, never guessed.
 *
 * Single-instance: a permanent flock guard protects `pid procStart` owner metadata.
 */

interface Peer {
  cc: CcRecord;
  client: BrokerClient;
  sessionId: string;
  name: string;
}

interface HolderState {
  piSessionId: string;
  alias: string;
  childPid?: number;
  child?: ChildProcess;
  socket?: net.Socket;
  socketPath?: string;
  status?: string;
  contactedOnly: boolean; // spawned via contact-driven exposure, not config
}

const HOLDER_LINE = 10; // \n
const MAX_RELAY_BYTES = 900 * 1024; // below CC's ~1M-char refusal (P1-7)
const SPAWN_COOLDOWN_MS = 10_000;
const MAX_CRASHES = 5;
const HOLD_FAILED = "(hold failed)";
const isLiveCcRecord = (record: CcRecord) => record.pid !== process.pid && record.kind !== "bridge" &&
  isTargetableRecord(record) && isPidAlive(record.pid);

export class Bridge {
  private peers = new Map<string, Peer>(); // key: CC sessionId
  private inbox: CcInboxServer | null = null;
  private pollTimer: NodeJS.Timeout | null = null;
  private sweepTimer: NodeJS.Timeout | null = null;
  private stopping = false;
  private roster = new Map<string, BrokerSessionInfo>(); // by session id
  private holders = new Map<string, HolderState>(); // key: pi session id
  private contacted = new Set<string>(); // pi sessions that messaged CC first
  private ipcServer: net.Server | null = null;
  private readonly ipcPath: string;
  private lastSpawn = new Map<string, { at: number; crashes: number }>();
  private retryAt = new Map<string, number>();
  private pending = new Map<string, { fromSocket?: string; alias: string; at: number }>();
  private warnedOnce = new Set<string>();
  private logOnce = (key: string, msg: string) => {
    if (!this.warnedOnce.has(key)) {
      this.warnedOnce.add(key);
      this.log(msg);
    }
  };
  private readonly asks = new AskReplies();
  private readonly piLimiter: RateLimiter;
  private readonly ccLimiter: RateLimiter;
  private heldDir: string;

  constructor(
    readonly config: BridgeConfig,
    private readonly log: (msg: string) => void,
    private readonly paths: CcPathsOptions = {},
  ) {
    this.heldDir = join(bridgeStateDir(config.agentDir), "held");
    mkdirSync(this.heldDir, { recursive: true, mode: 0o700 });
    this.ipcPath = join(bridgeStateDir(config.agentDir), "holders-ipc.sock");
    this.piLimiter = new RateLimiter(config.relayPerMin, config.relayBurst);
    this.ccLimiter = new RateLimiter(config.relayPerMin, config.relayBurst);
  }

  async start(): Promise<void> {
    ensurePrivateDir(ccSocketDir(this.paths)); // P0-7: /run/user/<uid>/cc-socks may not exist yet
    ensureSessionsDir(this.paths);
    gcOwnStaleRecords(process.pid, this.paths); // P0-3: previous incarnations (bridgeOwner-marked, dead pid)

    const socketPath = ownSocketPath(this.paths);
    const records = listRecordsSync(this.paths);
    selfCheckProcStart(records, this.log);

    // Funnel masquerade: identity fields mirror verified CC shapes where possible.
    const record: CcRecord = {
      pid: process.pid,
      sessionId: randomUUID(),
      cwd: process.cwd(),
      startedAt: Date.now(),
      procStart: procStart(),
      version: records.find(r => r.record.kind !== "bridge")?.record.version ?? "0.0.0",
      peerProtocol: 1,
      peerFeatures: ["reply_across_default_dirs"], // notify_idle only once implemented (P1-10)
      kind: "bridge",
      entrypoint: "cli",
      pidDomain: pidDomainFor(records),
      messagingSocketPath: socketPath,
      name: this.config.peerName,
      nameSource: "user",
      nameSince: Date.now(),
      updatedAt: Date.now(),
      status: "idle",
      statusUpdatedAt: Date.now(),
    };
    // Bind first, then publish the record: CC preflights the socket's peer pid,
    // so a record must never point at a socket that is not listening yet (P3-7).
    this.inbox = await startCcInbox({
      socketPath,
      onFrame: frame => void this.onCcFrame(frame),
      onError: error => this.log(`inbox: ${error.message}`),
    });
    writeOwnRecord(record, this.paths);
    this.log(`bridge inbox at ${socketPath}, registry record written`);

    this.startIpcServer();

    this.pollTimer = setInterval(() => this.syncPeers(), this.config.pollMs);
    this.pollTimer.unref?.();
    this.sweepTimer = setInterval(() => this.sweepPending(), 30_000);
    this.sweepTimer.unref?.();
    await this.syncPeers();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    this.asks.clear();
    if (this.pollTimer) clearInterval(this.pollTimer);
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    for (const peer of this.peers.values()) peer.client.close();
    this.peers.clear();
    for (const holder of this.holders.values()) this.shutdownHolder(holder);
    this.holders.clear();
    if (this.inbox) await this.inbox.close();
    if (this.ipcServer) {
      await new Promise<void>(done => this.ipcServer!.close(() => done()));
      try {
        unlinkSync(this.ipcPath);
      } catch {
        /* best effort */
      }
      this.ipcServer = null;
    }
    removeOwnRecord(process.pid, this.paths);
    this.log("bridge stopped");
  }

  // ── CC peer discovery & lifecycle ────────────────────────────────────────────

  /** Align synthetic peers with the CC registry; reconnect with backoff (P0-2). */
  private syncPeers(): void {
    if (this.stopping) return;
    const records = listRecordsSync(this.paths);
    selfCheckProcStart(records, this.log);
    gcOwnStaleRecords(process.pid, this.paths); // dead-pid bridge records (crashed holders) — periodic
    // Drift detection: an unfamiliar record shape or CC version means our
    // protocol assumptions may be stale — say so loudly, once each (P1-6/P2-10).
    for (const { record, file } of records) {
      if (record.kind === "bridge") continue;
      const shapeKey = Object.keys(record).sort().join(",");
      if (!this.seenRecordShapes.has(shapeKey)) {
        // First sight of a shape is the baseline; warn only when it CONFLICTS
        // with a shape we already accepted (schema drift), not on first boot.
        if (this.seenRecordShapes.size > 0) {
          this.logOnce(`shape-${shapeKey}`, `unrecognized CC record shape (${file}): fields [${shapeKey}] — check protocol assumptions`);
        }
        this.seenRecordShapes.add(shapeKey);
      }
      if (record.version && !this.knownCcVersions.has(record.version)) {
        const first = this.knownCcVersions.size === 0;
        this.knownCcVersions.add(record.version);
        if (!first) this.log(`CC version ${record.version} seen alongside other versions — wire format captured on 2.1.296; verify before relying on it`);
      }
    }
    const seen = new Set<string>();
    const usedNames = new Set<string>([this.config.peerName, ...[...this.holders.values()].map(h => `pi-${h.alias}`)]);

    for (const { record } of records) {
      if (!isLiveCcRecord(record)) continue;
      if (this.peers.size >= this.config.maxPeers && !this.peers.has(record.sessionId)) {
        this.logOnce("maxPeers", `maxPeers ${this.config.maxPeers} reached; not mirroring ${record.name ?? record.sessionId}`);
        continue;
      }
      seen.add(record.sessionId);
      if ((this.retryAt.get(record.sessionId) ?? 0) > Date.now()) continue;

      const existing = this.peers.get(record.sessionId);
      if (existing) {
        // Live-state refresh: socket/pid changes (resume) must not go stale (P1-6).
        if (existing.cc.pid !== record.pid || existing.cc.procStart !== record.procStart || existing.cc.messagingSocketPath !== record.messagingSocketPath) {
          this.asks.clearCc(record.sessionId);
          existing.cc = record;
        } else if (record.status && record.status !== existing.cc.status) {
          existing.cc = record;
          existing.client.presence(toCcStatus(record.status));
        }
        // CC renames: retire the peer so it re-registers under the new name (P2-8).
        if (record.name?.trim() && `claude:${record.name.trim()}` !== existing.name) {
          this.log(`CC session renamed (${existing.name} → claude:${record.name.trim()}); re-registering`);
          this.asks.clearCc(record.sessionId);
          existing.client.close();
          this.peers.delete(record.sessionId);
        }
        continue;
      }

      const baseName = record.name?.trim() ? `claude:${record.name.trim()}` : `claude:pid-${record.pid}`;
      let name = baseName;
      let suffix = 1;
      while (usedNames.has(name)) name = `${baseName}#${++suffix}`; // first collision is #2
      usedNames.add(name);

      const sessionId = `cc-${record.sessionId}`;
      const client = new BrokerClient(this.agentDir(), m => this.log(m));
      const peer: Peer = { cc: record, client, sessionId, name };
      this.peers.set(record.sessionId, peer); // reserve before await (no duplicate clients)

      const forget = () => {
        if (this.peers.get(record.sessionId)?.client === client) {
          this.asks.clearCc(record.sessionId);
          this.peers.delete(record.sessionId);
        }
        this.retryAt.set(record.sessionId, Date.now() + 5_000);
        if (this.peers.size === 0) this.clearHoldersAndRoster();
      };
      client.on("disconnected", forget);
      client.on("error", (error: Error) => this.log(`peer ${name}: ${error.message}`));
      client.on("message", event => void this.onPiToCc(record.sessionId, event, client));
      client.on("message_control", event => {
        if (this.peers.get(record.sessionId)?.client === client && (event.control.action === "cancel" || event.control.action === "supersede")) this.asks.cancelMessage(record.sessionId, event.from.id, event.control.messageId);
      });
      client.on("registered", () => {
        this.log(`peer registered: ${name} (${sessionId})`);
        client.list();
      });
      client.on("sessions", event => {
        if (client === this.rosterClient()) this.replaceRoster(event.sessions);
      });
      client.on("session_joined", event => {
        if (client === this.rosterClient()) {
          this.roster.set(event.session.id, event.session);
          this.reconcileHolders();
        }
      });
      client.on("session_left", event => {
        if (client === this.rosterClient()) this.onRosterLeft(event.sessionId);
      });
      client.on("presence_update", event => {
        if (client === this.rosterClient()) this.onRosterSession(event.session);
      });
      client.on("delivery_failed", event => this.onDeliveryFailed(event.messageId, event.reason));
      client.on("message_receipt", event => {
        if (event.receipt.status === "injected" || event.receipt.status === "receiver_received") {
          this.pending.delete(event.receipt.messageId);
        }
      });

      client
        .connectAndRegister(sessionId, {
          name,
          cwd: record.cwd ?? process.cwd(),
          model: "claude-code (external)",
          pid: process.pid,
          startedAt: record.startedAt ?? Date.now(),
          lastActivity: Date.now(),
          status: toCcStatus(record.status),
          acknowledgesReceipts: true,
          livenessIntervalMs: 10_000,
        })
        .catch(error => {
          const msg = (error as Error).message;
          if (msg.includes("no pi-intercom broker endpoint")) {
            this.logOnce(
              "no-broker",
              `PREREQUISITE MISSING: no pi-intercom broker at ${this.agentDir()}/intercom/broker.sock — run \`pi install npm:pi-intercom\` and restart pi. The bridge keeps retrying; CC\u2194pi delivery stays down until then.`,
            );
          } else {
            this.log(`peer ${name} register failed: ${msg}`);
          }
          if (this.peers.get(record.sessionId)?.client === client) this.peers.delete(record.sessionId);
          this.retryAt.set(record.sessionId, Date.now() + 5_000);
          client.close();
        });
    }

    for (const [ccSessionId, peer] of this.peers) {
      if (!seen.has(ccSessionId)) {
        // Torn reads and pid-reuse windows must not flap peers: retire only on
        // a confirmed-dead pid or two consecutive misses (P1-5).
        if (!isPidAlive(peer.cc.pid)) {
          peer.client.close();
          this.asks.clearCc(ccSessionId);
          this.peers.delete(ccSessionId);
          this.missCount.delete(ccSessionId);
          this.log(`peer retired (pid dead): ${peer.name}`);
          continue;
        }
        const misses = (this.missCount.get(ccSessionId) ?? 0) + 1;
        if (misses >= 2) {
          peer.client.close();
          this.asks.clearCc(ccSessionId);
          this.peers.delete(ccSessionId);
          this.missCount.delete(ccSessionId);
          this.log(`peer retired (2 consecutive misses): ${peer.name}`);
        } else {
          this.missCount.set(ccSessionId, misses);
        }
      } else {
        this.missCount.delete(ccSessionId);
      }
    }

    if (this.peers.size > 0 && this.pollTick++ % 4 === 0) {
      const first = [...this.peers.values()][0].client;
      if (first.isRegistered) first.list();
    }
    this.reconcileHolders();
  }
  private pollTick = 0;
  private missCount = new Map<string, number>();
  private knownCcVersions = new Set<string>();
  private seenRecordShapes = new Set<string>();

  /** One connection is authoritative for roster events — process broadcasts once (P1-3). */
  private rosterClient(): BrokerClient | undefined {
    return [...this.peers.values()].find(p => p.client.isRegistered)?.client;
  }

  private replaceRoster(sessions: BrokerSessionInfo[]): void {
    const next = new Map(sessions.map(s => [s.id, s]));
    for (const [id, holder] of [...this.holders]) {
      if (!next.has(id)) {
        this.asks.clearPi(id);
        this.shutdownHolder(holder);
        this.holders.delete(id);
        this.log(`holder retired: ${holder.alias} (left roster)`);
      }
    }
    for (const [id, old] of this.roster) {
      const current = next.get(id);
      if (!current || current.pid !== old.pid || current.endpointEpoch !== old.endpointEpoch) this.asks.clearPi(id);
    }
    this.roster = next;
    this.reconcileHolders();
  }

  private onRosterSession(session: BrokerSessionInfo): void {
    const old = this.roster.get(session.id);
    if (old && (old.pid !== session.pid || old.endpointEpoch !== session.endpointEpoch)) this.asks.clearPi(session.id);
    this.roster.set(session.id, session);
    const holder = this.holders.get(session.id);
    if (holder) {
      const mapped = toCcStatus(session.status);
      if (mapped !== holder.status) {
        holder.status = mapped;
        holder.socket?.write(`${JSON.stringify({ type: "status", status: mapped })}\n`);
      }
      const wanted = this.holderNameFor(session);
      if (wanted && session.name && wanted !== holder.alias) {
        holder.alias = wanted;
        holder.socket?.write(`${JSON.stringify({ type: "alias", name: `pi-${wanted}` })}\n`);
      }
    }
    this.reconcileHolders();
  }

  private onRosterLeft(sessionId: string): void {
    this.asks.clearPi(sessionId);
    this.roster.delete(sessionId);
    const holder = this.holders.get(sessionId);
    if (holder) {
      this.shutdownHolder(holder);
      this.holders.delete(sessionId);
      this.log(`holder retired: ${holder.alias} (pi session left)`);
    }
  }

  private clearHoldersAndRoster(): void {
    for (const holder of this.holders.values()) this.shutdownHolder(holder);
    this.holders.clear();
    this.roster.clear();
    this.asks.clear();
  }

  // ── Holders ─────────────────────────────────────────────────────────────────

  /** Spawn/kill holder children so each eligible pi session has a CC entry. */
  private reconcileHolders(): void {
    if (this.stopping) return;
    let spawned = 0;
    for (const session of this.roster.values()) {
      if (session.id.startsWith("cc-")) continue; // our own synthetic CC peers
      if (this.holders.has(session.id)) continue;
      const rec = this.lastSpawn.get(session.id);
      if (rec && rec.crashes >= MAX_CRASHES) {
        this.logOnce(`crashloop-${session.id}`, `holder for ${session.name ?? session.id} crash-looping; giving up`);
        continue;
      }
      if (rec && Date.now() - rec.at < SPAWN_COOLDOWN_MS) continue;
      if (!this.isExposed(session) && !this.contacted.has(session.id)) continue; // contact-driven (P1-13)
      if (this.holders.size >= this.config.maxHolders) {
        this.logOnce("maxHolders", `maxHolders ${this.config.maxHolders} reached; skipping ${session.name ?? session.id}`);
        break;
      }
      this.spawnHolder(session);
      spawned++;
      if (spawned >= 3) break; // gentle ramp within one tick
    }
  }

  private isExposed(session: BrokerSessionInfo): boolean {
    const raw = session.name?.trim();
    const alias = raw && !isFallbackAlias(raw) ? raw : derivedCcName(session.id, session.cwd);
    return aliasExposed(alias, this.config.expose);
  }

  private resolveTsx(): string | undefined {
    try {
      const pkgDir = join(dirname(fileURLToPath(import.meta.url)), "..");
      return createRequire(join(pkgDir, "package.json")).resolve("tsx");
    } catch {
      const sibling = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "tsx", "dist", "loader.mjs");
      return existsSync(sibling) ? sibling : undefined;
    }
  }

  private spawnHolder(session: BrokerSessionInfo): void {
    // Prefer the prebuilt plain-JS bundle: no tsx loader in the child (~53MB
    // vs ~82MB RSS, faster spawn). Fall back to tsx when dist/ is absent.
    const bundled = join(dirname(fileURLToPath(import.meta.url)), "..", "dist", "holder.mjs");
    const useBundle = existsSync(bundled);
    const tsx = useBundle ? undefined : this.resolveTsx();
    if (!useBundle && !tsx) {
      this.log("holder spawn failed: no dist bundle and tsx loader not resolvable");
      return;
    }
    const aliasBase = this.holderNameFor(session)!;
    const state: HolderState = { piSessionId: session.id, alias: aliasBase, status: toCcStatus(session.status), contactedOnly: !this.isExposed(session) };
    this.holders.set(session.id, state);
    this.lastSpawn.set(session.id, { at: Date.now(), crashes: this.lastSpawn.get(session.id)?.crashes ?? 0 });
    const spec = {
      alias: aliasBase,
      piSessionId: session.id,
      parentPid: process.pid,
      ipcPath: this.ipcPath,
      peerPrefix: "pi-",
      heldDir: this.heldDir,
      sessionsDir: this.paths.ccSessionsDir,
      socketDir: this.paths.socketDir,
    };
    const entry = useBundle ? bundled : join(dirname(fileURLToPath(import.meta.url)), "..", "src", "holder.ts");
    const childArgs = useBundle ? [entry, JSON.stringify(spec)] : ["--import", tsx!, entry, JSON.stringify(spec)];
    const child = spawn(process.execPath, childArgs, {
      stdio: ["ignore", "ignore", "pipe"],
      env: { ...process.env, NODE_NO_WARNINGS: "1" },
    });
    state.childPid = child.pid;
    state.child = child;
    child.stderr?.on("data", d => this.log(`holder(${aliasBase}): ${d.toString().trim()}`));
    child.on("error", e => this.log(`holder ${aliasBase} spawn error: ${e.message}`));
    child.on("exit", code => {
      const st = this.holders.get(session.id);
      if (st?.childPid === child.pid) this.holders.delete(session.id); // dead state removed → respawn possible (P1-1)
      const rec = this.lastSpawn.get(session.id) ?? { at: 0, crashes: 0 };
      if (!this.stopping && code !== 0) rec.crashes++;
      rec.at = Date.now();
      this.lastSpawn.set(session.id, rec);
      if (!this.stopping && this.roster.has(session.id)) {
        setTimeout(() => !this.stopping && this.reconcileHolders(), SPAWN_COOLDOWN_MS + 1_000).unref();
      }
    });
    this.log(`holder spawning for pi session ${session.name ?? session.id}`);
  }

  /** Stable holder alias: never counts the session's own holder as a collision (P1-2). */
  private holderNameFor(session: BrokerSessionInfo): string {
    const base = (session.name?.trim() || `sid-${session.id.slice(0, 8)}`).replace(/[^\w.-]/g, "-");
    const taken = new Set<string>(
      [...this.holders.values()].filter(h => h.piSessionId !== session.id).map(h => h.alias),
    );
    taken.add(this.config.peerName.replace(/^pi-/, ""));
    for (const { record } of listRecordsSync(this.paths)) {
      if (record.kind !== "bridge" && record.name?.startsWith("pi-")) taken.add(record.name.slice(3));
    }
    const current = this.holders.get(session.id)?.alias;
    if (current && (current === base || current.startsWith(`${base}#`)) && !taken.has(current)) return current;
    if (!taken.has(base)) return base;
    let n = 2;
    while (taken.has(`${base}#${n}`)) n++;
    return `${base}#${n}`;
  }

  private shutdownHolder(holder: HolderState): void {
    if (holder.socket) {
      try {
        holder.socket.write(`${JSON.stringify({ type: "shutdown" })}\n`);
      } catch {
        /* best effort */
      }
    }
    if (holder.childPid && isPidAlive(holder.childPid)) {
      try {
        process.kill(holder.childPid, "SIGTERM");
      } catch {
        /* already gone */
      }
    }
  }

  private startIpcServer(): void {
    // Probe before unlink — never steal another daemon's socket (P0-3).
    if (existsSync(this.ipcPath)) {
      const probe = net.createConnection(this.ipcPath);
      probe.once("connect", () => {
        probe.destroy();
        this.log(`IPC socket ${this.ipcPath} is served elsewhere; not starting holders IPC`);
      });
      probe.once("error", () => this.bindIpc());
      probe.setTimeout(500, () => {
        probe.destroy();
        this.bindIpc();
      });
      return;
    }
    this.bindIpc();
  }

  private bindIpc(): void {
    try {
      if (existsSync(this.ipcPath)) unlinkSync(this.ipcPath);
    } catch {
      /* not present */
    }
    this.ipcServer = net.createServer(socket => {
      let buffer = Buffer.alloc(0);
      let holder: HolderState | undefined;
      socket.on("data", chunk => {
        buffer = Buffer.concat([buffer, chunk]);
        if (buffer.length > MAX_RELAY_BYTES) {
          socket.destroy();
          buffer = Buffer.alloc(0);
          return;
        }
        let idx: number;
        while ((idx = buffer.indexOf(HOLDER_LINE)) !== -1) {
          const line = buffer.subarray(0, idx).toString("utf-8").trim();
          buffer = buffer.subarray(idx + 1);
          if (!line) continue;
          let msg: Record<string, unknown>;
          try {
            msg = JSON.parse(line) as Record<string, unknown>;
          } catch {
            continue;
          }
          if (msg.type === "hello" && typeof msg.piSessionId === "string") {
            holder = this.holders.get(msg.piSessionId);
            if (holder) {
              holder.socket = socket;
              holder.socketPath = typeof msg.socketPath === "string" ? msg.socketPath : undefined;
              const session = this.roster.get(holder.piSessionId);
              if (session) socket.write(`${JSON.stringify({ type: "status", status: toCcStatus(session.status) })}\n`);
              this.log(`holder hello from ${holder.alias} (pid ${String(msg.pid)})`);
            }
            continue;
          }
          if (!holder) continue;
          if (msg.type === "cc-message") {
            this.relayCcToPi(holder, msg);
          } else if (msg.type === "reply") {
            void this.sendHolderReply(holder, msg);
          }
        }
      });
      socket.on("error", () => socket.destroy());
      socket.on("close", () => {
        if (holder && holder.socket === socket) holder.socket = undefined;
      });
    });
    this.ipcServer.listen(this.ipcPath, () => this.log(`holder IPC at ${this.ipcPath}`));
    this.ipcServer.on("error", error => this.log(`ipc: ${error.message}`));
  }

  // ── Relays ──────────────────────────────────────────────────────────────────

  /** pi → CC. Sender identity: the pi session's own holder entry (P0-4). */
  private async onPiToCc(ccSessionId: string, event: { from: BrokerSessionInfo; message: IntercomMessage }, client: BrokerClient): Promise<void> {
    const peer = this.peers.get(ccSessionId);
    if (!peer || peer.client !== client) return; // read live state, not closure snapshots (P1-6)
    const sender = event.from;
    const label = sender.name?.trim() || sender.id;
    if (!this.piLimiter.allow(`${sender.id}>${ccSessionId}`)) {
      this.log(`rate limit: pi ${label} → ${peer.name}; message held`);
      this.hold("rate-pi-to-cc", { from: label, to: peer.name, text: event.message.content.text });
      if (event.message.expectsReply) this.failAsk(client, sender.id, event.message.id, "bridge rate limit reached");
      return;
    }

    // Contact-driven exposure: first outbound message earns the holder (P1-13).
    this.contacted.add(sender.id);
    this.reconcileHolders();

    let holder = this.holders.get(sender.id);
    if (!holder) {
      // First contact: give the holder a moment to bind so the message can go
      // out natively (from pi-<alias>, reply-addressable). If it is still not
      // up in time, fall back to the funnel — the send is never lost.
      this.reconcileHolders();
      const deadline = Date.now() + 2_000;
      while (!holder && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 100));
        holder = this.holders.get(sender.id);
        if (holder?.socketPath) break;
      }
    }

    const native = holder?.socketPath !== undefined;
    let text = event.message.content.text;
    let token: string | undefined;
    try {
      if (this.stopping || this.peers.get(ccSessionId) !== peer) throw new Error("Claude peer disconnected");
      if (event.message.expectsReply) {
        const current = this.roster.get(sender.id);
        if (!current || current.pid !== sender.pid || current.endpointEpoch !== sender.endpointEpoch) throw new Error("pi sender disconnected or rebound");
        token = this.asks.add(this.askRoute(peer, sender.id), event.message.id);
        text = `${askInstructions(token)}\n${text}`;
      }
      await postToCcInbox({
        socketPath: peer.cc.messagingSocketPath,
        senderSocketPath: native ? holder!.socketPath! : ownSocketPath(this.paths),
        senderName: native ? `pi-${holder!.alias}` : this.config.peerName,
        senderMode: this.config.piSenderMode,
        text: native ? text : `[from pi session ${senderLabel(sender)}; reply with first line "To: ${label}"]\n${text}`,
        msgId: event.message.id,
        targetSessionId: peer.cc.sessionId, // CC drops it if the pid was recycled (P2-9)
      });
      client.receipt(event.message.id);
      this.log(`pi message ${event.message.id} handed to ${peer.name}${native ? ` via pi-${holder!.alias}` : " (funnel)"}`);
    } catch (error) {
      if (token) this.asks.cancel(token);
      this.log(`post to ${peer.name} failed: ${(error as Error).message}`);
      // Tell the pi sender instead of failing silently (P1-7).
      try {
        client.send(sender.id, {
          id: event.message.expectsReply ? randomUUID() : `err-${event.message.id}`.slice(0, 128),
          timestamp: Date.now(),
          ...(event.message.expectsReply ? { replyTo: event.message.id } : {}),
          content: { text: `Delivery failed: CC session ${peer.name} unreachable (${(error as Error).message}).` },
        });
      } catch {
        /* best effort */
      }
    }
  }

  private reachablePiList(): string | undefined {
    const names = [...this.holders.values()].map(h => `- pi-${h.alias}`);
    return names.length ? `Reachable Pi sessions:\n${names.join("\n")}` : undefined;
  }

  /** CC → pi via a holder entry. Trust policy + routing live HERE, not in the child. */
  private relayCcToPi(holder: HolderState, msg: Record<string, unknown>): void {
    const fromSocketRaw = typeof msg.fromSocket === "string" ? msg.fromSocket : undefined;
    const fromSocket = fromSocketRaw?.startsWith("uds:") ? fromSocketRaw.slice(4) : fromSocketRaw;
    const fromName = typeof msg.fromName === "string" ? msg.fromName : undefined;
    const fromMode = typeof msg.fromMode === "string" ? msg.fromMode : "unknown";
    let text = typeof msg.text === "string" ? msg.text : "";
    const msgId = typeof msg.msgId === "string" ? msg.msgId : `cc-${randomUUID()}`;
    if (!text) return;

    // Inbound trust: CC would hold bypass-class senders itself; mirror that (P0-6/P1-13).
    if (this.config.ccInbound === "hold-bypass" && fromMode !== "prompting" && fromMode !== "unknown-child") {
      const file = this.hold("bypass-sender", { fromName, fromMode, text, msgId });
      void this.sendHolderReply(holder, {
        toSocket: fromSocket,
        text: `Message held for review (sender class "${fromMode}"): ${file}`,
        msgId: `held-${msgId}`.slice(0, 128),
      });
      this.log(`held bypass-class sender message from ${fromName ?? fromSocket}`);
      return;
    }

    if (!this.ccLimiter.allow(`${fromSocket ?? "?"}>${holder.piSessionId}`)) {
      this.hold("rate-cc-to-pi", { fromName, text, msgId });
      this.log(`rate limit: ${fromName ?? fromSocket} → ${holder.alias}; held`);
      return;
    }

    if (Buffer.byteLength(text) > MAX_RELAY_BYTES) {
      const file = this.hold("oversize", { fromName, text, msgId });
      text = `${text.slice(0, 4000)}\n…[truncated; full text: ${file}]`;
    }

    if (text.trim() === "/list") {
      void this.sendHolderReply(holder, {
        toSocket: fromSocket,
        text: this.reachablePiList() ?? "No other pi sessions are currently connected to intercom.",
        msgId: `list-${msgId}`.slice(0, 128),
      });
      return;
    }

    // Cross-routing: explicit `To:` header only (never guessed) (P1-8/P1-9).
    const parsed = parseToHeader(text);
    if (parsed) {
      const target = matchTarget(parsed.target, [...this.holders.values()].map(h => ({ id: h.piSessionId, name: h.alias })));
      if (!target) {
        const file = this.hold("unknown-address", { fromName, text, msgId, to: parsed.target });
        this.log(`held: explicit To:${parsed.target} matches no pi session (${file})`);
        return;
      }
      const targetHolder = this.holders.get(target);
      if (targetHolder) {
        this.deliverToHolder(targetHolder, fromName, fromSocket, parsed.rest, msgId, peerNameFor(fromName, fromSocket, this.peers));
        return;
      }
    }

    this.deliverToHolder(holder, fromName, fromSocket, text, msgId, peerNameFor(fromName, fromSocket, this.peers));
  }

  private deliverToHolder(
    holder: HolderState,
    fromName: string | undefined,
    fromSocket: string | undefined,
    text: string,
    msgId: string,
    ccName: string,
  ): void {
    const peer = fromSocket ? [...this.peers.values()].find(p => p.cc.messagingSocketPath === fromSocket) : undefined;
    if (!peer) {
      // Unknown sender: re-sync and retry once, then HOLD — never misattribute (P1-4).
      this.syncPeers();
      setTimeout(() => {
        const retryPeer = fromSocket ? [...this.peers.values()].find(p => p.cc.messagingSocketPath === fromSocket) : undefined;
        if (retryPeer) {
          this.deliverToHolder(holder, fromName, fromSocket, text, msgId, ccNameFor(retryPeer));
        } else {
          const file = this.hold("unknown-sender", { fromName, fromSocket, text, msgId });
          this.log(`held: sender ${fromName ?? fromSocket} matches no CC session (${file})`);
        }
      }, 3_000).unref();
      return;
    }
    if (this.relayCcMessage(peer, holder.piSessionId, text, msgId)) {
      this.pending.set(msgId, { fromSocket, alias: holder.alias, at: Date.now() });
      this.log(`cc message from ${ccNameFor(peer)} relayed to ${holder.alias}`);
    }
  }

  private onDeliveryFailed(messageId: string, reason: string): void {
    const p = this.pending.get(messageId);
    if (!p) return;
    this.pending.delete(messageId);
    this.hold("delivery_failed", { ...p, reason });
    if (p.fromSocket) {
      void this.noticeToCc(p.fromSocket, p.alias, `Not delivered to pi-${p.alias}: ${reason}. Queued at held/.`);
    }
  }

  private sweepPending(): void {
    this.asks.sweep();
    const now = Date.now();
    for (const [msgId, p] of this.pending) {
      if (now - p.at < 60_000) continue;
      this.pending.delete(msgId);
      if (p.fromSocket) {
        void this.noticeToCc(p.fromSocket, p.alias, `pi-${p.alias} has not picked up your message after 60s (busy or non-interactive); queued at held/.`);
      }
    }
  }

  /** Child-initiated reply, posted back to the CC sender's own socket. */
  private async sendHolderReply(holder: HolderState, msg: Record<string, unknown>): Promise<void> {
    const toSocketRaw = typeof msg.toSocket === "string" ? msg.toSocket : undefined;
    const toSocket = toSocketRaw?.startsWith("uds:") ? toSocketRaw.slice(4) : toSocketRaw;
    if (!toSocket || typeof msg.text !== "string") return;
    await this.noticeToCc(toSocket, holder.alias, msg.text, typeof msg.msgId === "string" ? msg.msgId : undefined);
  }

  /**
   * Reply-target validation (P2-1): the daemon only ever posts to sockets that
   * belong to a live CC session record or one of our own holders. A forged
   * `from` in message content can never aim the bridge at an arbitrary socket.
   */
  private isAllowedReplySocket(socketPath: string): boolean {
    const bare = socketPath.startsWith("uds:") ? socketPath.slice(4) : socketPath;
    if (!bare.startsWith(ccSocketDir(this.paths) + "/")) return false; // only CC's runtime dir
    for (const peer of this.peers.values()) if (peer.cc.messagingSocketPath === bare) return true;
    for (const holder of this.holders.values()) if (holder.socketPath === bare) return true;
    for (const { record } of listRecordsSync(this.paths)) {
      if (record.kind !== "bridge" && record.messagingSocketPath === bare && isPidAlive(record.pid)) return true;
    }
    return false;
  }

  private async noticeToCc(socketPath: string, alias: string | undefined, text: string, msgId?: string): Promise<void> {
    if (!this.isAllowedReplySocket(socketPath)) {
      this.log(`refused reply to unvalidated socket ${socketPath}`);
      return;
    }
    try {
      await postToCcInbox({
        socketPath,
        senderSocketPath: ownSocketPath(this.paths),
        senderName: this.config.peerName,
        senderMode: this.config.piSenderMode,
        text,
        msgId: msgId ?? `notice-${randomUUID()}`.slice(0, 128),
      });
      this.log(`notice sent to ${socketPath}${alias === undefined ? "" : ` (about pi-${alias})`}`);
    } catch (error) {
      this.log(`notice to ${socketPath} failed: ${(error as Error).message}`);
    }
  }

  /** Unified hold: sanitized filename, never guessed onward. (P1-7, P1-17) */
  private hold(reason: string, payload: object): string {
    try {
      mkdirSync(this.heldDir, { recursive: true, mode: 0o700 });
      const file = join(this.heldDir, `${Date.now()}-${reason}-${randomUUID().slice(0, 8)}.json`);
      writeFileSync(file, JSON.stringify({ reason, ...payload, at: Date.now() }, null, 2), { flag: "wx", mode: 0o600 });
      return file;
    } catch (error) {
      this.log(`hold() failed: ${(error as Error).message}`);
      return HOLD_FAILED;
    }
  }

  private async onCcFrame(frame: ParsedInbound): Promise<void> {
    if (frame.kind !== "user") {
      if (frame.kind === "action") this.log(`cc action frame: ${frame.action} (ignored)`);
      else if (frame.kind === "unknown") this.log(`cc line dropped: ${frame.reason}`);
      return;
    }
    const fromSocket = frame.line.from ?? frame.meta.from;
    const fromName = frame.meta.fromName ?? fromSocket ?? "claude-session";

    if (frame.text.trim() === "/list") {
      const list = this.reachablePiList();
      try {
        await postToCcInbox({
          socketPath: (fromSocket ?? "").startsWith("uds:") ? fromSocket!.slice(4) : fromSocket ?? "",
          senderSocketPath: ownSocketPath(this.paths),
          senderName: this.config.peerName,
          senderMode: this.config.piSenderMode,
          text: list
            ? `${list}\n(You can SendMessage these directly, or use the funnel with a leading "To: <name>" line.)`
            : "No other pi sessions are currently connected to intercom.",
          msgId: `list-${frame.line.msg_id}`.slice(0, 128),
        });
        this.log(`list query from ${fromName} answered with ${this.holders.size} sessions`);
      } catch (error) {
        this.log(`list reply to ${fromName} failed: ${(error as Error).message}`);
      }
      return;
    }

    const route = routeFunnel(frame.text, this.roster.values(), this.config.routeTo);
    const { target, text } = route;
    if (!target) {
      const file = this.hold("funnel-unrouted", { fromName, text, msgId: frame.line.msg_id });
      this.log(`cc message from ${fromName} held (no route): ${file}`);
      const socket = fromSocket?.startsWith("uds:") ? fromSocket.slice(4) : fromSocket;
      const registered = socket && listRecordsSync(this.paths).some(({ record }) => record.messagingSocketPath === socket && isLiveCcRecord(record));
      if (!socket || (registered && !existsSync(socket))) {
        this.log(`skipped funnel notice to ${fromName}: sender socket absent`);
        return;
      }
      if (!registered) {
        this.log(`refused funnel notice to ${fromName}: no live CC record for ${socket}`);
        return;
      }
      const reason = route.reason === "to-miss" ? `To: target <${route.requested}> matches no Pi session`
        : route.reason === "several" ? `no To: line and ${route.count} Pi sessions connected` : "no Pi sessions connected";
      const saved = file === HOLD_FAILED ? "Could not save the message in held/." : "Message saved in held/.";
      const list = this.reachablePiList();
      await this.noticeToCc(socket, undefined, `Not delivered: ${reason}. ${saved}\n${list ? `${list}\n` : ""}` +
        "Resend directly to pi-<alias> or put To: <alias> on the first line.");
      return;
    }
    const peer = this.findPeerBySender(fromSocket);
    const client = peer?.client;
    if (!client) {
      this.syncPeers();
      setTimeout(() => {
        const retry = this.findPeerBySender(fromSocket);
        if (!retry) {
          const file = this.hold("unknown-sender", { fromName, fromSocket, text, msgId: frame.line.msg_id });
          this.log(`held: funnel sender ${fromName} matches no CC session (${file})`);
          return;
        }
        void this.relayFunnelToPi(retry, target, text, frame.line.msg_id, fromName);
      }, 3_000).unref();
      return;
    }
    void this.relayFunnelToPi(peer, target, text, frame.line.msg_id, fromName);
  }

  private async relayFunnelToPi(peer: Peer, target: string, text: string, msgId: string, fromName: string): Promise<void> {
    if (this.relayCcMessage(peer, target, text, msgId)) this.log(`cc message from ${fromName} relayed to ${target}`);
  }

  private askRoute(peer: Peer, target: string) {
    return { ccSessionId: peer.cc.sessionId, piSessionId: target,
      ccIdentity: JSON.stringify([peer.cc.pid, peer.cc.procStart, peer.cc.messagingSocketPath]) };
  }

  private failAsk(client: BrokerClient, target: string, replyTo: string, reason: string): void {
    try {
      client.send(target, { id: randomUUID(), timestamp: Date.now(), replyTo, content: { text: `Delivery failed: ${reason}` } });
    } catch (error) { this.log(`ask failure reply failed: ${(error as Error).message}`); }
  }

  private relayCcMessage(peer: Peer, target: string, text: string, msgId: string): boolean {
    const reply = this.asks.match(this.askRoute(peer, target), text);
    if (text.startsWith("Bridge-Reply:") && !reply) {
      this.hold("invalid-ask-reply", { from: peer.cc.sessionId, target, text, msgId });
      return false;
    }
    try {
      peer.client.send(target, { id: msgId, timestamp: Date.now(),
        content: { text: `Message from Claude Code session ${ccNameFor(peer)}:\n${reply?.text ?? text}` },
        ...(reply ? { replyTo: reply.replyTo } : {}),
      });
      if (reply) this.asks.cancel(reply.token);
      return true;
    } catch (error) {
      this.hold("send-failed", { from: peer.cc.sessionId, target, text, msgId, error: (error as Error).message });
      return false;
    }
  }

  private findPeerBySender(senderSocket: string | undefined): Peer | undefined {
    if (!senderSocket) return undefined;
    const bare = senderSocket.startsWith("uds:") ? senderSocket.slice(4) : senderSocket;
    for (const peer of this.peers.values()) {
      if (peer.cc.messagingSocketPath === bare) return peer;
    }
    return undefined;
  }

  private agentDir(): string {
    return (
      this.config.agentDir?.trim() ||
      process.env.PI_CODING_AGENT_DIR?.trim() ||
      join(process.env.HOME ?? "", ".pi", "agent")
    );
  }
}

function peerNameFor(fromName: string | undefined, fromSocket: string | undefined, peers: Map<string, Peer>): string {
  if (!fromSocket) return fromName ?? "unknown";
  const bare = fromSocket.startsWith("uds:") ? fromSocket.slice(4) : fromSocket;
  for (const peer of peers.values()) {
    if (peer.cc.messagingSocketPath === bare) return ccNameFor(peer);
  }
  return fromName ?? "unknown";
}

/** Human label for a pi session; runtime fallback aliases read as strangers. */
function senderLabel(sender: { name?: string; cwd?: string; id: string }): string {
  const name = sender.name?.trim();
  if (name && !isFallbackAlias(name)) return name;
  return `pi session ${derivedCcName(sender.id, sender.cwd)}`;
}

function ccNameFor(peer: Peer): string {
  return peer.cc.name?.trim() ? peer.cc.name.trim() : `pid-${peer.cc.pid}`;
}
