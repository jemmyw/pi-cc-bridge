import { randomUUID } from "node:crypto";
import { lstatSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { BRIDGE_OWNER, CcRecord, isTargetableRecord, ownSocketPath, ccSocketDir, CcPathsOptions, ensurePrivateDir, ensureSessionsDir } from "./cc/paths.ts";
import { gcOwnStaleRecords, listRecordsSync, procStart, selfCheckProcStart, pidDomainFor, writeOwnRecord, removeOwnRecord, isPidAlive } from "./cc/registry.ts";
import { startCcInbox, CcInboxServer } from "./cc/inbox.ts";
import { CcDiscovery } from "./cc/discovery.ts";
import { ParsedInbound } from "./cc/protocol.ts";
import { BrokerClient, IntercomMessage, BrokerSessionInfo } from "./intercom/client.ts";
import { BridgeConfig, bridgeStateDir } from "./config.ts";
import { addressLines, isPiSession, routeToPi, toCcStatus, toLineForId } from "./routing.ts";
import { RateLimiter } from "./ratelimit.ts";
import { AskReplies, askInstructions } from "./ask-replies.ts";
export { tryLock } from "./daemon-lock.ts";
export { runDaemon } from "./daemon.ts";

/** One Claude-side inbox; one synthetic Intercom peer per live Claude session. */
interface Peer {
  cc: CcRecord;
  client: BrokerClient;
  sessionId: string;
  name: string;
}

interface PendingRelay {
  sender: CcRecord;
  target: string;
  text: string;
  msgId: string;
  at: number;
}

const MAX_RELAY_BYTES = 900 * 1024; // below CC's wire cap
const HOLD_FAILED = "(hold failed)";
const isLiveCcRecord = (record: CcRecord) => record.pid !== process.pid && record.kind !== "bridge" && record.bridgeOwner !== BRIDGE_OWNER &&
  isTargetableRecord(record) && Number.isSafeInteger(record.pid) && isPidAlive(record.pid);

export class Bridge {
  private peers = new Map<string, Peer>(); // key: CC sessionId
  private inbox: CcInboxServer | null = null;
  private pollTimer: NodeJS.Timeout | null = null;
  private sweepTimer: NodeJS.Timeout | null = null;
  private stopping = false;
  private roster = new Map<string, BrokerSessionInfo>(); // by session id
  private rosterSource?: BrokerClient;
  private rosterRequest?: { id: string; at: number; changes: Map<string, BrokerSessionInfo | undefined> };
  private lastRosterRequestAt = 0;
  private rosterReady = false;
  private readonly ccSessionId = randomUUID();
  private retryAt = new Map<string, number>();
  private pending = new Map<string, PendingRelay>();
  private warnedOnce = new Set<string>();
  private logOnce = (key: string, msg: string) => {
    if (!this.warnedOnce.has(key)) {
      this.warnedOnce.add(key);
      this.log(msg);
    }
  };
  private readonly asks = new AskReplies();
  private readonly discovery: CcDiscovery;
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
    this.discovery = new CcDiscovery(config.peerName);
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

    // Bridge record: identity fields mirror verified CC shapes where possible.
    const record: CcRecord = {
      pid: process.pid,
      sessionId: this.ccSessionId,
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

    this.pollTimer = setInterval(() => this.syncPeers(), this.config.pollMs);
    this.pollTimer.unref?.();
    this.sweepTimer = setInterval(() => this.sweepPending(), 30_000);
    this.sweepTimer.unref?.();
    this.syncPeers();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    this.asks.clear();
    if (this.pollTimer) clearInterval(this.pollTimer);
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    for (const peer of this.peers.values()) peer.client.close();
    this.peers.clear();
    if (this.inbox) await this.inbox.close();
    removeOwnRecord(process.pid, this.paths);
    this.log("bridge stopped");
  }

  // ── CC peer discovery & lifecycle ────────────────────────────────────────────

  /** Align synthetic peers with the CC registry; reconnect with backoff (P0-2). */
  private syncPeers(): void {
    if (this.stopping) return;
    const records = listRecordsSync(this.paths);
    selfCheckProcStart(records, this.log);
    gcOwnStaleRecords(process.pid, this.paths);
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
    const usedNames = new Set([...this.peers.values()].map(peer => peer.name));

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
        this.refreshRosterSource();
      };
      client.on("disconnected", forget);
      client.on("error", (error: Error) => this.log(`peer ${name}: ${error.message}`));
      client.on("message", event => void this.onPiToCc(record.sessionId, event, client));
      client.on("message_control", event => {
        if (this.peers.get(record.sessionId)?.client === client && (event.control.action === "cancel" || event.control.action === "supersede")) this.asks.cancelMessage(record.sessionId, event.from.id, event.control.messageId);
      });
      client.on("registered", () => {
        this.log(`peer registered: ${name} (${sessionId})`);
        this.refreshRosterSource();
      });
      client.on("sessions", event => {
        const request = this.rosterRequest;
        if (client !== this.rosterSource || !request || request.id !== event.requestId) return;
        this.rosterRequest = undefined;
        const next = new Map<string, BrokerSessionInfo>(event.sessions.map((s: BrokerSessionInfo) => [s.id, s]));
        // The broker may respond asynchronously after newer roster events arrived.
        for (const [id, session] of request.changes) {
          if (session) next.set(id, session);
          else next.delete(id);
        }
        for (const [id, old] of this.roster) {
          const current = next.get(id);
          if (!current || current.pid !== old.pid || current.endpointEpoch !== old.endpointEpoch) this.asks.clearPi(id);
        }
        this.roster = next;
        this.rosterReady = true;
      });
      client.on("session_joined", event => this.updateRoster(client, event.session.id, event.session));
      client.on("session_left", event => this.updateRoster(client, event.sessionId));
      client.on("presence_update", event => this.updateRoster(client, event.session.id, event.session));
      client.on("delivery_failed", event => this.onDeliveryFailed(record.sessionId, event.messageId, event.reason));
      client.on("message_receipt", event => {
        const key = pendingKey(record.sessionId, event.receipt.messageId);
        if (this.pending.get(key)?.target === event.from.id &&
            (event.receipt.status === "injected" || event.receipt.status === "receiver_received")) this.pending.delete(key);
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

    this.refreshRosterSource();
  }
  private missCount = new Map<string, number>();
  private knownCcVersions = new Set<string>();
  private seenRecordShapes = new Set<string>();

  private refreshRosterSource(): void {
    if (this.stopping) return;
    const source = [...this.peers.values()].find(p => p.client.isRegistered && p.client.isConnected)?.client;
    if (source !== this.rosterSource) {
      this.rosterSource = source;
      this.rosterRequest = undefined;
      this.rosterReady = false;
      this.lastRosterRequestAt = 0;
      if (!source) {
        this.roster.clear();
        this.asks.clear();
      }
    }
    const now = Date.now();
    if (source && now - this.lastRosterRequestAt >= Math.max(1_000, this.config.pollMs * 4) &&
        (!this.rosterRequest || now - this.rosterRequest.at >= 5_000)) {
      this.rosterRequest = { id: source.list(), at: now, changes: new Map() };
      this.lastRosterRequestAt = now;
    }
  }

  private updateRoster(client: BrokerClient, id: string, session?: BrokerSessionInfo): void {
    if (client !== this.rosterSource) return;
    this.rosterRequest?.changes.set(id, session);
    const old = this.roster.get(id);
    if (!session || (old && (old.pid !== session.pid || old.endpointEpoch !== session.endpointEpoch))) this.asks.clearPi(id);
    if (session) this.roster.set(id, session);
    else this.roster.delete(id);
  }

  private async onPiToCc(ccSessionId: string, event: { from: BrokerSessionInfo; message: IntercomMessage }, client: BrokerClient): Promise<void> {
    const peer = this.peers.get(ccSessionId);
    if (!peer || peer.client !== client) return; // read live state, not closure snapshots (P1-6)
    const sender = event.from;
    if (!isPiSession(sender)) return;
    const replyLine = toLineForId(sender.id);
    const label = sender.name ? JSON.stringify(sender.name) : replyLine.slice("To: ".length);
    if (!this.piLimiter.allow(`${sender.id}>${ccSessionId}`)) {
      this.log(`rate limit: pi ${label} → ${peer.name}; message held`);
      this.hold("rate-pi-to-cc", { from: label, to: peer.name, text: event.message.content.text });
      if (event.message.expectsReply) this.failAsk(client, sender.id, event.message.id, "bridge rate limit reached");
      return;
    }

    const text = event.message.content.text;
    let token: string | undefined;
    try {
      if (this.stopping || this.peers.get(ccSessionId) !== peer) throw new Error("Claude peer disconnected");
      if (event.message.expectsReply) {
        const current = this.roster.get(sender.id);
        if (!this.rosterReady || !current || current.pid !== sender.pid || current.endpointEpoch !== sender.endpointEpoch) throw new Error("pi sender disconnected or rebound");
        token = this.asks.add(this.askRoute(peer, sender.id), event.message.id);
      }
      await this.discovery.post(peer.cc.sessionId, {
        socketPath: peer.cc.messagingSocketPath,
        senderSocketPath: ownSocketPath(this.paths),
        senderName: this.config.peerName,
        senderMode: this.config.piSenderMode,
        text: `From Pi session ${label}. To reply, send to ${JSON.stringify(this.config.peerName)} with this exact first line:\n${replyLine}\n` +
          (token ? `${askInstructions(token)}\n` : "") + `\nPi message:\n${text}`,
        msgId: event.message.id,
        targetSessionId: peer.cc.sessionId, // CC drops it if the pid was recycled (P2-9)
      });
      client.receipt(event.message.id);
      this.log(`pi message ${event.message.id} handed to ${peer.name}`);
    } catch (error) {
      if (token) this.asks.cancel(token);
      this.hold("post-failed", { from: sender.id, to: peer.name, text, msgId: event.message.id, error: (error as Error).message });
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

  private reachablePiList(): string {
    if (!this.rosterReady) return "The Pi roster is not available yet. Try /list again shortly.";
    const lines = addressLines(this.roster.values());
    return lines.length ? `Reachable Pi sessions:\n${lines.join("\n")}` : "No Pi sessions are currently connected to Intercom.";
  }

  private onDeliveryFailed(ccSessionId: string, msgId: string, reason: string): void {
    const key = pendingKey(ccSessionId, msgId);
    const pending = this.pending.get(key);
    if (!pending) return;
    this.pending.delete(key);
    void this.holdCc("delivery_failed", pending, `Broker could not deliver to ${pending.target}: ${reason}`);
  }

  private sweepPending(): void {
    this.asks.sweep();
    const now = Date.now();
    for (const [key, pending] of this.pending) {
      if (now - pending.at < 60_000) continue;
      this.pending.delete(key);
      void this.holdCc("delivery-timeout", pending, `No receipt from ${pending.target} after 60 seconds`);
    }
  }

  private liveCcSender(socketPath: string | undefined): CcRecord | undefined {
    if (!socketPath || dirname(socketPath) !== join(ccSocketDir(this.paths), ".")) return undefined;
    try {
      if (!lstatSync(socketPath).isSocket()) return undefined;
    } catch {
      return undefined;
    }
    const matches = listRecordsSync(this.paths).map(({ record }) => record)
      .filter(record => record.messagingSocketPath === socketPath && isLiveCcRecord(record));
    return matches.length === 1 ? matches[0] : undefined;
  }

  private async noticeToCc(sender: CcRecord, text: string, msgId = `notice-${randomUUID()}`, alwaysHint = false): Promise<void> {
    const socketPath = sender.messagingSocketPath;
    const current = this.liveCcSender(socketPath);
    if (!current || !sameCcIdentity(current, sender)) return;
    try {
      await this.discovery.post(sender.sessionId, {
        socketPath,
        senderSocketPath: ownSocketPath(this.paths),
        senderName: this.config.peerName,
        senderMode: this.config.piSenderMode,
        targetSessionId: sender.sessionId,
        text,
        msgId,
      }, alwaysHint);
    } catch (error) {
      this.log(`notice to ${socketPath} failed: ${(error as Error).message}`);
    }
  }

  private async holdCc(reason: string, payload: { sender: CcRecord; text: string; msgId: string; target?: string },
    explanation: string, correction = ""): Promise<void> {
    const { sender, text, msgId, target } = payload;
    const file = this.hold(reason, { from: sender.sessionId, fromSocket: sender.messagingSocketPath, text, msgId, target });
    this.log(`held ${msgId} from ${sender.sessionId} (${reason}): ${file}`);
    const saved = file === HOLD_FAILED ? "Could not save the message in held/." : "Message saved in held/.";
    await this.noticeToCc(sender, `Not delivered: ${explanation}. ${saved}${correction}`, undefined, Boolean(correction));
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
    if (this.stopping) return;
    if (frame.kind !== "user") {
      if (frame.kind === "action") this.log(`cc action frame: ${frame.action} (ignored)`);
      else if (frame.kind === "unknown") this.log(`cc line dropped: ${frame.reason}`);
      return;
    }
    const { text, line, meta } = frame;
    if (line.session_id !== undefined && line.session_id !== this.ccSessionId) return;
    const msgId = line.msg_id;
    const fromSocket = bareSocket(line.from ?? meta.from);
    const sender = this.liveCcSender(fromSocket);
    if (!sender || (line.from && meta.from && bareSocket(meta.from) !== fromSocket)) {
      this.hold("unknown-sender", { fromSocket, text, msgId });
      return;
    }
    const payload = { sender, text, msgId };

    // Discovery never injects into Pi, but only a registered Claude socket gets a reply.
    if (text.trim() === "/list") {
      if (!this.ccLimiter.allow(`list:${sender.sessionId}`)) {
        await this.holdCc("rate-list", payload, "bridge /list rate limit reached");
        return;
      }
      if (!this.rosterReady) {
        this.syncPeers();
        const deadline = Date.now() + 1_000;
        while (!this.rosterReady && !this.stopping && Date.now() < deadline) await sleep(25);
        if (this.stopping) return;
      }
      await this.noticeToCc(sender, `${this.reachablePiList()}\nCopy a To: line to the first line of your message to ${JSON.stringify(this.config.peerName)}.`, `list-${msgId}`.slice(0, 128));
      return;
    }
    if (this.config.ccInbound === "hold-bypass" && meta.fromMode !== "prompting" && meta.fromMode !== "unknown-child") {
      await this.holdCc("bypass-sender", payload, `sender class ${JSON.stringify(meta.fromMode ?? "unknown")} is held by policy`);
      return;
    }
    if (Buffer.byteLength(text) > MAX_RELAY_BYTES) {
      await this.holdCc("oversize", payload, "message exceeds the relay size limit");
      return;
    }

    const route = routeToPi(text, this.rosterReady ? this.roster.values() : []);
    if (!route.target) {
      const explanation = route.reason === "ambiguous" ? `To: target ${JSON.stringify(route.requested)} is ambiguous`
        : route.reason === "unknown" ? `To: target ${JSON.stringify(route.requested)} matches no reachable Pi session`
        : route.reason === "empty" ? "no message body after the To: line" : "missing or invalid first-line To: address";
      await this.holdCc(`route-${route.reason}`, payload, explanation, "\nSend /list to the bridge for a current, copyable To: line.");
      return;
    }
    const target = route.target;
    if (!this.ccLimiter.allow(`relay:${sender.sessionId}>${target.id}`)) {
      await this.holdCc("rate-cc-to-pi", { ...payload, target: target.id }, "bridge relay rate limit reached");
      return;
    }
    const peer = this.peers.get(sender.sessionId);
    if (!peer || !sameCcIdentity(peer.cc, sender) || !peer.client.isRegistered || !peer.client.isConnected) {
      this.syncPeers();
      await this.holdCc("sender-unavailable", payload, "your Intercom peer is not ready; resend shortly");
      return;
    }
    if (!peer.client.supportsExactSend || !target.endpointEpoch) {
      await this.holdCc("exact-send-unavailable", payload, "the Intercom broker does not support exact endpoint delivery; upgrade pi-intercom");
      return;
    }
    await this.relayCcMessage(peer, target, route.text, msgId, sender, text);
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

  private async relayCcMessage(peer: Peer, target: BrokerSessionInfo, text: string, msgId: string, sender: CcRecord, original: string): Promise<void> {
    const held = { sender, target: target.id, text: original, msgId };
    const reply = this.asks.match(this.askRoute(peer, target.id), text);
    if (text.startsWith("Bridge-Reply:") && !reply) {
      await this.holdCc("invalid-ask-reply", held, "invalid, expired or already used Bridge-Reply token");
      return;
    }
    const key = pendingKey(sender.sessionId, msgId);
    if (this.pending.has(key)) return;
    this.pending.set(key, { ...held, at: Date.now() });
    try {
      peer.client.sendExact(target, { id: msgId, timestamp: Date.now(),
        content: { text: `Message from Claude Code session ${ccNameFor(peer)}:\n${reply?.text ?? text}` },
        ...(reply ? { replyTo: reply.replyTo } : {}),
      });
      if (reply) this.asks.cancel(reply.token);
      this.log(`cc message from ${ccNameFor(peer)} submitted to ${target.id}`);
    } catch (error) {
      this.pending.delete(key);
      await this.holdCc("send-failed", held, `broker send failed: ${(error as Error).message}`);
    }
  }

  private agentDir(): string {
    return (
      this.config.agentDir?.trim() ||
      process.env.PI_CODING_AGENT_DIR?.trim() ||
      join(process.env.HOME ?? "", ".pi", "agent")
    );
  }
}

function bareSocket(socket?: string): string | undefined {
  return socket?.startsWith("uds:") ? socket.slice(4) : socket;
}

function sameCcIdentity(a: CcRecord, b: CcRecord): boolean {
  return a.sessionId === b.sessionId && a.pid === b.pid && a.procStart === b.procStart && a.messagingSocketPath === b.messagingSocketPath;
}

function pendingKey(ccSessionId: string, msgId: string): string {
  return JSON.stringify([ccSessionId, msgId]);
}

function ccNameFor(peer: Peer): string {
  return peer.cc.name?.trim() ? peer.cc.name.trim() : `pid-${peer.cc.pid}`;
}
