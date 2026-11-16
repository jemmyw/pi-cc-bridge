import { randomUUID } from "node:crypto";

export interface AskRoute {
  ccSessionId: string;
  ccIdentity: string;
  piSessionId: string;
}

interface PendingAsk extends AskRoute {
  messageId: string;
  expiresAt: number;
}

export function askTimeoutMs(): number {
  const raw = process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return 10 * 60 * 1000;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error("PI_INTERCOM_ASK_TIMEOUT_MS must be a positive integer number of milliseconds");
  return value;
}

export class AskReplies {
  private readonly pending = new Map<string, PendingAsk>();

  constructor(private readonly timeoutMs = askTimeoutMs(), private readonly limit = 256) {}

  add(route: AskRoute, messageId: string, now = Date.now()): string {
    this.sweep(now);
    if (this.pending.size >= this.limit) throw new Error("bridge pending ask limit reached");
    const token = randomUUID();
    this.pending.set(token, { ...route, messageId, expiresAt: now + this.timeoutMs });
    return token;
  }

  match(route: AskRoute, text: string, now = Date.now()): { token: string; replyTo: string; text: string } | undefined {
    this.sweep(now);
    const match = /^Bridge-Reply: ([0-9a-f-]{36})\r?\n([\s\S]+)$/.exec(text);
    if (!match) return undefined;
    const ask = this.pending.get(match[1]);
    if (!ask || ask.ccSessionId !== route.ccSessionId || ask.ccIdentity !== route.ccIdentity || ask.piSessionId !== route.piSessionId) return undefined;
    return { token: match[1], replyTo: ask.messageId, text: match[2] };
  }

  cancel(token: string): void { this.pending.delete(token); }

  cancelMessage(ccSessionId: string, piSessionId: string, messageId: string): void {
    for (const [token, ask] of this.pending) {
      if (ask.ccSessionId === ccSessionId && ask.piSessionId === piSessionId && ask.messageId === messageId) this.pending.delete(token);
    }
  }

  clearCc(ccSessionId: string): void {
    for (const [token, ask] of this.pending) if (ask.ccSessionId === ccSessionId) this.pending.delete(token);
  }

  clearPi(piSessionId: string): void {
    for (const [token, ask] of this.pending) if (ask.piSessionId === piSessionId) this.pending.delete(token);
  }

  clear(): void { this.pending.clear(); }

  sweep(now = Date.now()): void {
    for (const [token, ask] of this.pending) if (now >= ask.expiresAt) this.pending.delete(token);
  }
}

export function askInstructions(token: string): string {
  return `[Bridge ask: reply using SendMessage to this sender. Start your reply payload with exactly "Bridge-Reply: ${token}" on its own line, followed by your answer. For the funnel, keep its required To: line before Bridge-Reply. Do not reuse this token for notifications or later replies.]`;
}
