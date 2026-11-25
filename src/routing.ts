export interface RosterEntryLike {
  id: string;
  name?: string;
}

const bareAddress = /^[a-zA-Z0-9_.:#-]+$/;

export function isPiSession(session: RosterEntryLike): boolean {
  return !session.id.startsWith("cc-");
}

export function toLineForId(id: string): string {
  return `To: ${JSON.stringify(id).replace(/\u2028/g, "\\u2028").replace(/\u2029/g, "\\u2029")}`;
}

export function parseToHeader(text: string): { target: string; exact: boolean; rest: string } | undefined {
  const newline = text.indexOf("\n");
  const firstLine = (newline === -1 ? text : text.slice(0, newline)).replace(/\r$/, "");
  if (!firstLine.startsWith("To:")) return undefined;
  const address = firstLine.slice(3).replace(/^[ \t]+|[ \t]+$/g, "");
  const rest = newline === -1 ? "" : text.slice(newline + 1);
  if (bareAddress.test(address)) return { target: address, exact: false, rest };
  if (address.startsWith('"')) {
    try {
      const target: unknown = JSON.parse(address);
      if (typeof target === "string" && target) return { target, exact: true, rest };
    } catch { /* Invalid quoted ID. */ }
  }
  return undefined;
}

function resolve<T extends RosterEntryLike>(target: string, exact: boolean, roster: T[]): T | "ambiguous" | undefined {
  const byId = roster.find(session => session.id === target);
  if (byId && !isPiSession(byId)) return undefined;
  if (exact) return byId;
  const lower = target.toLowerCase();
  const matches = roster.filter(session => isPiSession(session) && (session === byId || session.name?.toLowerCase() === lower));
  return matches.length > 1 ? "ambiguous" : matches[0];
}

export function addressLines(roster: Iterable<RosterEntryLike>): string[] {
  const sessions = [...roster];
  return sessions.filter(isPiSession).map(session => {
    const name = session.name;
    const safeName = name && bareAddress.test(name) && !name.startsWith("subagent-chat-") &&
      sessions.filter(other => isPiSession(other) && other.name?.toLowerCase() === name.toLowerCase()).length === 1 &&
      !sessions.some(other => other !== session && other.id.toLowerCase() === name.toLowerCase());
    return safeName ? `To: ${name}` : toLineForId(session.id);
  }).sort();
}

type PiRoute<T> = { target: T; text: string; reason?: never } |
  { target?: never; reason: "missing" | "empty" | "unknown" | "ambiguous"; requested?: string };

export function routeToPi<T extends RosterEntryLike>(text: string, roster: Iterable<T>): PiRoute<T> {
  const parsed = parseToHeader(text);
  if (!parsed) return { reason: "missing" };
  const target = resolve(parsed.target, parsed.exact, [...roster]);
  if (!target || target === "ambiguous") return { reason: target ?? "unknown", requested: parsed.target };
  if (!parsed.rest.trim()) return { reason: "empty" };
  return { target, text: parsed.rest };
}

export function toCcStatus(status: string | undefined): "idle" | "busy" {
  return !status || status.startsWith("idle") ? "idle" : "busy";
}
