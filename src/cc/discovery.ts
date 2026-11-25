import { postToCcInbox, type PostOptions } from "./poster.ts";

export class CcDiscovery {
  private readonly hint: string;
  private readonly firstPosts = new Map<string, Promise<void>>();

  constructor(peerName: string) {
    this.hint = `[pi-cc-bridge] Send /list to the bridge peer ${JSON.stringify(peerName)} to find reachable Pi sessions, then message pi-<alias> directly.`;
  }

  async post(sessionId: string | undefined, options: PostOptions, alwaysHint = false): Promise<void> {
    if (sessionId) {
      // Concurrent sends wait for the first write. If it fails, the next send carries the hint.
      for (let first = this.firstPosts.get(sessionId); first; first = this.firstPosts.get(sessionId)) {
        if (await first.then(() => true, () => false)) {
          return postToCcInbox(alwaysHint ? this.withHint(options) : options);
        }
      }
    }
    const first = postToCcInbox(this.withHint(options));
    if (sessionId) this.firstPosts.set(sessionId, first);
    try {
      await first;
    } catch (error) {
      if (sessionId) this.firstPosts.delete(sessionId);
      throw error;
    }
  }

  private withHint(options: PostOptions): PostOptions {
    return { ...options, text: `${this.hint}\n\n${options.text}` };
  }
}
