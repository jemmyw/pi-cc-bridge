# pi-cc-bridge

Optional: [Claude external-job writer provider](docs/claude-jobs.md), loaded separately. Normal bridge sessions and stock pi-subagents stay unchanged.

![version](https://img.shields.io/badge/version-0.3.0-blue)
![license](https://img.shields.io/badge/license-MIT-green)
![node](https://img.shields.io/badge/node-%E2%89%A520.6-brightgreen)
![platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey)
![Claude Code](https://img.shields.io/badge/Claude_Code-%E2%89%A52.1.224-orange)
![pi-intercom](https://img.shields.io/badge/pi--intercom-%E2%89%A50.17-purple)

> I use both Claude Code and pi, and I run coordinators and supervisors across
> multiple sessions on both. I wanted them talking to each other, so I built
> this extension: it interconnects them using Claude Code's own inter-session
> messaging protocol and the pi-intercom extension — with **no modifications**
> to either tool. Just install it, and your pi and Claude Code sessions talk
> to each other.
>
> — mauro

Bridge [pi-intercom](https://github.com/nicobailon/pi-intercom) and
[Claude Code cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging):
pi sessions and Claude Code sessions appear as native peers to each other.

**Requires [pi-intercom](https://github.com/nicobailon/pi-intercom) — it is mandatory,
not optional. The bridge speaks its broker protocol; without it, nothing delivers.

```
pi sessions ⇄ pi-intercom broker ⇄ [pi-cc-bridge daemon] ⇄ CC inbox sockets ⇄ CC sessions
```

- **From pi**: every live Claude Code session shows up in `intercom list` as a
  `claude:<name>` peer. Message it with the ordinary intercom tool.
- **From Claude Code**: every live pi session shows up in `/list-agents` as a
  `pi-<alias>` session (status mirrored). `SendMessage` it directly — or use
  the `pi-bridge` funnel with a `To: <pi-alias>` first line (or send `list`
  to discover pi sessions).

Both wire directions were verified live against Claude Code 2.1.296 and
pi-intercom 0.17.0; see `docs/DESIGN.md` for the captured protocol and
`docs/probes/` for the probe transcripts.

## It looks like this

Claude Code sees your pi sessions (`pi-*` — here the `pi-bridge` funnel and the
contact-driven `pi-deploy-coordinator` holder); pi sees every live Claude Code
session as `claude:<name>`:

| Claude Code — `/list-agents` | pi — `/intercom` |
| --- | --- |
| ![Claude Code /list-agents](docs/screenshots/cc-list-agents.png) | ![pi /intercom](docs/screenshots/pi-intercom-list.png) |

## Install (pi package)

```bash
pi install npm:pi-intercom       # 1. mandatory prerequisite — skip only if already installed
pi install npm:pi-cc-bridge      # 2. the bridge (npm once published; locally:
                                 #    pi install /home/mauro/projects/pi-cc-bridge)
```
Then restart pi. The extension starts the daemon (idempotent, one per machine)
and adds `/cc-bridge [status|start|stop]`.

The bridge does not auto-install pi-intercom for you; without it the daemon stays
up and keeps retrying, and `/cc-bridge status` flags the missing broker.

```bash
pi install npm:pi-cc-bridge     # once published


## Install the Claude Code side (optional but recommended)

The plugin only adds awareness (a skill); the messaging protocol needs nothing
from CC. Either load it in place:

```bash
claude --plugin-dir ~/projects/pi-cc-bridge/claude-plugin
```

or copy it into CC's plugin location, or add this repo as a marketplace:

```
/plugin marketplace add ~/projects/pi-cc-bridge/claude-plugin
/plugin install pi-cc-bridge@pi-cc-bridge
```

Validate: `claude plugin validate ~/projects/pi-cc-bridge/claude-plugin`.

## Credits

The pi side of this bridge exists because of
[pi-intercom](https://github.com/nicobailon/pi-intercom) by **nicobailon** — the bridge
speaks its broker protocol, and its clean, client-asserted registration meant this could
be a companion package rather than a fork. Zero changes were needed on pi-intercom's side.

## Platforms

Linux and macOS are supported. On macOS, native Claude Code identity uses
`pidDomain: "darwin"` and the UTC, C-locale `ps -o lstart=` timestamp; sockets default
to `/tmp/cc-socks`. Linux retains its `/proc` identity and
`$XDG_RUNTIME_DIR/cc-socks` default (fallback `/tmp/cc-socks-<uid>`).
Explicit socket-directory overrides take precedence on both platforms.
Directory ownership checks, private permissions, and messaging policies are unchanged.

## Configuration

`~/.pi/agent/pi-cc-bridge/config.json` (all optional):

```json
{
  "enabled": true,
  "peerName": "pi-bridge",
  "pollMs": 2000,
  "routeTo": "planner",
  "logLevel": "normal"
}
```

- `routeTo`: pi session alias that receives CC messages lacking a `To:` header.
- Messages that resolve to no route are held in `~/.pi/agent/pi-cc-bridge/held/`,
  never guessed.

## Running the daemon manually

The daemon requires `fs-ext` 2.1.1, built locally with node-gyp (Python, a C++
compiler and Node headers). Normal npm installation builds it. If installing
with lifecycle scripts disabled, review it then run `npm rebuild fs-ext` before
starting the daemon. No native addon is loaded for extension status/stop checks.

```bash
npm install
npm run build        # dist/holder.mjs — plain-node holder children (~53MB vs ~82MB)
npm start            # or: bin/pi-cc-bridge.mjs
```

The daemon holds a kernel flock on the permanent private `daemon.lock.guard`
file for its lifetime. Never delete that guard while any daemon is running.
Normal exit removes only its own `daemon.lock` metadata; a crash releases flock
automatically. Legacy live or unknown-owner metadata is preserved conservatively;
stop older bridge versions before upgrading, as they do not honour the guard.

## Blocking asks

Intercom `ask` (`expectsReply: true`) adds a one-use `Bridge-Reply: <token>`
instruction to the Claude message. Claude must return that exact first payload
line with its answer, after any funnel `To:` line. The bridge validates the token,
Claude session/endpoint and pi recipient before setting the original `replyTo`.
Unrelated notifications never answer an ask; invalid, duplicate or late token
replies are held. Ordinary sends are unchanged except that `Bridge-Reply:` is a
reserved first payload line.

Pending asks are capped at 256, without evicting active requests. Their timeout
matches pi-intercom's 10-minute default or the daemon's
`PI_INTERCOM_ASK_TIMEOUT_MS`. The bridge cannot know a different timeout in an
individual pi session. Broker-expired/cancelled asks reject late replies rather
than matching another request. Transport/capacity failures return a correlated
failure reply; disconnects and daemon shutdown clear pending state.

## Tests

```bash
npm run typecheck
npm test             # protocol fixtures from live captures + broker client integration
```

Manual live round-trip: `npx tsx scripts/e2e-smoke.ts` (requires `claude` and
pi-intercom). `PI_INTERCOM_DIR` and `TSX_LOADER` override dependency paths.
It uses a temporary broker, uniquely named funnel and Claude probe, and checks
exact nonce messages received through the broker, not Claude stdout. Only the
probe opts into `crossSessionInbound: "accept"` for that launch; bridge trust
settings stay unchanged. Owned children and matching records are cleaned in
`finally`; existing Claude sessions are never messaged. The smoke checks both
funnel delivery and a native holder round-trip with an actual blocking ask and
original `replyTo` assertion. Failure retains private diagnostic
tails and daemon logs in the reported temporary directory; success removes them.

## Memory-file sections

Paste-ready, token-conscious orientation for agents on each side:

- pi: `snippets/agents-md-section.md` → `AGENTS.md`
- Claude Code: `claude-plugin/snippets/claude-md-section.md` → `CLAUDE.md`

## Security & trust

Same-uid local IPC only; no network exposure.

- **pi → CC**: pi sessions are declared `bypass`-class (`piSenderMode`), because pi has no
  approval gate by default — so prompting-mode CC sessions hold pi messages for approval,
  exactly as CC would for any bypass sender.
- **CC → pi**: bypass-class CC senders are held (`ccInbound: "hold-bypass"`).
- **Exposure is opt-in**: `expose` (default `[]`) chooses which pi sessions appear in CC's
  `/list-agents` up front; every pi session that messages CC first gets an entry lazily.
- **Rate limits** per direction pair; excess is held.
- Anything unroutable, failed, or rate-limited lands in `~/.pi/agent/pi-cc-bridge/held/` —
  never silently dropped, never guessed.

See `docs/DESIGN.md` for the full model.

## Status

MVP. **Publish is deliberately on hold**: the package is in an extensive-use
trial on this machine first. Do not `npm publish` until that decision is made.
Install locally for trial use (see above). Known limits: CC session renames while the daemon runs are not re-synced;
cross-machine CC messaging (Remote Control relay) is out of scope; the pi→CC
direction inherits CC's inbound controls verbatim (bypass-mode CC sessions
will hold `prompting`-class messages for user approval — set
`crossSessionInbound: "accept"` on unattended CC workers).

---

**Keywords:** pi coding agent, Claude Code, pi-intercom, multi-agent coordination, cross-session messaging, agent bridge, coordinators, supervisors, subagents, agent teams, inter-agent communication, session orchestration, interoperability
