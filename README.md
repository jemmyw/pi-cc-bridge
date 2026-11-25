# pi-cc-bridge

Optional: [Claude external-job writer provider](docs/claude-jobs.md), loaded separately. Normal bridge sessions and stock pi-subagents stay unchanged.

![version](https://img.shields.io/badge/version-0.3.0-blue)
![license](https://img.shields.io/badge/license-MIT-green)
![node](https://img.shields.io/badge/node-%E2%89%A520.6-brightgreen)
![platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey)
![Claude Code](https://img.shields.io/badge/Claude_Code-%E2%89%A52.1.224-orange)
![pi-intercom](https://img.shields.io/badge/pi--intercom-%E2%89%A50.18.1-purple)

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
Claude sessions appear as Intercom peers, and one Claude-side bridge routes to Pi.

**Requires [pi-intercom](https://github.com/nicobailon/pi-intercom) — it is mandatory,
not optional. The bridge speaks its broker protocol; without it, nothing delivers.

```
pi sessions ⇄ pi-intercom broker ⇄ [pi-cc-bridge daemon] ⇄ CC inbox sockets ⇄ CC sessions
```

- **From pi**: every live Claude Code session shows up in `intercom list` as a
  `claude:<name>` peer. Message it with the ordinary intercom tool.
- **From Claude Code**: find the configured bridge recipient (`pi-bridge` by default)
  in `/list-agents` or `ListAgents`. Send `/list` to it for the current Pi roster,
  including Pi sessions that have not contacted Claude. Send all Pi-bound messages
  to that same recipient. Copy one returned `To:` line as the first line, then put
  the message on the next line:

  ```text
  To: planner
  Please review the patch.
  ```

  `/list` uses unique aliases when safe and JSON-quoted session IDs otherwise
  (for example, `To: "<session-id>"`). Replies to a Pi message should use the
  exact `To:` line shown above its payload, including the quotes. There is no
  default target, even when only one Pi session is connected.

The wire protocol was previously verified live against Claude Code 2.1.296.
The single-recipient flow is covered by an isolated daemon and pi-intercom 0.18.1
broker with a Claude-compatible inbox fixture. See [Design](docs/DESIGN.md).

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
  "logLevel": "normal"
}
```

**Upgrading:** Stop the old daemon before upgrading. Remove `routeTo`, `expose`,
`maxHolders` and any legacy `nativeEntries` setting. These no longer affect
routing. Replace direct `pi-<alias>` targets in prompts and automation with
`SendMessage` to `peerName` and a first-line `To:` address from `/list`. If
reusing a checkout, remove the obsolete `dist/holder.mjs` build artefact.

Claude-to-Pi relay requires the broker to advertise `exact-send-v1` and return
endpoint epochs (tested on pi-intercom 0.18.1). Older brokers can still list and
relay Pi-to-Claude; Claude-to-Pi messages are held with an upgrade notice.

## Running the daemon manually

The daemon requires `fs-ext` 2.1.1, built locally with node-gyp (Python, a C++
compiler and Node headers). Normal npm installation builds it. If installing
with lifecycle scripts disabled, review it then run `npm rebuild fs-ext` before
starting the daemon. No native addon is loaded for extension status/stop checks.

```bash
npm install
npm run build        # dist/cli.mjs
npm start            # or: bin/pi-cc-bridge.mjs
```

The daemon holds a kernel flock on the permanent private `daemon.lock.guard`
file for its lifetime. Never delete that guard while any daemon is running.
Normal exit removes only its own `daemon.lock` metadata; a crash releases flock
automatically. Legacy live or unknown-owner metadata is preserved conservatively;
stop older bridge versions before upgrading, as they do not honour the guard.

## Blocking asks

Intercom `ask` (`expectsReply: true`) adds a one-use `Bridge-Reply: <token>`
instruction to the Claude message. Claude must return that exact token line
with its answer, immediately after the required `To:` line. The bridge validates the token,
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
PI_CC_BRIDGE_STRICT=1 npm test  # real isolated broker, plus protocol and provider fixtures
```

Manual live round-trip: `npx tsx scripts/e2e-smoke.ts` (requires `claude` and
pi-intercom). `PI_INTERCOM_DIR` and `TSX_LOADER` override dependency paths.
It uses a temporary broker, uniquely named bridge recipient and Claude probe, and checks
exact nonce messages received through the broker, not Claude stdout. Only the
probe opts into `crossSessionInbound: "accept"` for that launch; bridge trust
settings stay unchanged. Owned children and matching records are cleaned in
`finally`; existing Claude sessions are never messaged. The smoke checks cold discovery and a round trip through the same recipient,
including a blocking ask and the original `replyTo`. Failure retains private diagnostic
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
- **CC → pi**: the default `ccInbound: "hold-bypass"` only relays `prompting`
  and `unknown-child` senders. `accept` opts out of the mode check, not sender
  validation or explicit addressing.
- **Discovery**: a validated live Claude sender of any mode can use `/list`.
  It returns Pi addresses only, not metadata, and does not deliver to Pi.
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
