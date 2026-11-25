# pi-cc-bridge — Design

> Built on [pi-intercom](https://github.com/nicobailon/pi-intercom) by nicobailon —
> this bridge only speaks its protocol; pi-intercom itself required zero changes.

Bridge **pi-intercom** (pi sessions) and **Claude Code cross-session messaging** (CC sessions)
using a single recipient on the Claude side.

- pi direction: a companion daemon registers synthetic pi-intercom peer sessions, one per live
  Claude Code session. pi sessions message `claude:<name>` through pi-intercom as usual.
- CC direction: the same daemon masquerades as a Claude Code session (registry record + inbox
  socket), so CC sessions see `pi-bridge` in `/list-agents` and `SendMessage` to it natively.

The original wire shapes were captured on Claude Code 2.1.296 / pi-intercom 0.17.0
(`docs/probes/`). Current isolated relay tests use pi-intercom 0.18.1.

## Architecture

```
pi sessions ⇄ pi-intercom broker ⇄ [pi-cc-bridge daemon] ⇄ CC inbox sockets
                      (one broker connection         ⇄ CC sessions registry (~/.claude/sessions)
                       per synthetic peer)
```

The daemon is a standalone process (spawned and supervised by the pi extension, like
pi-intercom's broker). It owns:

1. **Discovery loop** — polls the CC sessions registry, creates/updates/retires one broker
   connection + synthetic intercom session per live CC session.
2. **CC-facing endpoint** — binds one inbox socket + writes one registry record, named
   `peerName` (default `pi-bridge`), that CC sessions can address.
3. **Relay** — moves text between the two transports with the envelopes below.

## Claude Code protocol (captured from 2.1.296)

### Peer discovery

Each live CC session writes `~/.claude/sessions/<pid>.json` (also honors `CLAUDE_CONFIG_DIR`),
plus a `~/.claude/sessions/<pid>.<sha256(socketPath)>.key` auth file:

```json
{
  "pid": 4009,
  "sessionId": "41f88b21-ee42-406e-bacd-f0efedc4a15b",
  "cwd": "/home/mauro/projects/lynx-2.0",
  "startedAt": 1791666925609,
  "procStart": "2003",
  "version": "2.1.296",
  "peerProtocol": 1,
  "peerFeatures": ["notify_idle", "reply_across_default_dirs", "artifact_yield"],
  "kind": "interactive",
  "entrypoint": "cli",
  "pidDomain": "linux:<machine-hash>:pid:[<pid-ns-inode>]",
  "messagingSocketPath": "/run/user/1000/cc-socks/4009.sock",
  "name": "wave-1",
  "nameSource": "user",
  "nameSince": 1791666925754,
  "updatedAt": 1791693631983,
  "status": "busy",
  "statusUpdatedAt": 1791693631983
}
```

Socket path: `$XDG_RUNTIME_DIR/cc-socks/<pid>.sock` (fallback `/tmp/cc-socks-<uid>/<pid>.sock`;
`/run/user/<uid>/cc-socks/` is on CC's default-dirs allowlist, alongside `/tmp/cc-socks*`).

Listing = read the registry + liveness-check records. When CC sends to a peer it preflights
the socket's peer pid against the record (`expectPeerPid`, `expectPeerProcStart`,
`noFollowSymlink`) — so a masquerading bridge binds its own socket from its own pid and writes
its own record, self-consistent by construction.

### Inbound line (what a CC session's inbox accepts)

Newline-delimited JSON on the socket. First line may be an auth line
`{"type":"auth","token":"…"}` — optional on Linux (socket perms gate it), required on Windows.
Silent connections are dropped after 30 s. Then user lines:

```json
{
  "msgV": 1,
  "msg_id": "<unique id>",
  "type": "user",
  "message": {"role": "user", "content": "<cross-session-message from=\"uds:<sender-socket>\" from-name=\"<sender-name>\" from-mode=\"prompting\">\n<text>\n</cross-session-message>"},
  "priority": "next",
  "from": "<sender-socket-path>"
}
```

Observed sender behavior (mirror probe):

- `from` = the sender's own socket path; sender name/mode ride inside the
  `<cross-session-message …>` wrapper (`from-mode` is the permission-class self-ID:
  `prompting` vs bypass). `msgV: 1`.
- Optional `session_id` field must equal the receiving session's own id when present.
- No auth line is sent on Linux; no ack is written back; fire-and-forget.
- Other frames: `{"action":"notify_when_idle","from":…,"msg_id":…,"from_mode":…}`,
  `peer_idle_notice`, `type:"control"` (SDK control protocol), artifact-yield actions.

Malformed lines are dropped with logged reasons ("without valid type field",
"session_id mismatch (got …, expected …)").

### Deliver

Message is delivered **between tool calls** of an active turn, or starts a new turn if idle.
Plain text only; `@` file mentions carry nothing; commands arrive as inert text.

### Inbound controls (trust)

`crossSessionInbound`: `accept` / `hold` / `refuse`; per-message default from the two sessions'
permission-mode classes (bypass ↔ bypass delivers; anything else holds bypass senders,
delivering senders with prompting class to prompting receivers). Held messages surface an
approval dialog (`dialogExpiry`, 5 min default, 100 held max). Size cap ≈1 M chars; bursts and
loops are rate-limited (drop identical repeats, queue ≤ 50).

Recommendation for CC worker sessions that should accept pi traffic unattended:
`claude -p … --settings '{"crossSessionInbound":"accept"}'` or set it in the CC session.

## pi-intercom protocol

- Endpoint: `~/.pi/agent/intercom/broker.sock` (Unix), `broker.port.json` (Windows TCP).
- Frames: 4-byte big-endian length + JSON payload (see pi-intercom `broker/framing.ts`).
- Client flow: `{type:"register", sessionId, session}` → `{type:"registered", sessionId,
  features}`; `{type:"list", requestId}` → `{type:"sessions",…}`; `{type:"send", to, message}`;
  inbound `{type:"message", from, message}`; receipts
  `{type:"message_receipt", receipt:{messageId,status:"receiver_received"}}`;
  heartbeat = any frame ≥ `livenessIntervalMs` (send `{type:"presence"}`).
- `sessionId`: any non-empty string (`cc-<ccSessionId>` for synthetic peers; stable across
  daemon restarts).
- `SessionRegistration` requires `cwd`, `model`, `pid`, `startedAt`, `lastActivity`
  (strings/numbers), optional `name`, `status`, `acknowledgesReceipts`, `livenessIntervalMs`.
- One registered session per connection (`currentKey`), so the daemon opens one broker
  connection per CC peer — that is what makes inbound `from` correct.

## Identity mapping

| CC concept | Bridge representation |
|---|---|
| CC session record `<pid>.json` | Synthetic intercom session `cc-<sessionId>`, name `claude:<name>` (fallback `claude-pid-<pid>`; `#<pid>` suffix on name collisions) |
| CC `status` | intercom presence `status` (idle/busy) |
| CC→pi message | relayed by the *sender peer's* connection, so pi sees `from = claude:<name>` |
| pi→CC message | From the daemon inbox socket and `peerName`; `from-mode` is `piSenderMode` (default `bypass`). Attribution and the exact reply `To:` line precede the unchanged payload. |
| pi→CC text | wrapped in `<cross-session-message>` exactly like a native CC sender |
| CC→pi text | wrapper stripped, replaced with a `Message from Claude Code session <name>` prefix |

### Claude to Pi addressing

Send `/list` to the bridge recipient to see all real Pi sessions in the current
Intercom roster. The daemon reads an initial snapshot from a mirrored peer and
tracks joins, leaves and presence changes; synthetic `cc-` peers are excluded.
Copy a returned `To:` line into the first line of the message to the bridge.
Bare addresses match exact IDs or case-insensitive aliases and must be unique.
A JSON-quoted address matches only the exact ID. List entries use unique safe
aliases or quoted IDs; reply and job instructions always use quoted IDs.

There is no last-sender, single-session or configured default. Invalid, missing,
unknown and ambiguous addresses are held and the validated sender is notified.
The bridge requires the negotiated `exact-send-v1` capability and target
`endpointEpoch` for each relay. The broker rejects departed or rebound endpoints
instead of falling back to an alias or partial ID. Broker failures and missing
receipts are held and reported to the original sender.

## Runtime layout

```
~/.pi/agent/pi-cc-bridge/
├── config.json        # { enabled, peerName, pollMs, logLevel, ccSessionsDir? }
├── daemon.lock        # owner metadata: `pid procStart`
├── daemon.lock.guard  # permanent private inode, kernel flock held for daemon lifetime
├── daemon.log
└── held/              # unroutable CC messages
```

Signal handling: SIGTERM/SIGINT → remove own CC registry record + socket, unregister broker
sessions, exit. Startup GC removes only records marked `bridgeOwner` whose PID is dead.

## Claude Code side (plugin)

`claude-plugin/` is a standard CC plugin (manifest `.claude-plugin/plugin.json`, skill
`skills/pi-bridge/SKILL.md`). It carries no protocol code — CC's native cross-session
messaging already speaks to the bridge — only awareness:

- how to find the bridge in `/list-agents` and ask it for `/list`,
- the first-line `To:` convention and exact reply instructions,
- notes on held-message semantics and `crossSessionInbound`.

Install: `/plugin marketplace add <path-or-repo>` then install `pi-cc-bridge`; or load in
place with `claude --plugin-dir <path>/claude-plugin`. `claude plugin validate` must pass.

## Version gating

Gate on the registry record's `peerProtocol` (currently `1`) and `peerFeatures`, not on
`version` strings. Records with an unsupported `peerProtocol` are skipped with a warning.
CC releases weekly; the wire shapes here are pinned by fixtures from a live 2.1.296 capture.

## Upstream asks (pi-intercom, nicobailon)

Small, additive PRs rather than a fork:

1. A documented **external-peer registration flag** so the overlay/UI can badge bridge peers
   (e.g. `external: true`) instead of them looking like pi sessions.
2. Optional delivery detail for "handed to external transport".

Both are cosmetic; the current registration format works unmodified today.

## Security notes

- All transports are same-uid local IPC; no network exposure anywhere.
- **Sender classes are honest** (P0-6): pi sessions have no approval gate by default, so
  the bridge declares them `bypass`-class to CC (`piSenderMode`). CC's own hold matrix then
  applies verbatim: prompting-mode CC sessions hold pi messages for user approval; only
  bypass↔bypass delivers unattended. Declaring `prompting` is an explicit operator
  assertion the bridge cannot verify.
- **Inbound validation:** The declared sender socket must match a live, non-bridge
  Claude registry record in the Claude runtime directory. Registry validation is
  not authentication against other same-UID processes. Relay attribution comes
  from the matched registry peer, not the claimed display name.
- **Bypass-class and unknown-mode CC messages are held** by default except
  `/list`, a rate-limited, read-only query allowed for validated Claude senders
  of any mode. It returns only addresses and does not inject anything into Pi.
- **Rate limits** (`relayPerMin`/`relayBurst`, default 6/min burst 3 per direction pair):
  excess traffic is held, not delivered — a CC↔pi agent loop self-extinguishes.
- Unroutable messages, delivery failures, and rate-limited traffic land in `held/` — the
  bridge never guesses. `SendMessage` success means the Claude socket accepted the write, not that Pi received it.
- The bridge never touches CC `.key` auth files and never writes or deletes other
  processes' registry records; ownership is marked out-of-band (`bridgeOwner`).
- Record identity fields are verified against CC's own records at startup
  (`selfCheckProcStart`) so parse drift cannot go silent.
