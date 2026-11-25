# Opt-in Claude writer jobs

Load `pi -e /absolute/path/pi-cc-bridge/extensions/claude-jobs.ts` in the launching parent. This is separate from the normal bridge extension and is not auto-loaded by this package. No stock pi-subagents, profile, permission or settings changes are made.

Requires the existing running bridge, pi-intercom broker, authenticated local `claude`, and pi-subagents with the public `pi-subagents/external-job-provider` export (verified against 0.88.1). The extension resolves that public export from this package or the active profile's `npm` directory. Nothing is installed automatically. A missing package fails visibly.

The parent must use its default broker ID equal to `ctx.sessionManager.getSessionId()`. Custom broker IDs and native child sessions using custom intercom routes are unsupported: fail closed, no environment/alias fallback. One loaded session may own the `claude-worker` provider per Pi process. A conflicting owner fails visibly without replacing the first. Separate Pi processes are independent. Unload/reload cancels jobs before releasing ownership.

## Launch

Use the example [claude-job-worker profile](agents/claude-job-worker.md) through your normal approved project agent discovery. It is an example, not installed by this extension. Existing `claude-worker` and `claude-oracle` profiles stay unchanged.

```js
subagent({ agent: "claude-job-worker", task: "In src/example.ts only, implement the approved change and run its focused test.", async: true })
```

Runner `options` accept only:
- `scope`: required non-empty bounded scope, at most 4 KiB. Narrow it in the profile and task brief.
- `timeoutMs`: 100 ms to 15 minutes, default 10 minutes. It bounds the entire job including decision waits.
- `parentTarget`: optional assertion equal to the owning Pi session UUID, not an alias or another session.

No untrusted command, executable, argv, environment, model or shell template options are accepted. Prompts are at most 64 KiB. Each job gets unique provider and Claude session IDs. Start retries with the same run/step and identical digest/options reuse the existing handle; conflicting retries fail. At most two live processes and 128 lifetime job handles per loaded owner. Reload releases history but cannot reattach old jobs or redispatch them automatically.

## Decisions, steering and completion

The worker discovers the verified daemon's uniquely named Claude recipient, then uses SendMessage with `To: "<exact parent UUID>"` before every payload. The bridge name/socket/session identity is pinned against the verified daemon lock/record; parent broker pid/cwd/endpoint epoch and worker route are checked. Missing, rebound or disconnected identities cancel the job; no peer fallback or broadcast.

Reply through normal intercom to the **exact sender `cc-<Claude session UUID>`** shown on the question. This is cross-session messaging, not pi-subagents native `contact_supervisor` support. The existing exact Bridge-Reply ask protocol remains unchanged.

The worker emits a nonce-bound `WAIT` assistant marker when awaiting a decision. The provider keeps state `running`, never terminal `blocked`. `claude_job_control` reports its cooperative phase. Intermediate Claude result events do not finish the job. Only the matching `DONE` assistant marker followed by a successful matching Claude result completes it and closes the process. These markers describe trusted model intent, not proof of task correctness or approval.

```js
claude_job_control({ jobId: "<providerJobId>", action: "steer", requestId: "decision-1", message: "Use the approved smaller scope." })
claude_job_control({ jobId: "<providerJobId>", action: "cancel", requestId: "cancel-1" })
```

The tool requires the current owner context. Steering goes into the same open stdin stream, cooperatively between tool calls; queued delivery is not an execution acknowledgement. Retry with the same requestId and payload to avoid duplication. At most 128 control identities and 16 pending IPC messages; steering at most 16 KiB. `/claude-job-cancel <providerJobId>` is an explicit cancellation command. Use the provider job ID from stock external-job status, not the stock run ID.

**Stock subagent stop/timeout only stops local monitoring.** It does not cancel Claude. Use the provider cancel surface or wait for its hard deadline. Native stock steer, stop and supervisor channels are not promised. No `followUp` is offered: completed jobs are closed permanently, and stock resume is honestly unsupported, never silently a fresh conversation.

## Safety and lifecycle

Defaults match the existing writer profile: `acceptEdits`, Read/Write/Edit/Glob/Grep/Bash, adding only ListAgents/SendMessage. Strict empty MCP, user settings only, no browser, slash commands or session persistence. Inbound cross-session acceptance is enabled only on the dedicated launch. Review and trust user settings/hooks before use. Bash grants broad host access. Scope, sender/recipient restrictions and waiting are cooperative, not an OS sandbox or protection against malicious local peers/extensions. Broker IDs and process metadata are identity checks, not cryptographic authentication.

Start is owner-session-bound. Public status/result/reattach accept bearer job handles because the stock API supplies no caller identity for those operations; do not treat them as authenticated reads. Unknown/stale handles fail closed, never spawn or signal a recovered PID.

The provider owns a watchdog subprocess, which owns the dedicated Claude process group. Deadline, explicit cancellation, binding loss, reload, session switch/fork/shutdown, launch/parse errors and parent-process IPC loss terminate that owned group with TERM then KILL. No global process kills. The watchdog keeps its own deadline even if the parent event loop stalls. Host suspension, watchdog SIGKILL, or deliberately detached worker descendants are outside this best-effort process-tree guarantee. Completed/failed status initiates cleanup; it is not stock native process-terminal proof.

No raw stderr or stream logs are persisted by the provider. Frames are limited to 1 MiB, total stdout to 8 MiB, result text to 64 KiB, stdin buffering to 128 KiB. Limit/parse errors fail closed. Claude and stock Pi may keep their own normal artifacts/settings-hook output; do not send secrets in tasks, messages or results.

## Verification

```sh
PI_INTERCOM_DIR=/path/to/pi-intercom PI_CC_BRIDGE_STRICT=1 npm test
npm run typecheck
npm run build
# Explicit opt-in: real Claude, isolated broker, private Git fixture, owned probes only.
PI_CODING_AGENT_DIR=/path/to/active/profile PI_INTERCOM_DIR=/path/to/pi-intercom npx tsx scripts/claude-jobs-smoke.ts
```

The live smoke asks a decision through the bridge before first parent contact, verifies no early edit, replies through the exact synthetic peer and checks the retained-context edit and explicit completion. It does not use stdin to fake the answer. Deterministic tests exercise the real installed stock external-job runner/request bridge as well as fake Claude subprocesses. No normal session is messaged by the smoke. Review and live acceptance remain required before operational use.
