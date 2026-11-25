import { fork, type ChildProcess, type Serializable } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { bindJob, type JobBinding } from "./binding.ts";
import { claudeJobArgs, jobInstructions, parseJobEvent, type TurnState } from "./protocol.ts";

export interface JobStart {
  prompt: string; promptDigest: string; cwd: string; runId: string; stepIndex: number;
  agent: string; options: Record<string, unknown>; sessionId?: string;
}
export interface JobHandle {
  providerJobId: string; state: "running" | "completed" | "failed" | "stopped";
  failureCode?: string; failureMessage?: string;
}
export interface JobResult extends JobHandle { output?: string }
interface Job {
  id: string; fingerprint: string; sessionId: string; nonce: string; turn: TurnState;
  handle: JobResult; guardian?: ChildProcess; binding?: JobBinding; stopping: boolean;
  controls: Map<string, string>; timer?: NodeJS.Timeout; pending: number;
}
export interface JobOwner { id: string; file?: string; cwd: string; parentTarget: string; agentDir: string }
export interface ProviderDependencies {
  bind?: typeof bindJob;
  command?: string;
  args?: string[];
  env?: NodeJS.ProcessEnv;
}
function text(value: unknown, max: number): value is string {
  return typeof value === "string" && value.trim().length > 0 && !value.includes("\0") && Buffer.byteLength(value) <= max;
}
function fail(code: string): never { throw Object.assign(new Error(`Claude jobs: ${code}`), { code }); }
const digest = (value: string) => createHash("sha256").update(value).digest("hex");

export class ClaudeJobs {
  readonly name = "claude-worker";
  private readonly jobs = new Map<string, Job>();
  private readonly starts = new Map<string, Job>();
  private closed = false;

  constructor(readonly owner: JobOwner, private readonly deps: ProviderDependencies = {}) {}

  private owns(sessionId?: string): boolean { return sessionId === this.owner.id || (!!this.owner.file && sessionId === this.owner.file); }
  private job(id: string): Job { return this.jobs.get(id) ?? fail("unknown-job"); }
  status = (id: string): JobHandle => {
    const { output: _output, ...handle } = this.job(id).handle;
    return { ...handle };
  };
  result = (id: string): JobResult => ({ ...this.job(id).handle });
  reattach = (id: string): JobHandle => this.status(id);
  phase(id: string): string { return this.job(id).turn.phase; }

  async start(input: JobStart): Promise<JobHandle> {
    if (this.closed || !this.owns(input.sessionId)) fail("owner-unavailable");
    if (!text(input.prompt, 64 * 1024) || digest(input.prompt) !== input.promptDigest ||
        !text(input.runId, 128) || !Number.isSafeInteger(input.stepIndex) || input.stepIndex < 0 ||
        !text(input.agent, 128) || !text(input.cwd, 4096)) fail("invalid-start");
    const options = input.options;
    if (!options || typeof options !== "object" || Array.isArray(options) || Object.keys(options).some(key => !["scope", "timeoutMs", "parentTarget"].includes(key)) ||
        !text(options.scope, 4096) || (options.parentTarget !== undefined && options.parentTarget !== this.owner.parentTarget)) fail("invalid-options");
    const timeoutMs = options.timeoutMs ?? 10 * 60_000;
    if (typeof timeoutMs !== "number" || !Number.isSafeInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > 15 * 60_000) fail("invalid-deadline");
    const cwd = realpathSync(input.cwd);
    const key = JSON.stringify([input.runId, input.stepIndex]);
    const fingerprint = digest(JSON.stringify([input.promptDigest, cwd, input.agent, options.scope, timeoutMs, this.owner.parentTarget]));
    const previous = this.starts.get(key);
    if (previous) {
      if (previous.fingerprint !== fingerprint) fail("duplicate-start-mismatch");
      return this.status(previous.id);
    }
    if (this.jobs.size >= 128) fail("history-capacity");
    if ([...this.jobs.values()].filter(job => job.handle.state === "running" || (job.guardian && job.guardian.exitCode === null)).length >= 2) fail("live-capacity");
    const id = randomUUID();
    const job: Job = { id, fingerprint, sessionId: randomUUID(), nonce: randomUUID(),
      turn: { phase: "working" }, handle: { providerJobId: id, state: "running" },
      stopping: false, controls: new Map(), pending: 0 };
    // Reserve before asynchronous bridge validation so duplicate delivery never spawns twice.
    this.jobs.set(id, job);
    this.starts.set(key, job);
    job.timer = setTimeout(() => this.finish(job, "stopped", "deadline"), timeoutMs);
    try {
      job.binding = await (this.deps.bind ?? bindJob)({
        agentDir: this.owner.agentDir, parentTarget: this.owner.parentTarget,
        cwd: this.owner.cwd, callerPid: process.pid,
        onLost: () => this.finish(job, "stopped", "binding-lost"),
      });
      if (job.stopping || this.closed) { job.binding.close(); return this.status(id); }
      const guardian = fork(fileURLToPath(new URL("./process.mjs", import.meta.url)), [], {
        execPath: process.execPath, execArgv: [], stdio: ["ignore", "ignore", "ignore", "ipc"],
      });
      job.guardian = guardian;
      guardian.on("error", () => this.finish(job, "failed", "guardian-error"));
      guardian.on("exit", () => {
        if (!job.stopping) this.finish(job, "failed", "guardian-exit");
      });
      guardian.on("message", (raw: unknown) => {
        const message = raw as { type: string; line: string; pid: number; reason: string };
        if (job.stopping) return;
        if (message.type === "spawned") {
          job.binding!.watchWorker(job.sessionId, message.pid);
          this.send(job, { type: "input", text: jobInstructions({ nonce: job.nonce,
            parentTarget: job.binding!.parentTarget, bridgeName: job.binding!.bridgeName,
            scope: options.scope as string, runId: input.runId, stepIndex: input.stepIndex }) + "\n\nTask:\n" + input.prompt });
        } else if (message.type === "line") {
          try {
            if (parseJobEvent(message.line, job.sessionId, job.nonce, job.turn) === "complete") {
              job.handle.output = job.turn.done;
              this.finish(job, "completed");
            }
          } catch { this.finish(job, "failed", "stream-protocol-error"); }
        } else if (message.type === "stopping") this.finish(job, message.reason === "deadline" ? "stopped" : "failed", message.reason);
      });
      this.send(job, { type: "launch", command: this.deps.command ?? "claude",
        args: this.deps.args ?? claudeJobArgs(job.sessionId, `claude-job-${id}`), cwd,
        timeoutMs: Math.max(1, timeoutMs), env: this.deps.env ?? process.env });
    } catch { this.finish(job, "failed", "launch-or-binding-error"); }
    return this.status(id);
  }

  private send(job: Job, message: Serializable): void {
    if (!job.guardian?.connected || job.pending >= 16) { this.finish(job, "failed", "ipc-unavailable"); return; }
    job.pending++;
    job.guardian.send(message, error => {
      job.pending--;
      if (error) this.finish(job, "failed", "ipc-error");
    });
  }
  private finish(job: Job, state: JobHandle["state"], code?: string): void {
    if (job.stopping) return;
    job.stopping = true;
    job.handle.state = state;
    if (code) { job.handle.failureCode = code; job.handle.failureMessage = `Claude worker ended: ${code}`; }
    clearTimeout(job.timer);
    job.binding?.close();
    if (job.guardian?.connected) {
      job.guardian.send({ type: "cancel" }, () => {});
      job.guardian.disconnect();
    }
  }
  control(sessionId: string, id: string, action: "cancel" | "steer", requestId: string, message?: string): JobHandle {
    if (this.closed || !this.owns(sessionId)) fail("owner-unavailable");
    const job = this.job(id);
    if (!text(requestId, 128) || (action !== "cancel" && action !== "steer") || (action === "steer" && !text(message, 16 * 1024))) fail("invalid-control");
    const fingerprint = digest(JSON.stringify([action, message]));
    const previous = job.controls.get(requestId);
    if (previous) {
      if (previous !== fingerprint) fail("duplicate-control-mismatch");
      return this.status(id);
    }
    if (job.stopping) fail("job-not-running");
    if (job.controls.size >= 128) fail("control-capacity");
    job.controls.set(requestId, fingerprint);
    if (action === "cancel") this.finish(job, "stopped", "explicit-cancel");
    else {
      job.turn.done = undefined;
      job.turn.phase = "working";
      this.send(job, { type: "input", text: `Bound parent steering (${requestId}):\n${message}` });
    }
    return this.status(id);
  }
  close(): void {
    this.closed = true;
    for (const job of this.jobs.values()) this.finish(job, "stopped", "session-shutdown");
  }
}
