import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { createRequire } from "node:module";
import { homedir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { ClaudeJobs, type JobStart, type JobHandle, type JobResult } from "../src/claude-jobs/provider.ts";

export interface ProviderApi {
  getExternalJobProvider(name: string): unknown;
  registerExternalJobProvider(provider: {
    name: string; start(input: JobStart): Promise<JobHandle>;
    status(id: string): JobHandle; result(id: string): JobResult; reattach(id: string): JobHandle;
  }): () => void;
}

export async function loadProviderApi(agentDir: string): Promise<ProviderApi> {
  const roots = [import.meta.url, pathToFileURL(join(agentDir, "npm", "package.json")).href];
  for (const root of roots) {
    try {
      const entry = createRequire(root).resolve("pi-subagents/external-job-provider");
      return await import(pathToFileURL(entry).href) as ProviderApi;
    } catch { /* Try the explicit caller profile package root, never a private registry. */ }
  }
  throw new Error("Claude jobs requires pi-subagents with its public external-job-provider export in this package or the active profile npm directory.");
}

export function registerClaudeJobs(api: ProviderApi, jobs: ClaudeJobs): () => void {
  if (api.getExternalJobProvider(jobs.name)) {
    throw new Error("Claude jobs provider already owned in this Pi process. Load it in only one session at a time; existing owner unchanged.");
  }
  return api.registerExternalJobProvider(jobs);
}

export default function claudeJobsExtension(pi: ExtensionAPI) {
  let jobs: ClaudeJobs | undefined;
  let dispose: (() => void) | undefined;
  let ownerId: string | undefined;
  const close = () => { jobs?.close(); dispose?.(); jobs = undefined; dispose = undefined; ownerId = undefined; };
  pi.on("session_start", async (_event, ctx) => {
    close();
    const agentDir = process.env.PI_CODING_AGENT_DIR?.trim() || join(homedir(), ".pi", "agent");
    ownerId = ctx.sessionManager.getSessionId();
    const api = await loadProviderApi(agentDir);
    jobs = new ClaudeJobs({ id: ownerId, file: ctx.sessionManager.getSessionFile(), cwd: ctx.cwd,
      // The binding verifies this exact ID against caller pid/cwd before any Claude launch.
      parentTarget: ownerId, agentDir });
    try { dispose = registerClaudeJobs(api, jobs); }
    catch (error) { close(); throw error; }
  });
  pi.on("session_shutdown", close);
  const current = (ctx: ExtensionContext) => {
    if (!jobs || ctx.sessionManager.getSessionId() !== ownerId) throw new Error("Claude jobs has no active caller-owned provider.");
    return jobs;
  };
  pi.registerTool({
    name: "claude_job_control", label: "Claude job control",
    description: "Cooperatively steer or actually cancel an owned Claude provider job. Use providerJobId, not stock run id. Steering is queued, not proof of execution. No resume after completion.",
    parameters: Type.Object({ jobId: Type.String(), action: Type.Union([Type.Literal("steer"), Type.Literal("cancel")]),
      requestId: Type.String({ description: "Stable unique delivery ID; retries reuse this ID" }), message: Type.Optional(Type.String()) }),
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const provider = current(ctx);
      const handle = provider.control(ctx.sessionManager.getSessionId(), params.jobId, params.action, params.requestId, params.message);
      return { content: [{ type: "text", text: JSON.stringify({ ...handle, phase: provider.phase(params.jobId), delivery: "queued" }) }], details: handle };
    },
  });
  pi.registerCommand("claude-job-cancel", {
    description: "Actually cancel an owned Claude provider job: /claude-job-cancel <providerJobId>",
    handler: async (args, ctx) => {
      const handle = current(ctx).control(ctx.sessionManager.getSessionId(), args.trim(), "cancel", `command-${args.trim()}`);
      ctx.ui.notify(`Claude job ${handle.providerJobId}: ${handle.state}`, "info");
    },
  });
}
