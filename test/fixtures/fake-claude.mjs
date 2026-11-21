import { createInterface } from "node:readline";
import { writeFileSync } from "node:fs";

let nonce;
let sessionId;
let turns = 0;
const mode = process.env.FAKE_CLAUDE_MODE;
function emit(event) {
  const record = { ...event, session_id: sessionId };
  const patch = process.env.FAKE_CLAUDE_EVENT_PATCH ? JSON.parse(process.env.FAKE_CLAUDE_EVENT_PATCH) : undefined;
  if (patch?.type === event.type) {
    Object.assign(record, patch.values);
    for (const field of patch.omit ?? []) delete record[field];
  }
  console.log(JSON.stringify(record));
}
function turn(text, result = text) {
  emit({ type: "assistant", message: { content: [{ type: "text", text }] } });
  emit({ type: "result", subtype: "success", is_error: false, result });
}
createInterface({ input: process.stdin }).on("line", line => {
  const text = JSON.parse(line).message.content;
  if (turns++ === 0) {
    nonce = /CLAUDE_JOB ([0-9a-f-]+) WAIT/.exec(text)?.[1];
    sessionId = process.argv[process.argv.indexOf("--session-id") + 1];
    if (process.env.FAKE_CLAUDE_PID) writeFileSync(process.env.FAKE_CLAUDE_PID, String(process.pid));
    if (mode === "invalid") return console.log("{not-json");
    if (mode === "overflow") return console.log("x".repeat(1024 * 1024 + 1));
    if (mode === "crash") return process.exit(3);
    if (mode === "hang") return;
    if (mode === "complete") {
      const header = `CLAUDE_JOB ${nonce} DONE\n`;
      const report = process.env.FAKE_CLAUDE_REPORT ?? "Implemented fixture";
      const resultReport = process.env.FAKE_CLAUDE_RESULT_REPORT ?? report;
      turn(header + report, header + resultReport);
      if (resultReport.trim() !== report.trim()) turn(`CLAUDE_JOB ${nonce} WAIT`);
      return;
    }
    turn(`CLAUDE_JOB ${nonce} WAIT`);
    return;
  }
  if (text.includes("unrelated")) return turn("An unrelated message is not a decision");
  turn(`CLAUDE_JOB ${nonce} DONE\nContinued in retained context, turns=${turns}`);
});
