export * as BackgroundNotice from "./background-notice.js"

import { Effect, Schema } from "effect"
import type { Job } from "../job.js"
import type { Session } from "../session.js"
import { ShellResult } from "../shell/result.js"
import type { SessionMessage } from "./message.js"
import { SubagentOutcome } from "./subagent-outcome.js"

export type Input = Job.Terminal & {
  id: string
  recovery: Job.Recovery
  notificationID?: SessionMessage.ID
  resume?: boolean
}

/**
 * Script.it agent block metadata. Native children inherit it, so it marks every Session of a
 * block's tree. Only a request of the Script.it bridge starts work in a block tree.
 */
export const isBlock = Schema.is(Schema.Struct({ scriptit: Schema.Struct({ kind: Schema.Literal("block") }) }))

/**
 * Admits one background job's terminal as a synthetic notice to the Session that owns it, then
 * acknowledges the durable marker. Live observers and restart recovery share this path, so a
 * notice renders the same from the same terminal whenever it is delivered. A user stop is a quiet
 * notice: recorded for the model's next step, never a wake of an idle Session. So is every notice
 * in a block tree.
 */
export const deliver = Effect.fnUntraced(function* (
  sessions: Pick<Session.Interface, "get" | "synthetic">,
  jobs: Pick<Job.Interface, "completeBackground">,
  input: Input,
) {
  const notice = input.recovery.kind === "shell" ? shell(input, input.recovery) : subagent(input, input.recovery)
  const sessionID = input.recovery.kind === "shell" ? input.recovery.sessionID : input.recovery.parentSessionID
  const quiet =
    input.resume === false || notice.state === "stopped" || isBlock((yield* sessions.get(sessionID)).metadata)
  yield* sessions.synthetic({
    ...(input.notificationID ? { id: input.notificationID } : {}),
    sessionID,
    ...(quiet ? { resume: false } : {}),
    description: input.recovery.kind === "shell" ? input.recovery.command : input.recovery.description,
    text: notice.text,
    metadata: notice.metadata,
  })
  if (input.notificationID) yield* jobs.completeBackground(input.notificationID)
})

// A marker still `running` at delivery means the process died with the work: report it as cancelled.
function state(input: Job.Terminal, stopped: boolean): ShellResult.State {
  if (input.status === "completed") return stopped ? "stopped" : "completed"
  if (input.status === "error") return "error"
  return "cancelled"
}

function shell(input: Input, recovery: Extract<Job.Recovery, { kind: "shell" }>) {
  const outcome = input.result?.kind === "shell" ? input.result : undefined
  const ended = outcome ? ShellResult.state(outcome) : state(input, false)
  const text = outcome
    ? ShellResult.text(outcome)
    : input.status === "completed"
      ? (input.output ?? "Command completed")
      : input.status === "error"
        ? (input.error ?? "Command failed")
        : input.status === "running"
          ? "Command cancelled because the server restarted"
          : "Cancelled"
  return {
    state: ended,
    ...ShellResult.notification({
      jobID: input.id,
      shellID: recovery.shellID,
      command: recovery.command,
      state: ended,
      text,
      outcome,
    }),
  }
}

function subagent(input: Input, recovery: Extract<Job.Recovery, { kind: "subagent" }>) {
  const outcome = input.result?.kind === "subagent" ? input.result : undefined
  const ended = state(input, outcome?.status === "interrupted")
  const text = outcome
    ? outcome.status === "completed"
      ? outcome.text
      : SubagentOutcome.stopped
    : input.status === "completed"
      ? (input.output ?? "Subagent completed without a text response.")
      : input.status === "error"
        ? (input.error ?? "Subagent failed")
        : "Subagent cancelled"
  return {
    state: ended,
    text: `<subagent sessionID="${recovery.childSessionID}" state="${ended}" description="${recovery.description}">\n${text}\n</subagent>`,
    metadata: { source: "subagent", childID: recovery.childSessionID, agent: recovery.agent, state: ended },
  }
}
