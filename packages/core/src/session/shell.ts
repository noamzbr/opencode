export * as SessionShell from "./shell.js"

import type { Session } from "@opencode/schema/session"
import type { SessionMessage } from "@opencode/schema/session-message"
import { Effect } from "effect"
import { Instance } from "../instance/service.js"
import { Plugin } from "../plugin/service.js"
import { Shell } from "../shell.js"
import { ShellResult } from "../shell/result.js"

// Bound the excerpt carried by the shell row, its durable `session.shell.ended` event, and the
// export. The tail is the larger half: failures and the exit line land at the end of the output.
const PREVIEW_HEAD_BYTES = 16 * 1024
const PREVIEW_TAIL_BYTES = 48 * 1024

export const start = Effect.fn("SessionShell.start")(function* (input: {
  session: Session.Info
  command: string
  messageID: SessionMessage.ID
  // Caller-supplied process environment. Not persisted and not in metadata, so
  // it stays out of the command text and the shell row.
  env?: Record<string, string>
}) {
  const instances = yield* Instance.Service
  const shell = yield* Plugin.awaitActivation.pipe(Effect.andThen(Shell.Service), instances.provide(input.session))
  const info = yield* shell.create({
    command: input.command,
    cwd: input.session.location.directory,
    timeout: 0,
    env: input.env,
    metadata: { sessionID: input.session.id, messageID: input.messageID, background: true },
  })
  // Keep completion tied to the original shell even if the Session moves.
  return {
    info,
    result: shell.result(info),
    output: Effect.gen(function* () {
      const latest = yield* shell.output(info.id, { cursor: Number.MAX_SAFE_INTEGER })
      if (latest.size <= PREVIEW_HEAD_BYTES + PREVIEW_TAIL_BYTES)
        return yield* shell.output(info.id, { limit: PREVIEW_HEAD_BYTES + PREVIEW_TAIL_BYTES })
      const head = yield* shell.output(info.id, { limit: PREVIEW_HEAD_BYTES })
      const tail = yield* shell.output(info.id, {
        cursor: latest.size - PREVIEW_TAIL_BYTES,
        limit: PREVIEW_TAIL_BYTES,
      })
      return {
        output: `${head.output}\n[... ${latest.size - head.cursor - PREVIEW_TAIL_BYTES} bytes omitted ...]\n${tail.output}`,
        cursor: tail.cursor,
        size: latest.size,
        truncated: true,
      }
    }).pipe(Effect.catchTag("Shell.NotFoundError", () => Effect.succeed(ShellResult.unavailable))),
  }
})
