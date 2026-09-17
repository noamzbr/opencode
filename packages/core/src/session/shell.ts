export * as SessionShell from "./shell.js"

import type { Session } from "@opencode/schema/session"
import type { SessionMessage } from "@opencode/schema/session-message"
import { Effect } from "effect"
import { Instance } from "../instance/service.js"
import { Plugin } from "../plugin/service.js"
import { Shell } from "../shell.js"
import { ShellResult } from "../shell/result.js"

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
    output: shell
      .output(info.id, { limit: 1024 * 1024 })
      .pipe(Effect.catchTag("Shell.NotFoundError", () => Effect.succeed(ShellResult.unavailable))),
  }
})
