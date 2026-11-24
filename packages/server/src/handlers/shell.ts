import { Job } from "@opencode/core/job"
import { Shell } from "@opencode/core/shell"
import { Location } from "@opencode/core/location"
import { Effect } from "effect"
import { HttpApiBuilder, HttpApiSchema } from "effect/http-api"
import { ShellNotFoundError } from "@opencode/protocol/errors"
import { Api } from "../api"
import { response } from "../location"

export const ShellHandler = HttpApiBuilder.group(Api, "server.shell", (handlers) =>
  Effect.gen(function* () {
    const jobs = yield* Job.Service
    return handlers
      .handle(
        "shell.list",
        Effect.fn(function* () {
          const shell = yield* Shell.Service
          return yield* response(shell.list())
        }),
      )
      .handle(
        "shell.create",
        Effect.fn(function* (ctx) {
          const shell = yield* Shell.Service
          const location = yield* Location.Service
          return yield* response(
            shell.create({ ...ctx.payload, cwd: ctx.payload.cwd || location.directory }).pipe(Effect.orDie),
          )
        }),
      )
      .handle(
        "shell.stop",
        Effect.fn(function* (ctx) {
          const shell = yield* Shell.Service
          // A shell tool's Job observes the stopped process in its own fiber, so a reply at the stop could land
          // before the Job settles, and a session interrupt in that window would cancel the Job and remove the
          // capture. The reply waits for the exit and for the Job, whose ID is the shell's; a user shell has no Job,
          // so that wait returns at once.
          return yield* response(
            shell.stop(ctx.params.id, "killed").pipe(
              Effect.andThen(shell.wait(ctx.params.id)),
              Effect.tap(() => jobs.wait({ id: ctx.params.id })),
              Effect.catchTag(
                "Shell.NotFoundError",
                () =>
                  new ShellNotFoundError({ id: ctx.params.id, message: `Shell command not found: ${ctx.params.id}` }),
              ),
            ),
          )
        }),
      )
      .handle(
        "shell.get",
        Effect.fn(function* (ctx) {
          const shell = yield* Shell.Service
          return yield* response(
            shell
              .get(ctx.params.id)
              .pipe(
                Effect.catchTag(
                  "Shell.NotFoundError",
                  () =>
                    new ShellNotFoundError({ id: ctx.params.id, message: `Shell command not found: ${ctx.params.id}` }),
                ),
              ),
          )
        }),
      )
      .handle(
        "shell.output",
        Effect.fn(function* (ctx) {
          const shell = yield* Shell.Service
          return yield* response(
            shell
              .output(ctx.params.id, { cursor: ctx.query.cursor, limit: ctx.query.limit })
              .pipe(
                Effect.catchTag(
                  "Shell.NotFoundError",
                  () =>
                    new ShellNotFoundError({ id: ctx.params.id, message: `Shell command not found: ${ctx.params.id}` }),
                ),
              ),
          )
        }),
      )
      .handle(
        "shell.remove",
        Effect.fn(function* (ctx) {
          const shell = yield* Shell.Service
          yield* shell.remove(ctx.params.id).pipe(Effect.catchTag("Shell.NotFoundError", () => Effect.void))
          return HttpApiSchema.NoContent.make()
        }),
      )
  }),
)
