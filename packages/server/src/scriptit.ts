export * as ScriptitServer from "./scriptit"

import path from "path"
import { Effect, Layer, Schema, Stream } from "effect"
import { Bus } from "@opencode/core/bus"
import { Config } from "@opencode/core/config"
import { Database } from "@opencode/core/database/database"
import { Watcher } from "@opencode/core/filesystem/watcher"
import { InstructionDiscovery } from "@opencode/core/instruction-discovery"
import { Job } from "@opencode/core/job"
import { LocationActivity } from "@opencode/core/location-activity"
import { Session } from "@opencode/core/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionRestart } from "@opencode/core/session/execution/restart"
import { SessionInstructions } from "@opencode/core/session/instructions"
import { SessionStore } from "@opencode/core/session/store"
import { ShellResult } from "@opencode/core/shell/result"
import { WellKnown } from "@opencode/core/wellknown"
import type { TransformInput } from "@opencode/core/database/v1-migration"
import { ServerFetch } from "./fetch"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import type { ServerOptions } from "./options"

/** Embedded server profile: explicit configuration, durable events, and bridge-owned lifecycle. */
export interface Options {
  readonly app?: ServerOptions["app"]
  readonly password: string
  readonly databasePath: string
  readonly configFile?: string
  readonly modelsFile?: string
}

export const create = Effect.fnUntraced(function* (options: Options) {
  const handler = yield* ServerFetch.make(
    {
      app: options.app,
      password: options.password,
      // Durable rows are what the bridge replays from after a feed interruption.
      events: { persist: true },
      database: { path: options.databasePath },
      models: { file: options.modelsFile, fetch: false },
      fs: { filewatcher: false, fff: false },
    },
    {
      overrides: [
        Config.node.replace(Config.configured({ file: options.configFile, project: false, global: false })),
        Watcher.node.replace(configFileWatcher(options.configFile)),
        InstructionDiscovery.node.replace(InstructionDiscovery.configured({ project: false, global: false })),
        LocationActivity.node.replace(Layer.succeed(LocationActivity.Service, LocationActivity.Service.of({}))),
        WellKnown.node.replace(
          Layer.succeed(
            WellKnown.Service,
            WellKnown.Service.of({
              entries: () => Effect.succeed([]),
              cached: () => Effect.succeed([]),
              snapshot: () => [],
              refresh: () => Effect.succeed(false),
              add: () => Effect.fail(new Error("Well-known configuration is disabled")),
              remove: () => Effect.void,
              resolve: () => Effect.fail(new Error("Well-known configuration is disabled")),
            }),
          ),
        ),
        // The read tool asks this service to inject AGENTS.md files found above a read.
        SessionInstructions.node.replace(
          Layer.succeed(SessionInstructions.Service, SessionInstructions.Service.of({ load: () => Effect.void })),
        ),
        SessionRestart.node.replace(bootRecovery),
      ],
    },
  )
  return async (request: Request) => {
    if (new URL(request.url).pathname !== "/scriptit/internal/v1-transform") return handler(request)
    if (request.method !== "POST") return new Response(null, { status: 405 })
    if (request.headers.get("authorization") !== `Basic ${btoa(`opencode:${options.password}`)}`)
      return new Response(null, { status: 401 })
    const input = await request.json().catch(() => null)
    if (
      typeof input !== "object" ||
      input === null ||
      !("session" in input) ||
      !Array.isArray("messages" in input ? input.messages : null) ||
      !Array.isArray("parts" in input ? input.parts : null)
    )
      return new Response(null, { status: 400 })
    const { V1Migration } = await import("@opencode/core/database/v1-migration")
    return Response.json(V1Migration.transformSession(input as TransformInput))
  }
})

/** The native watcher for one file's entry in its directory; every other watch stays disabled. */
function configFileWatcher(file: string | undefined) {
  const target = file === undefined ? undefined : path.resolve(file)
  return Watcher.configured({ enabled: true }).mapLayer((layer) =>
    Layer.effect(
      Watcher.Service,
      Effect.gen(function* () {
        const watcher = yield* Watcher.Service
        return Watcher.Service.of({
          subscribe: (input, onReady) =>
            target !== undefined &&
            input.type === "entries" &&
            input.names.some((name) => path.resolve(input.path, name) === target)
              ? watcher.subscribe(
                  { path: path.dirname(target), type: "entries", names: [path.basename(target)] },
                  onReady,
                )
              : Effect.succeed(Stream.empty),
        })
      }),
    ).pipe(Layer.provide(layer)),
  )
}

/**
 * Stock restart recovery after a pass over what the previous server left
 * running. A user-shell row still marked running ends as `unavailable`, from
 * its saved `Shell.Started` info. A claimed agent-block Session answered a
 * bridge request that ended with that server; resumed, it would run with no
 * reader, so its execution fails and releases the claim instead.
 */
const bootRecovery = makeGlobalNode({
  service: SessionRestart.Service,
  layer: Layer.effect(
    SessionRestart.Service,
    Effect.gen(function* () {
      const bus = yield* Bus.Service
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      const shells = yield* database.db.all<{ data: string }>(RUNNING_SHELLS).pipe(Effect.orDie)
      yield* Effect.forEach(
        shells.map((row) => decodeShellStarted(row.data)),
        (started) =>
          bus.publish(SessionEvent.Shell.Ended, {
            sessionID: started.sessionID,
            shell: { ...started.shell, status: "unavailable" },
            output: ShellResult.unavailable,
          }),
        { discard: true },
      )
      yield* Effect.forEach(
        yield* store.listSuspended(),
        Effect.fnUntraced(function* (sessionID) {
          if (!isBlock((yield* store.get(sessionID))?.metadata)) return
          yield* bus.publish(
            SessionEvent.Execution.Failed,
            { sessionID, error: RESTART_ERROR },
            { commit: () => store.release(sessionID) },
          )
        }),
        { discard: true },
      )
      return yield* SessionRestart.Service
    }),
  ).pipe(Layer.provide(SessionRestart.layer())),
  deps: [Bus.node, Database.node, SessionStore.node, SessionExecution.node, Job.node, Session.node],
})

// The started events of shell rows still marked running. A shell row's ID is its started event's ID
// with the `msg_` prefix.
const RUNNING_SHELLS = `
  SELECT event.data AS data FROM session_message
  JOIN event ON event.id = 'evt_' || substr(session_message.id, 5)
  WHERE session_message.type = 'shell' AND json_extract(session_message.data, '$.status') = 'running'`

const decodeShellStarted = Schema.decodeUnknownSync(Schema.fromJsonString(SessionEvent.Shell.Started.data))

const isBlock = Schema.is(Schema.Struct({ scriptit: Schema.Struct({ kind: Schema.Literal("block") }) }))

const RESTART_ERROR = {
  type: "server_restart",
  message: "The server restarted. This turn ended; it will not resume automatically.",
}
