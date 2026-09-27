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
import { ServerFetch } from "@opencode/server/fetch"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Commands } from "../commands"
import { Runtime } from "../../framework/runtime"
import { OPENCODE_ARTIFACT, OPENCODE_CHANNEL, OPENCODE_VERSION } from "../../version"

/**
 * Embedded server for the Script.it bridge: one loopback listener owned by the
 * parent process through stdin. The bridge writes the per-boot password as the
 * first stdin line, reads the `{url}` line, authenticates with that password
 * and closes stdin to shut the server down. The password never enters the
 * process environment, which `/proc/<pid>/environ` would keep readable.
 *
 * The native Location idle sweeper is replaced by an inert service: the bridge
 * decides when a Location is unused and evicts it explicitly, so a quiet
 * long-running shell is never interrupted by an independent timer.
 *
 * Configuration comes only from the explicit file in OPENCODE_CONFIG: the
 * global config directory, `~/.claude`, `~/.agents`, AGENTS.md discovery, the
 * project walk-up and the well-known origins stored in the database all sit
 * inside agent-writable state and stay disabled. That file is the only path the
 * server watches, so rewriting it reloads the configuration in place. The
 * overrides below are applied after the server's own standard layers, so they
 * are what decides where configuration and instructions come from.
 *
 * At boot, before claimed Sessions resume, agent-block Sessions whose turn was
 * cut are failed instead, and user-shell rows still marked running end as
 * unavailable: the new server cannot observe their commands, which may still
 * be running.
 */
export default Runtime.handler(
  Commands.commands["scriptit-serve"],
  Effect.fnUntraced(function* () {
    const password = yield* readStartupPassword()
    const handler = yield* ServerFetch.make(
      {
        app: { name: process.env.OPENCODE_CLIENT ?? OPENCODE_ARTIFACT, version: OPENCODE_VERSION, channel: OPENCODE_CHANNEL },
        password,
        // Durable rows are what the bridge replays from after a feed interruption.
        events: { persist: true },
        database: { path: process.env.OPENCODE_DB ?? "opencode.db" },
        models: { file: process.env.OPENCODE_MODELS_PATH, fetch: false },
        fs: { filewatcher: false, fff: false },
      },
      {
        overrides: [
          Config.node.replace(Config.configured({ file: process.env.OPENCODE_CONFIG, project: false, global: false })),
          Watcher.node.replace(configFileWatcher(process.env.OPENCODE_CONFIG)),
          InstructionDiscovery.node.replace(InstructionDiscovery.configured({ project: false, global: false })),
          LocationActivity.node.replace(Layer.succeed(LocationActivity.Service, LocationActivity.Service.of({}))),
          WellKnown.node.replace(
            Layer.succeed(
              WellKnown.Service,
              WellKnown.Service.of({
                entries: () => Effect.succeed([]),
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
    const listener = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      // SSE subscriptions stay open for the life of the bridge.
      idleTimeout: 0,
      // Bun passes its server as the second argument; the handler's second argument is an Effect context.
      fetch: async (request) => {
        if (new URL(request.url).pathname !== "/scriptit/internal/v1-transform") return handler(request)
        if (request.method !== "POST") return new Response(null, { status: 405 })
        if (request.headers.get("authorization") !== `Basic ${btoa(`opencode:${password}`)}`)
          return new Response(null, { status: 401 })
        const input = await request.json().catch(() => null)
        if (
          typeof input !== "object" ||
          input === null ||
          !("session" in input) ||
          !Array.isArray("messages" in input ? input.messages : null) ||
          !Array.isArray("parts" in input ? input.parts : null)
        ) return new Response(null, { status: 400 })
        const { V1Migration } = await import("@opencode/core/database/v1-migration")
        return Response.json(V1Migration.transformSession(input as TransformInput))
      },
    })
    // Close connections, including SSE, before the application layer releases.
    yield* Effect.addFinalizer(() => Effect.sync(() => listener.stop(true)))
    console.log(JSON.stringify({ url: `http://127.0.0.1:${listener.port}` }))
    yield* waitForStdinClose()
  }),
)

/** The native watcher for one file's entry in its directory; every other watch stays disabled. */
function configFileWatcher(file: string | undefined) {
  const target = file === undefined ? undefined : path.resolve(file)
  return makeGlobalNode({
    service: Watcher.Service,
    layer: Layer.effect(
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
    ).pipe(Layer.provide(Watcher.layer())),
    deps: [Watcher.nativeNode],
  })
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

/** The first non-empty newline-delimited stdin value; stdin stays open as the lifetime signal. */
function readStartupPassword() {
  return Effect.callback<string, Error>((resume) => {
    let buffer = ""
    const detach = () => {
      process.stdin.off("data", onData)
      process.stdin.off("end", onEnd)
    }
    const onData = (chunk: Buffer | string) => {
      buffer += chunk.toString()
      const newline = buffer.indexOf("\n")
      if (newline < 0) return
      detach()
      const line = buffer.slice(0, newline).trim()
      resume(line ? Effect.succeed(line) : Effect.fail(new Error("Missing startup password on stdin")))
    }
    const onEnd = () => {
      detach()
      resume(Effect.fail(new Error("Stdin closed before the startup password")))
    }
    process.stdin.on("data", onData)
    process.stdin.once("end", onEnd)
    return Effect.sync(detach)
  })
}

function waitForStdinClose() {
  return Effect.callback<void>((resume) => {
    const close = () => resume(Effect.void)
    process.stdin.once("end", close)
    process.stdin.once("close", close)
    process.stdin.resume()
    if (process.stdin.readableEnded || process.stdin.destroyed) close()
    return Effect.sync(() => {
      process.stdin.off("end", close)
      process.stdin.off("close", close)
      process.stdin.pause()
    })
  })
}
