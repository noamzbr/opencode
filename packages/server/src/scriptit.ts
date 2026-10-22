export * as ScriptitServer from "./scriptit"

import path from "path"
import { Effect, Layer, Schema, Stream } from "effect"
import { Bus } from "@opencode/core/bus"
import { Config } from "@opencode/core/config"
import { Database } from "@opencode/core/database/database"
import { Environment } from "@opencode/core/environment/index"
import { EnvironmentUnavailable } from "@opencode/core/environment/unavailable"
import { FileSystem } from "@opencode/core/filesystem"
import { FileSystemSearch } from "@opencode/core/filesystem/search"
import { Watcher } from "@opencode/core/filesystem/watcher"
import { InstructionDiscovery } from "@opencode/core/instruction-discovery"
import { Job } from "@opencode/core/job"
import { LocationActivity } from "@opencode/core/location-activity"
import { PersistentPty } from "@opencode/core/persistent-pty"
import { PluginInternal } from "@opencode/core/plugin/internal"
import { Project } from "@opencode/core/project"
import { upsertProject } from "@opencode/core/project/sql"
import { Pty } from "@opencode/core/pty"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { BackgroundNotice } from "@opencode/core/session/background-notice"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionRestart } from "@opencode/core/session/execution/restart"
import { SessionInstructions } from "@opencode/core/session/instructions"
import { SessionStore } from "@opencode/core/session/store"
import { ShellResult } from "@opencode/core/shell/result"
import { WellKnown } from "@opencode/core/wellknown"
import type { TransformInput } from "@opencode/core/database/v1-migration"
import { ServerFetch } from "./fetch"
import { CrossSpawnSpawner } from "@opencode/util/cross-spawn-spawner"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import type { LayerNode } from "@opencode/util/effect/layer-node"
import { FSUtil } from "@opencode/util/fs-util"
import { Hash } from "@opencode/util/hash"
import { ScriptitDispatcher } from "./scriptit-dispatcher"
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
  const unknown = REMOVED.filter((id) => !PluginInternal.ids.has(id))
  if (unknown.length > 0) return yield* Effect.die(new Error(`The removed plugins do not exist: ${unknown.join(", ")}`))
  if (!SCRIPTIT_PLUGINS) return yield* Effect.die(new Error("SCRIPTIT_PLUGINS_ROOT is unset"))
  // The server graph builds in a memo map of its own. The CLI entry has already built AppProcess over the host
  // spawner, and a shared map would hand that instance to this graph past the overrides.
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
    { overrides: overrides(options.configFile) },
  ).pipe(Effect.provideService(Layer.CurrentMemoMap, Layer.makeMemoMapUnsafe()))
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

/**
 * The replacements applied after the server's standard layers. They decide
 * where configuration and instructions come from, and they confine every
 * process and file operation to the execution manager: a Location's
 * Environment is the manager's socket, and nothing else in the process can
 * spawn, open a terminal or serve the host filesystem.
 */
const overrides = (configFile: string | undefined): LayerNode.Replacements => [
  Config.node.replace(Config.configured({ file: configFile, content: INVARIANTS, project: false, global: false })),
  Watcher.node.replace(configFileWatcher(configFile)),
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
  Environment.node.replace(ScriptitDispatcher.environment),
  // Git, hg, formatters, command templates, integration auth commands and the ripgrep download spawn here.
  CrossSpawnSpawner.node.replace(EnvironmentUnavailable.layer),
  FileSystem.node.replace(
    Layer.succeed(
      FileSystem.Service,
      FileSystem.Service.of({
        read: () => unavailable("FileSystem.read"),
        list: () => unavailable("FileSystem.list"),
        find: () => unavailable("FileSystem.find"),
        write: () => unavailable("FileSystem.write"),
      }),
    ),
  ),
  FileSystemSearch.node.replace(
    Layer.succeed(
      FileSystemSearch.Service,
      FileSystemSearch.Service.of({ find: () => unavailable("FileSystemSearch.find") }),
    ),
  ),
  Pty.node.replace(
    Layer.succeed(
      Pty.Service,
      Pty.Service.of({
        list: () => Effect.succeed([]),
        get: (ptyID) => Effect.fail(new Pty.NotFoundError({ ptyID })),
        create: () => unavailable("Pty.create"),
        update: (ptyID) => Effect.fail(new Pty.NotFoundError({ ptyID })),
        remove: (ptyID) => Effect.fail(new Pty.NotFoundError({ ptyID })),
        write: (ptyID) => Effect.fail(new Pty.NotFoundError({ ptyID })),
        attach: (ptyID) => Effect.fail(new Pty.NotFoundError({ ptyID })),
      }),
    ),
  ),
  // The plugin host builds on this service, so it must construct; it never starts the terminal daemon.
  PersistentPty.node.replace(
    Layer.succeed(
      PersistentPty.Service,
      PersistentPty.Service.of({
        list: () => Effect.succeed([]),
        get: (ptyID) => Effect.fail(new PersistentPty.NotFoundError({ ptyID })),
        create: () => Effect.fail(new PersistentPty.UnavailableError({ message: "Terminals are unavailable" })),
        write: (ptyID) => Effect.fail(new PersistentPty.NotFoundError({ ptyID })),
        resize: (ptyID) => Effect.fail(new PersistentPty.NotFoundError({ ptyID })),
        control: (ptyID) => Effect.fail(new PersistentPty.NotFoundError({ ptyID })),
        input: (ptyID) => Effect.fail(new PersistentPty.NotFoundError({ ptyID })),
        snapshot: (ptyID) => Effect.fail(new PersistentPty.NotFoundError({ ptyID })),
        read: () => Effect.succeed(null),
        remove: (ptyID) => Effect.fail(new PersistentPty.NotFoundError({ ptyID })),
        shutdown: () => Effect.void,
        handoff: () => Effect.succeed(null),
        attach: (ptyID) => Effect.fail(new PersistentPty.NotFoundError({ ptyID })),
      }),
    ),
  ),
  Project.node.replace(directoryProjects),
]

/**
 * The native plugins the server removes. A removal of an ID that no plugin has
 * removes nothing, so the server does not start while one names no internal plugin.
 */
const REMOVED = ["opencode.tool.websearch", "opencode.tool.webfetch", "opencode.tools", "opencode.browser"]

/** The bridge names the installed Script.it plugin root, at the same path in the jail. */
const SCRIPTIT_PLUGINS = process.env.SCRIPTIT_PLUGINS_ROOT
const TOOL_OUTPUT = { max_lines: 2_000, max_bytes: 50 * 1024 }

/**
 * The last configuration entry, so it wins per key over the rendered file, and
 * its plugin operations follow the file's. No snapshots and no formatters,
 * which would spawn outside the Environment; no native web search or fetch,
 * which would connect from this process; none of the Code Mode tools that move
 * a session to another directory or drive a browser. The Script.it web fetch
 * and tool-output plugins load after the file's plugins, so the file cannot
 * drop them: one fetches in the caller's execution context, the other spills
 * oversized results there, at the tool-output limits fixed here.
 */
const INVARIANTS = JSON.stringify({
  snapshots: false,
  formatter: false,
  tool_output: TOOL_OUTPUT,
  plugins: [
    ...REMOVED.map((id) => `-${id}`),
    `file://${SCRIPTIT_PLUGINS}/scriptit-webfetch`,
    { package: `file://${SCRIPTIT_PLUGINS}/scriptit-tool-output`, options: TOOL_OUTPUT },
  ],
})

const unavailable = (what: string) => Effect.die(new Error(`${what} is unavailable in the Script.it server`))

/**
 * Every Location is a project of its own directory: `sha1("directory:" + realpath)`,
 * the identity upstream gives a directory without a repository. No walk to a
 * `.git` or `.hg` marker and no read of a cached `opencode` id file, since the
 * workload can plant both, and the project id keys the Location's shell output
 * directory.
 */
const directoryProjects = makeGlobalNode({
  service: Project.Service,
  layer: Layer.effect(
    Project.Service,
    Effect.gen(function* () {
      const projects = yield* Project.Service
      const fs = yield* FSUtil.Service
      const database = yield* Database.Service
      return Project.Service.of({
        ...projects,
        resolve: Effect.fn("Project.resolve")(function* (input) {
          const directory = AbsolutePath.make(yield* fs.resolve(input))
          const id = Project.ID.make(Hash.fast(`directory:${directory}`))
          yield* upsertProject(database.db, { id, canonical: directory }).pipe(Effect.orDie)
          return { id, directory, canonical: directory }
        }),
      })
    }),
  ),
  // A copy of the stock node: depending on Project.node itself would resolve to this replacement.
  deps: [Project.node.mapLayer((layer) => layer), FSUtil.node, Database.node],
})

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
 * reader, so its execution fails and releases the claim instead. A block
 * tree's background job still marked running died with that server: its
 * marker ends as a quiet cancellation notice, so stock recovery never runs
 * the block's subagent child again.
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
          if (!BackgroundNotice.isBlock((yield* store.get(sessionID))?.metadata)) return
          yield* bus.publish(
            SessionEvent.Execution.Failed,
            { sessionID, error: RESTART_ERROR },
            { commit: () => store.release(sessionID) },
          )
        }),
        { discard: true },
      )
      const jobs = yield* Job.Service
      const sessions = yield* Session.Service
      yield* Effect.forEach(
        (yield* jobs.pendingBackground).filter((background) => background.status === "running"),
        Effect.fnUntraced(function* (background) {
          const owner =
            background.recovery.kind === "shell" ? background.recovery.sessionID : background.recovery.parentSessionID
          if (!BackgroundNotice.isBlock((yield* store.get(owner))?.metadata)) return
          yield* BackgroundNotice.deliver(sessions, jobs, { ...background, resume: false }).pipe(Effect.orDie)
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

const RESTART_ERROR = {
  type: "server_restart",
  message: "The server restarted. This turn ended; it will not resume automatically.",
}
