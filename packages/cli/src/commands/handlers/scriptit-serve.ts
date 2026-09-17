import { Effect, Layer } from "effect"
import { Config } from "@opencode/core/config"
import { InstructionDiscovery } from "@opencode/core/instruction-discovery"
import { LocationActivity } from "@opencode/core/location-activity"
import { SessionInstructions } from "@opencode/core/session/instructions"
import { WellKnown } from "@opencode/core/wellknown"
import type { TransformInput } from "@opencode/core/database/v1-migration"
import { ServerFetch } from "@opencode/server/fetch"
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
 * inside agent-writable state and stay disabled. The overrides below are
 * applied after the server's own standard layers, so they are what decides
 * where configuration and instructions come from.
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
