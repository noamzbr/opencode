import { Effect } from "effect"
import { ScriptitServer } from "@opencode/server/scriptit"
import { Commands } from "../commands"
import { Runtime } from "../../framework/runtime"
import { OPENCODE_ARTIFACT, OPENCODE_CHANNEL, OPENCODE_VERSION } from "../../version"

/** The bridge owns this loopback listener through its password line and stdin lifetime. */
export default Runtime.handler(
  Commands.commands["scriptit-serve"],
  Effect.fnUntraced(function* () {
    const password = yield* readStartupPassword()
    const handler = yield* ScriptitServer.create({
      app: {
        name: process.env.OPENCODE_CLIENT ?? OPENCODE_ARTIFACT,
        version: OPENCODE_VERSION,
        channel: OPENCODE_CHANNEL,
      },
      password,
      databasePath: process.env.OPENCODE_DB ?? "opencode.db",
      configFile: process.env.OPENCODE_CONFIG,
      modelsFile: process.env.OPENCODE_MODELS_PATH,
    })
    const listener = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      fetch: (request) => handler(request),
    })
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
