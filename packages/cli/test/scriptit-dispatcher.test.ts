import { expect, test } from "bun:test"
import fs from "node:fs/promises"
import path from "node:path"
import { Environment } from "@opencode/core/environment/index"
import { EnvironmentUnavailable } from "@opencode/core/environment/unavailable"
import { Location } from "@opencode/core/location"
import { AbsolutePath } from "@opencode/core/schema"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { AppProcess } from "@opencode/util/process"
import { Effect, Layer, Stream } from "effect"
import type { Scope } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { environmentConformance } from "../../core/test/lib/environment-conformance"
import { location } from "../../core/test/fixture/location"
import { ScriptitDispatcher } from "../src/commands/handlers/scriptit-dispatcher"
import { overrides } from "../src/commands/handlers/scriptit-serve"
import { fakeManager, type Peer, tmpdir } from "./fixture/execution-manager"

const LOCATION = "/scriptit/session"

test("a spawn streams its output and settles with the manager's exit", async () => {
  await using manager = await fakeManager(async (peer) => {
    peer.reply({ event: "started", pid: 42 })
    peer.send(ScriptitDispatcher.Frame.stdout, "out")
    peer.send(ScriptitDispatcher.Frame.stderr, "err")
    peer.send(ScriptitDispatcher.Frame.stdout, "put")
    peer.reply({ event: "exit", code: 3, signal: null })
    peer.end()
  })
  const result = await run(manager.socket, (environment) =>
    Effect.gen(function* () {
      const handle = yield* environment.spawner.spawn(
        ChildProcess.make("/bin/sh", ["-c", "work"], {
          cwd: "sub",
          env: { KEPT: "1", DROPPED: undefined },
          extendEnv: true,
          stdin: "ignore",
          forceKillAfter: "2 seconds",
        }),
      )
      const [stdout, stderr, code] = yield* Effect.all(
        [
          Stream.mkString(Stream.decodeText(handle.stdout)),
          Stream.mkString(Stream.decodeText(handle.stderr)),
          handle.exitCode,
        ],
        { concurrency: "unbounded" },
      )
      return { pid: Number(handle.pid), stdout, stderr, code: Number(code), running: yield* handle.isRunning }
    }),
  )
  expect(result).toEqual({ pid: 42, stdout: "output", stderr: "err", code: 3, running: false })
  expect(manager.requests).toEqual([
    {
      v: 1,
      op: "spawn",
      location: LOCATION,
      command: "/bin/sh",
      args: ["-c", "work"],
      cwd: `${LOCATION}/sub`,
      env: { KEPT: "1" },
      shell: false,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      killSignal: "SIGTERM",
      forceKillAfterMs: 2000,
      additionalFds: 0,
      detached: false,
    },
  ])
})

test("kill sends the signal and waits for the job to settle", async () => {
  const received: unknown[] = []
  await using manager = await fakeManager(async (peer) => {
    peer.reply({ event: "started", pid: 7 })
    const frame = await peer.next()
    received.push(frame && JSON.parse(frame.payload.toString()))
    peer.reply({ event: "exit", code: null, signal: "SIGINT" })
    peer.end()
  })
  const message = await run(manager.socket, (environment) =>
    Effect.gen(function* () {
      const handle = yield* environment.spawner.spawn(ChildProcess.make("sleep", ["60"]))
      yield* handle.kill({ killSignal: "SIGINT" })
      expect(yield* handle.isRunning).toBe(false)
      return (yield* Effect.flip(handle.exitCode)).message
    }),
  )
  expect(received).toEqual([{ op: "kill", signal: "SIGINT" }])
  expect(message).toContain("Process interrupted due to receipt of signal: 'SIGINT'")
})

test("closing the spawn's scope before the exit closes the connection", async () => {
  const seen: Array<string> = []
  const closed = Promise.withResolvers<void>()
  await using manager = await fakeManager(async (peer) => {
    peer.reply({ event: "started", pid: 9 })
    const frame = await peer.next()
    seen.push(frame === undefined ? "closed" : `frame ${frame.type}`)
    closed.resolve()
  })
  await run(manager.socket, (environment) =>
    Effect.scoped(environment.spawner.spawn(ChildProcess.make("sleep", ["60"]))).pipe(Effect.asVoid),
  )
  await closed.promise
  expect(seen).toEqual(["closed"])
})

test("stdin streams in frames of at most 1 MiB and then ends", async () => {
  const sizes: number[] = []
  let ended = false
  await using manager = await fakeManager(async (peer) => {
    peer.reply({ event: "started", pid: 11 })
    for (let frame = await peer.next(); frame; frame = await peer.next()) {
      if (frame.type === ScriptitDispatcher.Frame.stdin) sizes.push(frame.payload.length)
      if (frame.type !== ScriptitDispatcher.Frame.stdinEnd) continue
      ended = true
      peer.reply({ event: "exit", code: 0, signal: null })
      peer.end()
      return
    }
  })
  const code = await run(manager.socket, (environment) =>
    Effect.gen(function* () {
      const handle = yield* environment.spawner.spawn(
        ChildProcess.make("cat", [], { stdin: Stream.make(new Uint8Array(1.5 * 1024 * 1024), new Uint8Array(3)) }),
      )
      return Number(yield* handle.exitCode)
    }),
  )
  expect(code).toBe(0)
  expect(sizes).toEqual([1024 * 1024, 0.5 * 1024 * 1024, 3])
  expect(ended).toBe(true)
})

test("manager errors become typed process and file errors", async () => {
  const codes = ["REFUSED", "CONTAINMENT_UNAVAILABLE", "CONTAINER_LIMIT", "SPAWN_FAILED", "CLOSED"]
  await using manager = await fakeManager(async (peer) => {
    if (peer.request.op === "file") return peer.reply({ event: "error", code: "REFUSED", message: "no owner" })
    const args = peer.request.args as string[]
    if (args[0] === "settles") {
      peer.reply({ event: "started", pid: 1 })
      peer.reply({ event: "error", code: "CLOSED", message: "context closed" })
      return peer.end()
    }
    if (args[0] === "drops") {
      peer.reply({ event: "started", pid: 1 })
      return peer.end()
    }
    peer.reply({ event: "error", code: args[0], message: `because ${args[0]}` })
    peer.end()
  })
  const failures = await run(manager.socket, (environment) =>
    Effect.gen(function* () {
      const spawn = (argument: string) =>
        environment.spawner.spawn(ChildProcess.make("job", [argument])).pipe(
          Effect.flip,
          Effect.map((error) => error.message),
        )
      const exit = (argument: string) =>
        environment.spawner.spawn(ChildProcess.make("job", [argument])).pipe(
          Effect.flatMap((handle) => Effect.flip(handle.exitCode)),
          Effect.map((error) => error.message),
        )
      return {
        spawn: yield* Effect.forEach(codes, spawn),
        settles: yield* exit("settles"),
        drops: yield* exit("drops"),
        piped: yield* environment.spawner
          .spawn(ChildProcess.make("a", []).pipe(ChildProcess.pipeTo(ChildProcess.make("b", []))))
          .pipe(
            Effect.flip,
            Effect.map((error) => error.message),
          ),
        file: yield* Effect.flip(environment.files.stat(`${LOCATION}/file`)),
      }
    }),
  )
  codes.forEach((code, index) => expect(failures.spawn[index]).toContain(`${code}: because ${code}`))
  expect(failures.settles).toContain("CLOSED: context closed")
  expect(failures.drops).toContain("execution manager closed the connection")
  expect(failures.piped).toContain("REFUSED: piped commands are not supported")
  expect(failures.file).toBeInstanceOf(Environment.Failed)
  expect(String((failures.file as Environment.Failed).cause)).toContain("REFUSED: no owner")
  // The piped command never reached the manager.
  expect(manager.requests.filter((request) => request.op === "spawn")).toHaveLength(codes.length + 2)
})

test("without SCRIPTIT_EXEC_SOCKET every process and file operation fails", async () => {
  const failures = await run(undefined, (environment) =>
    Effect.all([
      environment.spawner.spawn(ChildProcess.make("true", [])).pipe(Effect.flip),
      environment.files.read(`${LOCATION}/file`).pipe(Effect.flip),
    ]),
  )
  expect(failures[0].message).toContain("SCRIPTIT_EXEC_SOCKET is unset")
  expect(failures[1]).toBeInstanceOf(Environment.Failed)
})

test("a spawn outside a Location's Environment fails without running", async () => {
  await using tmp = await tmpdir()
  const marker = path.join(tmp.path, "ran")
  const error = await Effect.runPromise(
    Effect.gen(function* () {
      const processes = yield* AppProcess.Service
      return yield* Effect.flip(processes.run(ChildProcess.make("touch", [marker])))
    }).pipe(Effect.provide(AppNodeBuilder.build(AppProcess.node, overrides()))),
  )
  expect(error.message).toContain("no execution plane")
  expect(await fs.exists(marker)).toBe(false)
})

// The manager's file semantics are `local.ts`; this fake answers with that driver, so the suite checks the client.
environmentConformance("execution manager environment", () =>
  Effect.gen(function* () {
    const tmp = yield* Effect.promise(() => tmpdir())
    const local = Environment.makeFiles(Environment.makeLocalDriver(EnvironmentUnavailable.spawner))
    const manager = yield* Effect.promise(() =>
      fakeManager<FileRequest>(async (peer) => {
        const request = peer.request
        const body = request.action === "write" ? await readBody(peer, request.bytes) : new Uint8Array()
        const outcome = await Effect.runPromise(
          Effect.match(fileOperation(local, request, body), {
            onFailure: (error) => ({ reply: contractError(error), body: new Uint8Array() }),
            onSuccess: (value) => value,
          }),
        )
        peer.reply(outcome.reply)
        // Split the body so the client reassembles it.
        peer.send(ScriptitDispatcher.Frame.body, outcome.body.subarray(0, 3))
        if (outcome.body.length > 3) peer.send(ScriptitDispatcher.Frame.body, outcome.body.subarray(3))
        peer.end()
      }),
    )
    const environment = yield* Environment.Service.pipe(Effect.provide(layer(manager.socket)))
    return {
      files: environment.files,
      root: tmp.path,
      symlink: (target: string, link: string) =>
        Effect.tryPromise({
          try: () => fs.symlink(target, link),
          catch: (cause) => new Environment.Failed({ path: link, cause }),
        }),
      dispose: Effect.promise(async () => {
        await manager[Symbol.asyncDispose]()
        await tmp[Symbol.asyncDispose]()
      }),
    }
  }),
)

interface FileRequest {
  readonly action: string
  readonly path: string
  readonly from: string
  readonly to: string
  readonly bytes: number
  readonly range?: { readonly offset: number; readonly length: number }
}

function fileOperation(
  local: Environment.Files,
  request: FileRequest,
  body: Uint8Array,
): Effect.Effect<
  { readonly reply: object; readonly body: Uint8Array },
  Environment.NotFound | Environment.WrongKind | Environment.Failed
> {
  const ok = (fields: object = {}) => ({ reply: { event: "ok", ...fields }, body: new Uint8Array() })
  if (request.action === "read")
    return local
      .read(request.path, request.range)
      .pipe(
        Effect.map((read) => ({ reply: { event: "ok", info: read.info, bytes: read.bytes.length }, body: read.bytes })),
      )
  if (request.action === "write") return local.write(request.path, body).pipe(Effect.as(ok()))
  if (request.action === "stat") return local.stat(request.path).pipe(Effect.map((info) => ok({ info })))
  if (request.action === "list") return local.list(request.path).pipe(Effect.map((entries) => ok({ entries })))
  if (request.action === "remove") return local.remove(request.path).pipe(Effect.as(ok()))
  if (request.action === "move") return local.move(request.from, request.to).pipe(Effect.as(ok()))
  return local.mkdir(request.path).pipe(Effect.as(ok()))
}

function contractError(error: Environment.NotFound | Environment.WrongKind | Environment.Failed) {
  if (error._tag === "Environment.NotFound") return { event: "error", code: "NotFound", path: error.path }
  if (error._tag === "Environment.WrongKind")
    return { event: "error", code: "WrongKind", path: error.path, actual: error.actual }
  return { event: "error", code: "Failed", path: error.path, message: String(error.cause) }
}

async function readBody(peer: Peer<FileRequest>, size: number) {
  const chunks: Uint8Array[] = []
  for (let received = 0; received < size; ) {
    const frame = await peer.next()
    if (!frame) break
    chunks.push(frame.payload)
    received += frame.payload.length
  }
  return Buffer.concat(chunks)
}

function layer(socket: string | undefined) {
  return LayerNode.compile(ScriptitDispatcher.environment, {
    replacements: [
      ScriptitDispatcher.node.replace(Layer.succeed(ScriptitDispatcher.Service, ScriptitDispatcher.make(socket))),
      Location.node.replace(
        Layer.succeed(Location.Service, Location.Service.of(location({ directory: AbsolutePath.make(LOCATION) }))),
      ),
    ],
  })
}

function run<A, E>(
  socket: string | undefined,
  body: (environment: Environment.Interface) => Effect.Effect<A, E, Scope.Scope>,
) {
  return Effect.runPromise(Effect.scoped(Effect.flatMap(Environment.Service, body)).pipe(Effect.provide(layer(socket))))
}
