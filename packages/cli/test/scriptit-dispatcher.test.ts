import { expect, test } from "bun:test"
import { Environment } from "@opencode/core/environment/index"
import { Location } from "@opencode/core/location"
import { AbsolutePath } from "@opencode/core/schema"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Effect, Fiber, Layer, Stream } from "effect"
import type { Scope } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { location } from "../../core/test/fixture/location"
import { ScriptitDispatcher } from "../src/commands/handlers/scriptit-dispatcher"
import { fakeManager, Frame } from "./fixture/execution-manager"

const LOCATION = "/scriptit/session"
const MiB = 1024 * 1024

test("a spawn streams its output and settles with the manager's exit", async () => {
  await using manager = await fakeManager(async (peer) => {
    peer.reply({ event: "started", pid: 42 })
    peer.send(Frame.stdout, "out")
    // The manager pings a client it is not reading; the client ignores the ping.
    peer.send(Frame.ping)
    peer.send(Frame.stderr, "err")
    peer.send(Frame.stdout, "put")
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

test("kill settles past output nobody reads, and a reader that keeps taking loses nothing", async () => {
  const received: unknown[] = []
  await using manager = await fakeManager(async (peer) => {
    peer.reply({ event: "started", pid: 7 })
    // More stdout than the client holds; nobody reads it.
    for (const text of ["1", "2", "3"]) void peer.send(Frame.stdout, text)
    const frame = await peer.next()
    received.push(frame && JSON.parse(frame.payload.toString()))
    for (const text of ["a", "b", "c"]) void peer.send(Frame.stderr, text)
    peer.reply({ event: "exit", code: null, signal: "SIGINT" })
    peer.end()
  })
  const result = await run(manager.socket, (environment) =>
    Effect.gen(function* () {
      const handle = yield* environment.spawner.spawn(ChildProcess.make("sleep", ["60"]))
      // A slow but steady stderr reader.
      const stderr = yield* Effect.forkChild(
        Stream.mkString(Stream.decodeText(Stream.tap(handle.stderr, () => Effect.sleep("50 millis")))),
      )
      yield* Effect.sleep("100 millis")
      yield* handle.kill({ killSignal: "SIGINT" })
      expect(yield* handle.isRunning).toBe(false)
      return { stderr: yield* Fiber.join(stderr), message: (yield* Effect.flip(handle.exitCode)).message }
    }),
  )
  expect(received).toEqual([{ op: "kill", signal: "SIGINT" }])
  expect(result.stderr).toBe("abc")
  expect(result.message).toContain("Process interrupted due to receipt of signal: 'SIGINT'")
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

test("a slow reader holds the job's output back instead of buffering it", async () => {
  const total = 32 * MiB
  let sent = 0
  await using manager = await fakeManager(async (peer) => {
    peer.reply({ event: "started", pid: 3 })
    for (; sent < total; sent += 64 * 1024) await peer.send(Frame.stdout, new Uint8Array(64 * 1024))
    peer.reply({ event: "exit", code: 0, signal: null })
    peer.end()
  })
  const result = await run(manager.socket, (environment) =>
    Effect.gen(function* () {
      const handle = yield* environment.spawner.spawn(ChildProcess.make("yes", []))
      yield* Effect.sleep("300 millis")
      const stalled = sent
      const received = yield* Stream.runFold(
        handle.stdout,
        () => 0,
        (bytes, chunk) => bytes + chunk.length,
      )
      return { stalled, received, code: Number(yield* handle.exitCode) }
    }),
  )
  // The client holds about a frame's worth; the rest waits in the manager.
  expect(result.stalled).toBeLessThan(4 * MiB)
  expect(result).toMatchObject({ received: total, code: 0 })
})

test("stdin waits while the manager does not read, then streams in frames of at most 1 MiB and ends", async () => {
  const sizes: number[] = []
  const reading = Promise.withResolvers<void>()
  let ended = false
  await using manager = await fakeManager(async (peer) => {
    peer.pause()
    peer.reply({ event: "started", pid: 11 })
    // While it does not read the client, the manager pings it.
    await peer.send(Frame.ping)
    await reading.promise
    await peer.send(Frame.ping)
    peer.resume()
    for (let frame = await peer.next(); frame; frame = await peer.next()) {
      if (frame.type === Frame.stdin) sizes.push(frame.payload.length)
      if (frame.type !== Frame.stdinEnd) continue
      ended = true
      peer.reply({ event: "exit", code: 0, signal: null })
      peer.end()
      return
    }
  })
  const result = await run(manager.socket, (environment) =>
    Effect.gen(function* () {
      const handle = yield* environment.spawner.spawn(ChildProcess.make("cat", []))
      let written = false
      const writing = yield* Effect.forkChild(
        Stream.run(Stream.make(new Uint8Array(1.5 * MiB), new Uint8Array(3)), handle.stdin).pipe(
          Effect.ensuring(Effect.sync(() => (written = true))),
        ),
      )
      yield* Effect.sleep("300 millis")
      const stalled = !written
      reading.resolve()
      yield* Fiber.join(writing)
      return { stalled, code: Number(yield* handle.exitCode) }
    }),
  )
  expect(result).toEqual({ stalled: true, code: 0 })
  expect(sizes).toEqual([MiB, 0.5 * MiB, 3])
  expect(ended).toBe(true)
})

test("a JSON request must fit one frame", async () => {
  await using manager = await fakeManager((peer) => {
    peer.reply({ event: "error", code: "REFUSED", message: "probe" })
    peer.end()
  })
  const messages = await run(manager.socket, (environment) =>
    Effect.gen(function* () {
      const spawn = (padding: number) =>
        environment.spawner.spawn(ChildProcess.make("job", [], { env: { PAD: "x".repeat(padding) } })).pipe(
          Effect.flip,
          Effect.map((error) => error.message),
        )
      yield* spawn(0)
      const limit = MiB - manager.sizes[0]
      return [yield* spawn(limit), yield* spawn(limit + 1)]
    }),
  )
  expect(messages[0]).toContain("REFUSED: probe")
  expect(manager.sizes).toEqual([manager.sizes[0], MiB])
  expect(messages[1]).toContain(`a ${MiB + 1}-byte JSON message exceeds the ${MiB}-byte frame limit`)
})

test("manager errors become typed process and file errors", async () => {
  const codes = ["REFUSED", "CONTAINMENT_UNAVAILABLE", "CONTAINER_LIMIT", "SPAWN_FAILED", "CLOSED"]
  await using manager = await fakeManager(async (peer) => {
    if (peer.request.op === "file") {
      const target = peer.request.path
      if (target === `${LOCATION}/missing`) return peer.reply({ event: "error", code: "NotFound", path: target })
      if (target === `${LOCATION}/directory`)
        return peer.reply({ event: "error", code: "WrongKind", path: target, actual: "directory" })
      return peer.reply({ event: "error", code: "REFUSED", message: "no owner" })
    }
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
    if (args[0] === "stdin") {
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
        // Once the manager has gone, stdin fails instead of dropping the bytes.
        stdin: yield* environment.spawner.spawn(ChildProcess.make("job", ["stdin"])).pipe(
          Effect.tap((handle) => Effect.ignore(handle.exitCode)),
          Effect.flatMap((handle) => Effect.flip(Stream.run(Stream.make(new Uint8Array(3)), handle.stdin))),
          Effect.map((error) => error.message),
        ),
        piped: yield* environment.spawner
          .spawn(ChildProcess.make("a", []).pipe(ChildProcess.pipeTo(ChildProcess.make("b", []))))
          .pipe(
            Effect.flip,
            Effect.map((error) => error.message),
          ),
        file: yield* Effect.flip(environment.files.stat(`${LOCATION}/file`)),
        missing: yield* Effect.flip(environment.files.read(`${LOCATION}/missing`)),
        directory: yield* Effect.flip(environment.files.read(`${LOCATION}/directory`)),
      }
    }),
  )
  codes.forEach((code, index) => expect(failures.spawn[index]).toContain(`${code}: because ${code}`))
  expect(failures.settles).toContain("CLOSED: context closed")
  expect(failures.drops).toContain("execution manager closed the connection")
  expect(failures.stdin).toContain("ChildProcess.stdin")
  expect(failures.stdin).toContain("connection is closed")
  expect(failures.piped).toContain("REFUSED: piped commands are not supported")
  expect(failures.file).toBeInstanceOf(Environment.Failed)
  expect(String((failures.file as Environment.Failed).cause)).toContain("REFUSED: no owner")
  expect(failures.missing).toBeInstanceOf(Environment.NotFound)
  expect(failures.missing).toMatchObject({ path: `${LOCATION}/missing` })
  expect(failures.directory).toBeInstanceOf(Environment.WrongKind)
  expect(failures.directory).toMatchObject({ path: `${LOCATION}/directory`, actual: "directory" })
  // The piped command never reached the manager.
  expect(manager.requests.filter((request) => request.op === "spawn")).toHaveLength(codes.length + 3)
})

test("a file read sends its range and reassembles a body that the manager sends in several frames", async () => {
  await using manager = await fakeManager(async (peer) => {
    peer.reply({ event: "ok", info: { type: "file", size: 9, mtimeMs: 1 }, bytes: 6 })
    await peer.send(Frame.body, "abc")
    await peer.send(Frame.body, "def")
    peer.end()
  })
  const read = await run(manager.socket, (environment) =>
    environment.files.read(`${LOCATION}/file`, { offset: 2, length: 6 }),
  )
  expect(new TextDecoder().decode(read.bytes)).toBe("abcdef")
  expect(read.info).toEqual({ type: "file", size: 9, mtimeMs: 1 })
  expect(manager.requests).toEqual([
    { v: 1, op: "file", location: LOCATION, action: "read", path: `${LOCATION}/file`, range: { offset: 2, length: 6 } },
  ])
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

/** Builds the Location's Environment as `scriptit-serve` does, with `SCRIPTIT_EXEC_SOCKET` set to `socket` or unset. */
async function run<A, E>(
  socket: string | undefined,
  body: (environment: Environment.Interface) => Effect.Effect<A, E, Scope.Scope>,
) {
  const saved = process.env.SCRIPTIT_EXEC_SOCKET
  setSocket(socket)
  try {
    return await Effect.runPromise(
      Effect.scoped(Effect.flatMap(Environment.Service, body)).pipe(
        Effect.provide(
          LayerNode.compile(ScriptitDispatcher.environment, {
            replacements: [
              Location.node.replace(
                Layer.succeed(
                  Location.Service,
                  Location.Service.of(location({ directory: AbsolutePath.make(LOCATION) })),
                ),
              ),
            ],
          }),
        ),
      ),
    )
  } finally {
    setSocket(saved)
  }
}

function setSocket(socket: string | undefined) {
  if (socket === undefined) delete process.env.SCRIPTIT_EXEC_SOCKET
  else process.env.SCRIPTIT_EXEC_SOCKET = socket
}
