export * as ScriptitDispatcher from "./scriptit-dispatcher"

import net from "node:net"
import path from "node:path"
import {
  Cause,
  Context,
  Deferred,
  Duration,
  Effect,
  Layer,
  Option,
  PlatformError,
  Queue,
  Schema,
  Sink,
  Stream,
} from "effect"
import type { Scope } from "effect"
import { type ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { Environment } from "@opencode/core/environment/index"
import { EnvironmentFiles } from "@opencode/core/environment/files"
import { Location } from "@opencode/core/location"
import { makeGlobalNode, makeLocationNode } from "@opencode/util/effect/app-node"

/**
 * The client of the Script.it execution manager, a unix socket served by the
 * bridge at `SCRIPTIT_EXEC_SOCKET`. Each request is one connection. A frame is
 * a big-endian u32 length of the rest, a u8 type, then at most 1 MiB of
 * payload. The socket directory admits only the harness uid, so the caller's
 * uid is its identity and the protocol carries no secret.
 *
 * Without `SCRIPTIT_EXEC_SOCKET` the server still boots, and every process and
 * file operation of a Location fails: nothing falls back to the harness.
 */

export const Frame = { json: 1, stdout: 2, stderr: 3, stdin: 4, stdinEnd: 5, body: 6 } as const

const MAX_PAYLOAD = 1024 * 1024

export class Closed extends Schema.TaggedError<Closed>()("ScriptitDispatcher.Closed", {
  message: Schema.String,
}) {}

export interface Connection {
  /** The next frame from the manager; fails with `Closed` once the manager's frames are exhausted. */
  readonly next: Effect.Effect<{ readonly type: number; readonly payload: Uint8Array }, Closed>
  /** Writes `bytes` as frames of `type`, split at the payload limit; empty bytes write one empty frame. */
  readonly send: (type: number, bytes?: Uint8Array) => void
}

export interface Interface {
  /** Connects and sends `request` as the first frame. Closing the scope closes the connection. */
  readonly open: (request: object) => Effect.Effect<Connection, Closed, Scope.Scope>
}

export class Service extends Context.Service<Service, Interface>()("@scriptit/ExecutionDispatcher") {}

export const make = (socket: string | undefined) =>
  Service.of({
    open: (request) =>
      socket === undefined
        ? Effect.fail(new Closed({ message: "SCRIPTIT_EXEC_SOCKET is unset; the execution manager is unavailable" }))
        : Effect.acquireRelease(connect(socket, request), (connection) =>
            Effect.sync(() => connection.socket.destroy()),
          ),
  })

export const node = makeGlobalNode({
  service: Service,
  layer: Layer.sync(Service, () => make(process.env.SCRIPTIT_EXEC_SOCKET)),
  deps: [],
})

/**
 * The Location's Environment: every spawn and every file operation is a request
 * to the execution manager, which runs it in the context that owns this
 * Location directory. The manager resolves the owner, the uid, the mounts and
 * the environment; the request names only the Location.
 */
export const environment = makeLocationNode({
  service: Environment.Service,
  layer: Layer.effect(
    Environment.Service,
    Effect.gen(function* () {
      const location = yield* Location.Service
      const dispatcher = yield* Service
      const spawner = ChildProcessSpawner.make((command) => spawn(dispatcher, location.directory, command))
      return Environment.Service.of({
        spawner,
        files: Environment.makeFiles({ spawner, overrides: files(dispatcher, location.directory) }),
      })
    }),
  ),
  deps: [Location.node, node],
})

const spawn = Effect.fnUntraced(function* (dispatcher: Interface, location: string, command: ChildProcess.Command) {
  if (command._tag !== "StandardCommand")
    return yield* processError(command, "spawn", "REFUSED: piped commands are not supported")
  const options = command.options
  const stdin = stdinConfig(options.stdin)
  const stdout = outputConfig(options.stdout)
  const stderr = outputConfig(options.stderr)
  const fail = (closed: Closed) => processError(command, "spawn", closed.message)
  const connection = yield* dispatcher
    .open({
      v: 1,
      op: "spawn",
      location,
      command: command.command,
      args: command.args,
      cwd: path.resolve(location, options.cwd ?? "."),
      // The manager builds the job's environment from its allowlist; OpenCode's own environment is never sent.
      env: options.env ?? {},
      shell: options.shell ?? false,
      stdin: mode(stdin.stream),
      stdout: mode(stdout),
      stderr: mode(stderr),
      killSignal: options.killSignal ?? "SIGTERM",
      forceKillAfterMs: options.forceKillAfter === undefined ? undefined : Duration.toMillis(options.forceKillAfter),
      additionalFds: Object.keys(options.additionalFds ?? {}).length,
      detached: options.detached ?? false,
    })
    .pipe(Effect.mapError(fail))
  const started = yield* reply(connection, Started).pipe(Effect.mapError(fail))
  if (started.event === "error") return yield* processError(command, "spawn", describe(started))

  const stdoutQueue = yield* Queue.unbounded<Uint8Array, Cause.Done>()
  const stderrQueue = yield* Queue.unbounded<Uint8Array, Cause.Done>()
  const exit = yield* Deferred.make<Settled, PlatformError.PlatformError>()
  // ponytail: output frames queue without read backpressure, so a consumer slower than the job buffers its output
  // in memory. Upgrade: pause the socket above a queue high-water mark and resume it as the streams drain.
  yield* Effect.gen(function* () {
    for (;;) {
      const frame = yield* connection.next
      if (frame.type === Frame.stdout) Queue.offerUnsafe(stdoutQueue, frame.payload)
      if (frame.type === Frame.stderr) Queue.offerUnsafe(stderrQueue, frame.payload)
      if (frame.type === Frame.json) return yield* decode(frame.payload, Settlement)
    }
  }).pipe(
    Effect.flatMap((event) =>
      event.event === "exit"
        ? Deferred.succeed(exit, event)
        : Deferred.fail(exit, processError(command, "exitCode", describe(event))),
    ),
    Effect.catch((closed) => Deferred.fail(exit, processError(command, "exitCode", closed.message))),
    Effect.ensuring(
      Effect.sync(() => {
        Queue.endUnsafe(stdoutQueue)
        Queue.endUnsafe(stderrQueue)
      }),
    ),
    Effect.forkScoped,
  )

  const stdinSink =
    mode(stdin.stream) === "pipe"
      ? Sink.forEach((chunk: Uint8Array) => Effect.sync(() => connection.send(Frame.stdin, chunk))).pipe(
          Sink.mapEffect(() =>
            Effect.sync(() => {
              if (stdin.endOnDone) connection.send(Frame.stdinEnd)
            }),
          ),
        )
      : Sink.drain
  if (Stream.isStream(stdin.stream)) yield* Effect.forkScoped(Stream.run(stdin.stream, stdinSink))
  const stdoutStream = withSink(Stream.fromQueue(stdoutQueue), stdout)
  const stderrStream = withSink(Stream.fromQueue(stderrQueue), stderr)
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(started.pid),
    stdin: stdinSink,
    stdout: stdoutStream,
    stderr: stderrStream,
    all: Stream.merge(stdoutStream, stderrStream),
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    isRunning: Deferred.isDone(exit).pipe(Effect.map((done) => !done)),
    exitCode: Deferred.await(exit).pipe(
      Effect.flatMap((settled) =>
        settled.code === null
          ? Effect.fail(
              processError(command, "exitCode", `Process interrupted due to receipt of signal: '${settled.signal}'`),
            )
          : Effect.succeed(ChildProcessSpawner.ExitCode(settled.code)),
      ),
    ),
    // The manager signals the job's process group, then SIGKILLs it after the spawn's forceKillAfterMs.
    kill: (killOptions) =>
      Effect.gen(function* () {
        if (yield* Deferred.isDone(exit)) return
        connection.send(Frame.json, encode({ op: "kill", signal: killOptions?.killSignal ?? "SIGTERM" }))
        yield* Effect.ignore(Deferred.await(exit))
      }),
    // No local child process holds the event loop open.
    unref: Effect.succeed(Effect.void),
  })
})

const files = (dispatcher: Interface, location: string): Environment.FilesImpl => {
  const call = <S extends Schema.ConstraintDecoder<unknown>>(
    target: string,
    request: object,
    success: S,
    body?: Uint8Array,
  ) =>
    Effect.gen(function* () {
      const connection = yield* dispatcher.open({ v: 1, op: "file", location, ...request })
      if (body && body.length > 0) connection.send(Frame.body, body)
      const result = yield* reply(connection, Schema.Union([success, ErrorReply]))
      if (isErrorReply(result)) return yield* Effect.fail(fileError(target, result))
      return { connection, result }
    }).pipe(Effect.catchTag("ScriptitDispatcher.Closed", (closed) => Effect.fail(failed(target, closed))))

  return {
    read: (target, range) =>
      Effect.scoped(
        Effect.gen(function* () {
          const response = yield* call(target, { action: "read", path: target, range }, ReadReply)
          const bytes = new Uint8Array(response.result.bytes)
          let received = 0
          while (received < bytes.length) {
            const frame = yield* response.connection.next.pipe(Effect.mapError((closed) => failed(target, closed)))
            if (frame.type !== Frame.body || received + frame.payload.length > bytes.length)
              return yield* Effect.fail(failed(target, new Closed({ message: "malformed read body" })))
            bytes.set(frame.payload, received)
            received += frame.payload.length
          }
          return { info: response.result.info, bytes }
        }),
      ),
    write: (target, bytes) =>
      Effect.scoped(call(target, { action: "write", path: target, bytes: bytes.length }, OkReply, bytes)).pipe(
        Effect.mapError(onlyFailed),
        Effect.asVoid,
      ),
    stat: (target) =>
      Effect.scoped(call(target, { action: "stat", path: target }, StatReply)).pipe(
        Effect.map((response) => response.result.info),
        Effect.catchTag("Environment.WrongKind", (error) => Effect.fail(onlyFailed(error))),
      ),
    list: (target) =>
      Effect.scoped(call(target, { action: "list", path: target }, ListReply)).pipe(
        Effect.map((response) => response.result.entries),
      ),
    remove: (target) =>
      Effect.scoped(call(target, { action: "remove", path: target }, OkReply)).pipe(
        Effect.mapError(onlyFailed),
        Effect.asVoid,
      ),
    move: (from, to) =>
      Effect.scoped(call(from, { action: "move", from, to }, OkReply)).pipe(
        Effect.catchTag("Environment.WrongKind", (error) => Effect.fail(onlyFailed(error))),
        Effect.asVoid,
      ),
    mkdir: (target) =>
      Effect.scoped(call(target, { action: "mkdir", path: target }, OkReply)).pipe(
        Effect.mapError(onlyFailed),
        Effect.asVoid,
      ),
  }
}

const connect = (socket: string, request: object) =>
  Effect.gen(function* () {
    const frames = yield* Queue.unbounded<{ readonly type: number; readonly payload: Uint8Array }, Closed>()
    const client = net.createConnection(socket)
    // The first failure wins; frames that arrived before it are still taken.
    const close = (message: string) => Queue.failCauseUnsafe(frames, Cause.fail(new Closed({ message })))
    let pending: Buffer = Buffer.alloc(0)
    client.on("data", (chunk: Buffer) => {
      pending = pending.length === 0 ? chunk : Buffer.concat([pending, chunk])
      while (pending.length >= 4) {
        const length = pending.readUInt32BE(0)
        if (length === 0 || length > MAX_PAYLOAD + 1) {
          close(`execution manager sent a malformed frame of ${length} bytes`)
          client.destroy()
          return
        }
        if (pending.length < 4 + length) return
        Queue.offerUnsafe(frames, { type: pending[4], payload: pending.subarray(5, 4 + length) })
        pending = pending.subarray(4 + length)
      }
    })
    client.on("error", (error) => close(`execution manager connection failed: ${error.message}`))
    client.on("close", () => close("execution manager closed the connection"))
    const send = (type: number, bytes: Uint8Array = new Uint8Array()) => {
      if (!client.writable) return
      for (let offset = 0; offset === 0 || offset < bytes.length; offset += MAX_PAYLOAD) {
        const payload = bytes.subarray(offset, offset + MAX_PAYLOAD)
        const header = Buffer.alloc(5)
        header.writeUInt32BE(payload.length + 1, 0)
        header[4] = type
        client.write(header)
        client.write(payload)
      }
    }
    send(Frame.json, encode(request))
    return { socket: client, next: Queue.take(frames), send }
  })

const encode = (value: object) => new TextEncoder().encode(JSON.stringify(value))

const reply = <S extends Schema.ConstraintDecoder<unknown>>(connection: Connection, schema: S) =>
  connection.next.pipe(
    Effect.flatMap((frame) =>
      frame.type === Frame.json
        ? decode(frame.payload, schema)
        : Effect.fail(new Closed({ message: `execution manager sent frame type ${frame.type} before its reply` })),
    ),
  )

const decode = <S extends Schema.ConstraintDecoder<unknown>>(payload: Uint8Array, schema: S) =>
  Option.match(Schema.decodeUnknownOption(Schema.fromJsonString(schema))(new TextDecoder().decode(payload)), {
    onNone: () => Effect.fail(new Closed({ message: "execution manager sent a malformed reply" })),
    onSome: (value) => Effect.succeed(value),
  })

const FileInfo = Schema.Struct({ type: EnvironmentFiles.FileType, size: Schema.Number, mtimeMs: Schema.Number })

const ErrorReply = Schema.Struct({
  event: Schema.Literal("error"),
  code: Schema.String,
  message: Schema.optional(Schema.String),
  path: Schema.optional(Schema.String),
  actual: Schema.optional(EnvironmentFiles.FileType),
})
type ErrorReply = typeof ErrorReply.Type

const Started = Schema.Union([Schema.Struct({ event: Schema.Literal("started"), pid: Schema.Number }), ErrorReply])

const Exit = Schema.Struct({
  event: Schema.Literal("exit"),
  code: Schema.NullOr(Schema.Number),
  signal: Schema.NullOr(Schema.String),
})
type Settled = typeof Exit.Type
const Settlement = Schema.Union([Exit, ErrorReply])

const OkReply = Schema.Struct({ event: Schema.Literal("ok") })
const StatReply = Schema.Struct({ event: Schema.Literal("ok"), info: FileInfo })
const ReadReply = Schema.Struct({ event: Schema.Literal("ok"), info: FileInfo, bytes: Schema.Number })
const ListReply = Schema.Struct({
  event: Schema.Literal("ok"),
  entries: Schema.Array(Schema.Struct({ name: Schema.String, type: EnvironmentFiles.FileType })),
})

const isErrorReply = Schema.is(ErrorReply)

const describe = (error: ErrorReply) => (error.message ? `${error.code}: ${error.message}` : error.code)

const processError = (command: ChildProcess.Command, method: string, description: string) =>
  PlatformError.systemError({
    _tag: "Unknown",
    module: "ChildProcess",
    method,
    pathOrDescriptor: command._tag === "StandardCommand" ? [command.command, ...command.args].join(" ") : undefined,
    description,
  })

const fileError = (target: string, error: ErrorReply) => {
  const at = error.path ?? target
  if (error.code === "NotFound") return new Environment.NotFound({ path: at })
  if (error.code === "WrongKind" && error.actual) return new Environment.WrongKind({ path: at, actual: error.actual })
  return new Environment.Failed({ path: at, cause: new Error(describe(error)) })
}

const failed = (target: string, closed: Closed) => new Environment.Failed({ path: target, cause: closed })

const onlyFailed = (error: Environment.NotFound | Environment.WrongKind | Environment.Failed) =>
  error._tag === "Environment.Failed" ? error : new Environment.Failed({ path: error.path, cause: error })

const stdinConfig = (input: ChildProcess.CommandOptions["stdin"]) =>
  input === undefined || typeof input === "string" || Stream.isStream(input)
    ? { stream: input ?? "pipe", endOnDone: true }
    : { stream: input.stream, endOnDone: input.endOnDone ?? true }

const outputConfig = (output: ChildProcess.CommandOptions["stdout"]) =>
  output === undefined || typeof output === "string" || Sink.isSink(output) ? output : output.stream

// A job has no host stdio, so the manager treats `inherit` as `ignore`.
const mode = (stream: unknown) => (stream === "ignore" || stream === "inherit" ? stream : "pipe")

const withSink = (stream: Stream.Stream<Uint8Array>, output: ChildProcess.CommandOutput | undefined) =>
  Sink.isSink(output) ? Stream.transduce(stream, output) : stream
