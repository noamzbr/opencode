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
 *
 * Every connection shares this process's memory, so both directions are
 * bounded. Receiving pauses the socket while a frame's worth of payload waits
 * to be taken, and a job's stdout and stderr hold one frame each, so a slow
 * reader stalls the job the way a full pipe does. Sending waits for the socket
 * to drain after each frame. Closing a spawn's scope stops its receiver and
 * closes the connection, which the manager treats as a kill.
 */

const Frame = { json: 1, stdout: 2, stderr: 3, stdin: 4, stdinEnd: 5, body: 6, ping: 7 } as const

const MAX_PAYLOAD = 1024 * 1024

// The upstream spawner likewise waits one second after a process exits for its output to close, then drops the rest.
const UNREAD_OUTPUT_DEADLINE = Duration.seconds(1)

export class TransportError extends Schema.TaggedError<TransportError>()("ScriptitDispatcher.TransportError", {
  message: Schema.String,
}) {}

export interface Connection {
  /** The next frame from the manager; fails once the manager's frames are exhausted. */
  readonly next: Effect.Effect<{ readonly type: number; readonly payload: Uint8Array }, TransportError>
  /**
   * Sends `bytes` as frames of `type`: a JSON message must fit one frame, while stdin and file bodies split at
   * the payload limit. Waits until the socket can take more, and fails when the connection is closed.
   */
  readonly send: (type: number, bytes?: Uint8Array) => Effect.Effect<void, TransportError>
}

export interface Interface {
  /** Connects and sends `request` as the first frame. Closing the scope closes the connection. */
  readonly open: (request: object) => Effect.Effect<Connection, TransportError, Scope.Scope>
}

class Service extends Context.Service<Service, Interface>()("@scriptit/ExecutionDispatcher") {}

const node = makeGlobalNode({
  service: Service,
  layer: Layer.sync(Service, () => {
    const socket = process.env.SCRIPTIT_EXEC_SOCKET
    return Service.of({
      open: (request) =>
        socket === undefined
          ? Effect.fail(
              new TransportError({ message: "SCRIPTIT_EXEC_SOCKET is unset; the execution manager is unavailable" }),
            )
          : Effect.acquireRelease(connect(socket), (connection) => Effect.sync(() => connection.socket.destroy())).pipe(
              Effect.tap((connection) => connection.send(Frame.json, encode(request))),
            ),
    })
  }),
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
  const fail = (error: TransportError) => processError(command, "spawn", error.message)
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

  const stdoutQueue = yield* Queue.bounded<Uint8Array, Cause.Done>(1)
  const stderrQueue = yield* Queue.bounded<Uint8Array, Cause.Done>(1)
  const exit = yield* Deferred.make<Settled, PlatformError.PlatformError>()
  const killing = yield* Deferred.make<void>()
  // After a kill, a stream whose reader leaves a frame untaken for the deadline is abandoned and its later
  // frames are dropped, so the exit frame behind them still arrives. A reader that keeps taking loses nothing.
  const abandoned = new Set<Queue.Queue<Uint8Array, Cause.Done>>()
  const deliver = (queue: Queue.Queue<Uint8Array, Cause.Done>, payload: Uint8Array) =>
    Effect.gen(function* () {
      if (abandoned.has(queue)) return
      const taken = yield* Effect.raceFirst(
        Queue.offer(queue, payload),
        Deferred.await(killing).pipe(Effect.andThen(Effect.sleep(UNREAD_OUTPUT_DEADLINE)), Effect.as(false)),
      )
      if (!taken) abandoned.add(queue)
    })
  yield* Effect.gen(function* () {
    for (;;) {
      const frame = yield* connection.next
      if (frame.type === Frame.stdout) yield* deliver(stdoutQueue, frame.payload)
      if (frame.type === Frame.stderr) yield* deliver(stderrQueue, frame.payload)
      if (frame.type === Frame.json) return yield* decode(frame.payload, Settlement)
    }
  }).pipe(
    Effect.flatMap((event) =>
      event.event === "exit"
        ? Deferred.succeed(exit, event)
        : Deferred.fail(exit, processError(command, "exitCode", describe(event))),
    ),
    Effect.catch((error) => Deferred.fail(exit, processError(command, "exitCode", error.message))),
    Effect.ensuring(
      Effect.sync(() => {
        Queue.endUnsafe(stdoutQueue)
        Queue.endUnsafe(stderrQueue)
      }),
    ),
    Effect.forkScoped,
  )

  const write = (type: number, bytes?: Uint8Array) =>
    connection.send(type, bytes).pipe(Effect.mapError((error) => processError(command, "stdin", error.message)))
  const stdinSink =
    mode(stdin.stream) === "pipe"
      ? Sink.forEach((chunk: Uint8Array) => write(Frame.stdin, chunk)).pipe(
          Sink.mapEffect(() => (stdin.endOnDone ? write(Frame.stdinEnd) : Effect.void)),
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
    // Settlement ends the wait, even while the frame still waits behind unsent stdin.
    kill: (killOptions) =>
      Effect.gen(function* () {
        if (yield* Deferred.isDone(exit)) return
        yield* Deferred.succeed(killing, undefined)
        yield* Effect.raceFirst(
          connection
            .send(Frame.json, encode({ op: "kill", signal: killOptions?.killSignal ?? "SIGTERM" }))
            .pipe(Effect.ignore, Effect.andThen(Effect.never)),
          Effect.ignore(Deferred.await(exit)),
        )
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
      // A refusal closes the connection during the body; the reply below reports it.
      if (body && body.length > 0) yield* Effect.ignore(connection.send(Frame.body, body))
      const result = yield* reply(connection, Schema.Union([success, ErrorReply]))
      if (isErrorReply(result)) return yield* Effect.fail(fileError(target, result))
      return { connection, result }
    }).pipe(Effect.catchTag("ScriptitDispatcher.TransportError", (error) => Effect.fail(failed(target, error))))

  return {
    read: (target, range) =>
      Effect.scoped(
        Effect.gen(function* () {
          const response = yield* call(target, { action: "read", path: target, range }, ReadReply)
          const bytes = new Uint8Array(response.result.bytes)
          let received = 0
          while (received < bytes.length) {
            const frame = yield* response.connection.next.pipe(Effect.mapError((error) => failed(target, error)))
            if (frame.type !== Frame.body || received + frame.payload.length > bytes.length)
              return yield* Effect.fail(failed(target, new TransportError({ message: "malformed read body" })))
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

const connect = (socket: string) =>
  Effect.gen(function* () {
    const frames = yield* Queue.unbounded<{ readonly type: number; readonly payload: Uint8Array }, TransportError>()
    const client = net.createConnection(socket)
    // The first failure wins; frames that arrived before it are still taken.
    const close = (message: string) => Queue.failCauseUnsafe(frames, Cause.fail(new TransportError({ message })))
    let pending: Buffer = Buffer.alloc(0)
    // Payload bytes received and not yet taken. The socket pauses at the frame limit, so the queue stays bounded.
    let queued = 0
    client.on("data", (chunk: Buffer) => {
      pending = pending.length === 0 ? chunk : Buffer.concat([pending, chunk])
      while (pending.length >= 4) {
        const length = pending.readUInt32BE(0)
        if (length === 0 || length > MAX_PAYLOAD + 1) {
          close(`execution manager sent a malformed frame of ${length} bytes`)
          client.destroy()
          return
        }
        if (pending.length < 4 + length) break
        if (pending[4] === Frame.ping && length !== 1) {
          close("execution manager sent a ping with a payload")
          client.destroy()
          return
        }
        // A ping only probes the connection while the manager is not reading it. It is never queued, so a
        // receiver that waits on unread output holds no pings.
        if (pending[4] !== Frame.ping) {
          Queue.offerUnsafe(frames, { type: pending[4], payload: pending.subarray(5, 4 + length) })
          queued += length - 1
        }
        pending = pending.subarray(4 + length)
      }
      if (queued >= MAX_PAYLOAD) client.pause()
    })
    client.on("error", (error) => close(`execution manager connection failed: ${error.message}`))
    client.on("close", () => close("execution manager closed the connection"))
    const drained = Effect.callback<void, TransportError>((resume) => {
      const settle = (effect: Effect.Effect<void, TransportError>) => {
        detach()
        resume(effect)
      }
      const onDrain = () => settle(Effect.void)
      const onClose = () =>
        settle(Effect.fail(new TransportError({ message: "execution manager closed the connection" })))
      const detach = () => {
        client.off("drain", onDrain)
        client.off("close", onClose)
      }
      client.on("drain", onDrain)
      client.on("close", onClose)
      return Effect.sync(detach)
    })
    const send = (type: number, bytes: Uint8Array = new Uint8Array()) =>
      Effect.gen(function* () {
        // A JSON frame holds one whole message; only stdin and file bodies span frames.
        if (type === Frame.json && bytes.length > MAX_PAYLOAD)
          return yield* new TransportError({
            message: `a ${bytes.length}-byte JSON message exceeds the ${MAX_PAYLOAD}-byte frame limit`,
          })
        for (let offset = 0; offset === 0 || offset < bytes.length; offset += MAX_PAYLOAD) {
          if (!client.writable) return yield* new TransportError({ message: "execution manager connection is closed" })
          const payload = bytes.subarray(offset, offset + MAX_PAYLOAD)
          const header = Buffer.alloc(5)
          header.writeUInt32BE(payload.length + 1, 0)
          header[4] = type
          client.write(header)
          if (!client.write(payload)) yield* drained
        }
      })
    const next = Queue.take(frames).pipe(
      Effect.tap((frame) =>
        Effect.sync(() => {
          queued -= frame.payload.length
          if (queued < MAX_PAYLOAD && client.isPaused()) client.resume()
        }),
      ),
    )
    return { socket: client, next, send }
  })

const encode = (value: object) => new TextEncoder().encode(JSON.stringify(value))

const reply = <S extends Schema.ConstraintDecoder<unknown>>(connection: Connection, schema: S) =>
  connection.next.pipe(
    Effect.flatMap((frame) =>
      frame.type === Frame.json
        ? decode(frame.payload, schema)
        : Effect.fail(
            new TransportError({ message: `execution manager sent frame type ${frame.type} before its reply` }),
          ),
    ),
  )

const decode = <S extends Schema.ConstraintDecoder<unknown>>(payload: Uint8Array, schema: S) =>
  Option.match(Schema.decodeUnknownOption(Schema.fromJsonString(schema))(new TextDecoder().decode(payload)), {
    onNone: () => Effect.fail(new TransportError({ message: "execution manager sent a malformed reply" })),
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

const failed = (target: string, error: TransportError) => new Environment.Failed({ path: target, cause: error })

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
