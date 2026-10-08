import { describe, expect } from "bun:test"
import fs from "node:fs/promises"
import path from "node:path"
import { Effect, Exit, Option, Stream } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode/util/cross-spawn-spawner"
import { AppProcess } from "@opencode/util/process"
import { ProcessWriteBarrier } from "@opencode/util/process-write-barrier"
import { Global } from "@opencode/util/global"
import { Config } from "../../src/config"
import { AppNodeBuilder } from "../../src/effect/app-node-builder"
import { Environment } from "../../src/environment/index"
import { Formatter } from "../../src/formatter"
import { Location } from "../../src/location"
import { Shell } from "../../src/shell"
import { withEnv } from "../fixture/env"
import { hostEnvironmentLayer } from "../fixture/environment"
import { tempGlobalLayer } from "../fixture/global"
import { tempLocationLayer } from "../fixture/location"
import { withTempDir } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"

const it = testEffect(
  LayerNode.compile(LayerNode.group([AppProcess.node, Formatter.node, CrossSpawnSpawner.node]), {
    replacements: [Location.node.replace(tempLocationLayer)],
  }),
)
const empty = {
  SCRIPTIT_WRITE_BARRIER_HELPER: undefined,
  SCRIPTIT_WRITE_BARRIER_ROOT: undefined,
  SCRIPTIT_WRITE_BARRIER_PYTHON: undefined,
}
const python = Bun.which("python3")

function command(executable: string, args: string[], options?: ChildProcess.CommandOptions) {
  return ProcessWriteBarrier.guard(ChildProcess.make(executable, args, options))
}

function withBarrier<A, E, R>(body: (root: string) => Effect.Effect<A, E, R>) {
  return withTempDir((tmp) =>
    Effect.gen(function* () {
      const helper = path.join(tmp.path, "helper ; special.py")
      const root = path.join(tmp.path, "root $special")
      yield* Effect.promise(() => fs.mkdir(root))
      yield* Effect.promise(() => fs.copyFile(new URL("../fixture/process-write-barrier.py", import.meta.url), helper))
      return yield* withEnv(
        {
          SCRIPTIT_WRITE_BARRIER_HELPER: helper,
          SCRIPTIT_WRITE_BARRIER_ROOT: root,
          SCRIPTIT_WRITE_BARRIER_PYTHON: python!,
        },
        () => body(root),
      )
    }),
  )
}

function invocations(root: string) {
  return Effect.promise(async () => {
    const text = await fs.readFile(path.join(root, "invocations.jsonl"), "utf8")
    return text
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as { args: string[]; isolated: number })
  })
}

describe("process write barrier", () => {
  it.live("leaves launch options unchanged when the host does not opt in", () =>
    withEnv(empty, () =>
      Effect.gen(function* () {
        const options = { shell: true, detached: false, env: { SCRIPTIT_WRITE_BARRIER_HELPER: "/untrusted" } }
        const target = ProcessWriteBarrier.wrap(command("printf", ["unchanged"]), options)
        expect(target).toEqual({ command: "printf", args: ["unchanged"], options })
        expect(target.options).toBe(options)
        const process = yield* AppProcess.Service
        const result = yield* process.run(command(globalThis.process.execPath, ["-e", "console.log('ok')"]))
        expect(result.stdout.toString().trim()).toBe("ok")
      }),
    ),
  )

  for (const [name, variables] of Object.entries({
    incomplete: { SCRIPTIT_WRITE_BARRIER_HELPER: "/helper.py" },
    empty: { SCRIPTIT_WRITE_BARRIER_HELPER: "" },
    relative: {
      SCRIPTIT_WRITE_BARRIER_HELPER: "helper.py",
      SCRIPTIT_WRITE_BARRIER_ROOT: "/state",
      SCRIPTIT_WRITE_BARRIER_PYTHON: "/python",
    },
  })) {
    it.live(`rejects ${name} host configuration before executing a command`, () =>
      withTempDir((tmp) =>
        withEnv({ ...empty, ...variables }, () =>
          Effect.gen(function* () {
            const process = yield* AppProcess.Service
            const target = path.join(tmp.path, "written")
            const error = yield* process
              .run(
                command(globalThis.process.execPath, [
                  "-e",
                  `require('fs').writeFileSync(${JSON.stringify(target)}, 'bad')`,
                ]),
              )
              .pipe(Effect.flip)
            expect(error).toBeInstanceOf(AppProcess.AppProcessError)
            expect(String(error.cause)).toContain("SCRIPTIT_WRITE_BARRIER_")
            expect(yield* Effect.promise(() => Bun.file(target).exists())).toBe(false)
          }),
        ),
      ),
    )
  }

  describe.skipIf(process.platform === "win32" || !python)("configured process launches", () => {
    it.live("leaves unmarked long-lived service processes outside the barrier", () =>
      withBarrier((root) =>
        Effect.gen(function* () {
          yield* Effect.promise(() => fs.writeFile(path.join(root, "blocked"), ""))
          const process = yield* AppProcess.Service
          const handle = yield* process.spawn(
            ChildProcess.make(
              globalThis.process.execPath,
              ["-e", "console.log('ready'); setInterval(() => {}, 1000)"],
              {
                forceKillAfter: "1 second",
              },
            ),
          )
          const ready = yield* handle.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead)
          expect(Option.getOrThrow(ready)).toBe("ready")
          expect(yield* handle.isRunning).toBe(true)
          expect(yield* Effect.promise(() => Bun.file(path.join(root, "invocations.jsonl")).exists())).toBe(false)
          yield* handle.kill({ forceKillAfter: "1 second" })
        }),
      ),
    )

    it.live("preserves arguments, stdin, cwd, environment, streams and exit status", () =>
      withBarrier((root) =>
        Effect.gen(function* () {
          const process = yield* AppProcess.Service
          const args = ["spaces and 'quotes'", "$(not-a-command)", "--user-flag"]
          const code = `const fs = require('fs'); console.log(JSON.stringify({ args: process.argv.slice(1), input: fs.readFileSync(0, 'utf8'), cwd: process.cwd(), value: process.env.VALUE, helper: process.env.SCRIPTIT_WRITE_BARRIER_HELPER })); console.error('stderr'); process.exit(17)`
          const result = yield* process.run(
            command(globalThis.process.execPath, ["-e", code, "--", ...args], {
              cwd: root,
              env: { VALUE: "value", SCRIPTIT_WRITE_BARRIER_HELPER: "", SCRIPTIT_WRITE_BARRIER_ROOT: "/untrusted" },
              extendEnv: false,
            }),
            { stdin: "input bytes" },
          )
          expect(result.exitCode).toBe(17)
          expect(result.stderr.toString().trim()).toBe("stderr")
          expect(JSON.parse(result.stdout.toString())).toEqual({
            args,
            input: "input bytes",
            cwd: root,
            value: "value",
            helper: "",
          })
          expect(yield* invocations(root)).toEqual([
            { args: [globalThis.process.execPath, "-e", code, "--", ...args], isolated: 1 },
          ])
        }),
      ),
    )

    for (const shell of [true, "/bin/sh"] as const) {
      it.live(`preserves shell expansion and redirection with shell=${shell}`, () =>
        withBarrier((root) =>
          Effect.gen(function* () {
            const process = yield* AppProcess.Service
            const result = yield* process.run(
              command("printf", ["'%s'", '"$VALUE"', "> shell.txt"], {
                shell,
                cwd: root,
                env: { VALUE: "shell value" },
              }),
            )
            expect(result.exitCode).toBe(0)
            expect(yield* Effect.promise(() => fs.readFile(path.join(root, "shell.txt"), "utf8"))).toBe("shell value")
            expect((yield* invocations(root))[0].args).toEqual(["/bin/sh", "-c", "printf '%s' \"$VALUE\" > shell.txt"])
          }),
        ),
      )
    }

    it.live("guards explicit shell commands through the raw spawner", () =>
      withBarrier((root) =>
        Effect.gen(function* () {
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
          expect(yield* spawner.exitCode(command("/bin/sh", ["-c", "printf written > shell.txt"], { cwd: root }))).toBe(
            ChildProcessSpawner.ExitCode(0),
          )
          expect(yield* Effect.promise(() => fs.readFile(path.join(root, "shell.txt"), "utf8"))).toBe("written")
          expect((yield* invocations(root))[0].args).toEqual(["/bin/sh", "-c", "printf written > shell.txt"])
        }),
      ),
    )

    it.live("the shell owner guards execution after merging caller environment", () =>
      withBarrier((root) =>
        Effect.gen(function* () {
          const shell = yield* Shell.Service
          const input = {
            shell: "/bin/sh",
            command: "printf shell-owner > source.txt",
            cwd: root,
            timeout: 0,
            env: { SCRIPTIT_WRITE_BARRIER_HELPER: "", SCRIPTIT_WRITE_BARRIER_ROOT: "/untrusted" },
          }
          yield* Effect.promise(() => fs.writeFile(path.join(root, "blocked"), ""))
          const refused = yield* shell.create(input)
          expect((yield* shell.wait(refused.id)).exit).toBe(73)
          expect(yield* Effect.promise(() => Bun.file(path.join(root, "source.txt")).exists())).toBe(false)
          yield* Effect.promise(() => fs.unlink(path.join(root, "blocked")))
          const started = yield* shell.create(input)
          expect(started.command).toBe(input.command)
          expect((yield* shell.wait(started.id)).exit).toBe(0)
          expect(yield* Effect.promise(() => fs.readFile(path.join(root, "source.txt"), "utf8"))).toBe("shell-owner")
          expect(yield* invocations(root)).toHaveLength(2)
        }).pipe(
          Effect.provide(
            AppNodeBuilder.build(Shell.node, [
              Location.node.replace(tempLocationLayer),
              Global.node.replace(tempGlobalLayer),
              Config.node.replace(Config.testLayer()),
              Environment.node.replace(hostEnvironmentLayer),
            ]),
          ),
        ),
      ),
    )

    it.live("retains guard marks when pipeline commands acquire streamed input", () =>
      withBarrier((root) =>
        Effect.gen(function* () {
          const process = yield* AppProcess.Service
          const pipeline = ProcessWriteBarrier.guard(
            ChildProcess.make(globalThis.process.execPath, ["-e", "process.stdout.write('piped')"]).pipe(
              ChildProcess.pipeTo(
                ChildProcess.make(globalThis.process.execPath, ["-e", "process.stdin.pipe(process.stdout)"]),
              ),
            ),
          )
          const result = yield* process.run(pipeline)
          expect(result.stdout.toString()).toBe("piped")
          expect(result.exitCode).toBe(0)
          expect(yield* invocations(root)).toHaveLength(2)
        }),
      ),
    )

    it.live("guards the real formatter and prevents writes when the helper refuses launch", () =>
      withBarrier((root) =>
        Effect.gen(function* () {
          const formatter = yield* Formatter.Service
          const file = path.join(root, "source.barrier")
          yield* Effect.promise(() => fs.writeFile(file, "original"))
          yield* formatter.transform((editor) =>
            editor.set({
              name: "writer",
              extensions: [".barrier"],
              enabled: Effect.succeed([
                globalThis.process.execPath,
                "-e",
                "require('fs').writeFileSync(process.argv[1], 'formatted')",
                "$FILE",
              ]),
              environment: { SCRIPTIT_WRITE_BARRIER_HELPER: "", SCRIPTIT_WRITE_BARRIER_ROOT: "/untrusted" },
            }),
          )
          yield* Effect.promise(() => fs.writeFile(path.join(root, "blocked"), ""))
          expect(yield* formatter.file(file)).toBe(false)
          expect(yield* Effect.promise(() => fs.readFile(file, "utf8"))).toBe("original")
          yield* Effect.promise(() => fs.unlink(path.join(root, "blocked")))
          expect(yield* formatter.file(file)).toBe(true)
          expect(yield* Effect.promise(() => fs.readFile(file, "utf8"))).toBe("formatted")
          expect(yield* invocations(root)).toHaveLength(2)
        }),
      ),
    )

    it.live("passes additional file descriptors through the helper", () =>
      withBarrier(() =>
        Effect.gen(function* () {
          const handle = yield* command(globalThis.process.execPath, ["-e", "require('fs').writeSync(3, 'extra fd')"], {
            additionalFds: { fd3: { type: "output" } },
          })
          const output = yield* Stream.mkUint8Array(handle.getOutputFd(3))
          expect(new TextDecoder().decode(output)).toBe("extra fd")
          expect(yield* handle.exitCode).toBe(ChildProcessSpawner.ExitCode(0))
        }),
      ),
    )

    it.live("preserves signal termination as a process failure", () =>
      withBarrier(() =>
        Effect.gen(function* () {
          const process = yield* AppProcess.Service
          const result = yield* process
            .run(command(globalThis.process.execPath, ["-e", "process.kill(process.pid, 'SIGTERM')"]))
            .pipe(Effect.exit)
          expect(Exit.isFailure(result)).toBe(true)
        }),
      ),
    )

    for (const detached of [true, false]) {
      it.live(`forwards cancellation with detached=${detached}`, () =>
        withBarrier((root) =>
          Effect.gen(function* () {
            const process = yield* AppProcess.Service
            const handle = yield* process.spawn(
              command(
                globalThis.process.execPath,
                [
                  "-e",
                  "process.on('SIGTERM', () => { require('fs').writeFileSync('stopped', 'yes'); process.exit(23) }); console.log('ready'); setInterval(() => {}, 1000)",
                ],
                { cwd: root, detached, forceKillAfter: "1 second" },
              ),
            )
            const ready = yield* handle.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead)
            expect(Option.getOrThrow(ready)).toBe("ready")
            yield* handle.kill({ forceKillAfter: "1 second" })
            expect(yield* handle.exitCode).toBe(ChildProcessSpawner.ExitCode(23))
            expect(yield* Effect.promise(() => fs.readFile(path.join(root, "stopped"), "utf8"))).toBe("yes")
          }),
        ),
      )
    }
  })
})
