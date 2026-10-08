import type { SpawnOptions } from "node:child_process"
import { isAbsolute } from "node:path"
import type { ChildProcess } from "effect/unstable/process"

const guarded = new WeakSet<ChildProcess.Command>()

// Shell and formatter owners opt in; long-lived service processes do not hold workspace leases.
export function guard<A extends ChildProcess.Command>(command: A): A {
  guarded.add(command)
  if (command._tag === "PipedCommand") {
    guard(command.left)
    guard(command.right)
  }
  return command
}

export function inherit<A extends ChildProcess.Command>(command: A, source: ChildProcess.Command): A {
  return guarded.has(source) ? guard(command) : command
}

export function wrap(source: ChildProcess.StandardCommand, options: SpawnOptions) {
  const command = source.command
  const args = source.args
  if (!guarded.has(source)) return { command, args, options }
  // Only the host process configures the barrier; a command's environment cannot bypass it.
  const helper = process.env.SCRIPTIT_WRITE_BARRIER_HELPER
  const root = process.env.SCRIPTIT_WRITE_BARRIER_ROOT
  const python = process.env.SCRIPTIT_WRITE_BARRIER_PYTHON
  if (helper === undefined && root === undefined && python === undefined) return { command, args, options }
  for (const [name, value] of Object.entries({
    SCRIPTIT_WRITE_BARRIER_HELPER: helper,
    SCRIPTIT_WRITE_BARRIER_ROOT: root,
    SCRIPTIT_WRITE_BARRIER_PYTHON: python,
  })) {
    if (!value || value.includes("\0") || !isAbsolute(value)) {
      throw new Error(`${name} must be an absolute path when the process write barrier is configured`)
    }
  }
  if (process.platform === "win32") throw new Error("The process write barrier requires a POSIX host")

  // Match Node's shell option before wrapping so the shell never interprets helper arguments.
  const target = options.shell
    ? [
        typeof options.shell === "string"
          ? options.shell
          : process.platform === "android"
            ? "/system/bin/sh"
            : "/bin/sh",
        "-c",
        [command, ...args].join(" "),
      ]
    : [command, ...args]
  return {
    command: python!,
    args: ["-I", helper!, "run", "--root", root!, "--", ...target],
    options: { ...options, shell: false },
  }
}

export * as ProcessWriteBarrier from "./process-write-barrier.js"
