import { Effect, Layer, Schema, Semaphore, Context, Stream } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { formatPatch, structuredPatch } from "diff"
import path from "path"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { InstanceState } from "@/effect/instance-state"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { Hash } from "@opencode-ai/core/util/hash"
import { Config } from "@/config/config"
import { Global } from "@opencode-ai/core/global"
import * as Log from "@opencode-ai/core/util/log"

export const Patch = Schema.Struct({
  hash: Schema.String,
  files: Schema.mutable(Schema.Array(Schema.String)),
})
export type Patch = typeof Patch.Type

export const FileDiff = Schema.Struct({
  // Optional because legacy/imported `summary_diffs` on disk may omit
  // file details and patch text. Required Schema rejected the whole
  // session response and broke session loading on Desktop.
  file: Schema.optional(Schema.String),
  patch: Schema.optional(Schema.String),
  additions: Schema.Finite,
  deletions: Schema.Finite,
  status: Schema.optional(Schema.Literals(["added", "deleted", "modified"])),
}).annotate({ identifier: "SnapshotFileDiff" })
export type FileDiff = typeof FileDiff.Type

const log = Log.create({ service: "snapshot" })
const limit = 2 * 1024 * 1024
const day = 24 * 60 * 60 * 1000
// Full re-indexing freshens Git objects before backup's seven-day prune threshold.
const refresh = 6 * day
const core = ["-c", "core.longpaths=true", "-c", "core.symlinks=true"]
const cfg = ["-c", "core.autocrlf=false", ...core]
const quote = [...cfg, "-c", "core.quotepath=false"]
interface GitResult {
  readonly code: ChildProcessSpawner.ExitCode
  readonly text: string
  readonly stderr: string
}

type State = Omit<Interface, "init">

export class Error extends Schema.TaggedErrorClass<Error>()("SnapshotError", {
  operation: Schema.Literals(["restore", "revert"]),
  message: Schema.String,
}) {}

export interface Interface {
  readonly init: () => Effect.Effect<void>
  readonly track: () => Effect.Effect<string | undefined>
  readonly patch: (hash: string, to?: string) => Effect.Effect<Patch>
  readonly restore: (snapshot: string) => Effect.Effect<void, Error>
  readonly revert: (patches: Patch[], restoreSnapshot?: string) => Effect.Effect<void, Error>
  readonly diff: (hash: string) => Effect.Effect<string>
  readonly diffFull: (from: string, to: string) => Effect.Effect<FileDiff[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Snapshot") {}

export const layer: Layer.Layer<
  Service,
  never,
  AppFileSystem.Service | ChildProcessSpawner.ChildProcessSpawner | Config.Service
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* AppFileSystem.Service
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const config = yield* Config.Service
    const locks = new Map<string, Semaphore.Semaphore>()

    const lock = (key: string) => {
      const hit = locks.get(key)
      if (hit) return hit

      const next = Semaphore.makeUnsafe(1)
      locks.set(key, next)
      return next
    }

    const state = yield* InstanceState.make<State>(
      Effect.fn("Snapshot.state")(function* (ctx) {
        const state = {
          directory: ctx.directory,
          worktree: ctx.worktree,
          gitdir: path.join(Global.Path.data, "snapshot", ctx.project.id, Hash.fast(ctx.worktree)),
          vcs: ctx.project.vcs,
        }

        const args = (cmd: string[]) => ["--git-dir", state.gitdir, "--work-tree", state.worktree, ...cmd]

        const enc = new TextEncoder()
        const feed = (list: string[]) => Stream.make(enc.encode(list.join("\0") + "\0"))
        const feedSpec = (list: string[]) =>
          feed(list.map((item) => `:(top,literal)${item.replaceAll("\\", "/")}`))
        const scope = path.relative(state.worktree, state.directory).replaceAll("\\", "/")
        const spec = scope ? `:(top,literal)${scope}` : "."

        const git = Effect.fnUntraced(
          function* (
            cmd: string[],
            opts?: { cwd?: string; env?: Record<string, string>; stdin?: ChildProcess.CommandInput },
          ) {
            const proc = ChildProcess.make("git", cmd, {
              cwd: opts?.cwd,
              env: opts?.env,
              extendEnv: true,
              stdin: opts?.stdin,
            })
            const handle = yield* spawner.spawn(proc)
            const [text, stderr] = yield* Effect.all(
              [Stream.mkString(Stream.decodeText(handle.stdout)), Stream.mkString(Stream.decodeText(handle.stderr))],
              { concurrency: 2 },
            )
            const code = yield* handle.exitCode
            return { code, text, stderr } satisfies GitResult
          },
          Effect.scoped,
          Effect.catch((err) =>
            Effect.succeed({
              code: ChildProcessSpawner.ExitCode(1),
              text: "",
              stderr: err instanceof globalThis.Error ? err.message : String(err),
            }),
          ),
        )

        const ignore = Effect.fnUntraced(function* (files: string[]) {
          if (!files.length) return new Set<string>()
          const check = yield* git(
            [
              ...quote,
              "--git-dir",
              path.join(state.worktree, ".git"),
              "--work-tree",
              state.worktree,
              "check-ignore",
              "--no-index",
              "--stdin",
              "-z",
            ],
            {
              cwd: state.worktree,
              stdin: feed(files),
            },
          )
          if (check.code !== 0 && check.code !== 1) return undefined
          return new Set(check.text.split("\0").filter(Boolean))
        })

        const exists = (file: string) => fs.exists(file).pipe(Effect.orDie)
        const locked = <A, E, R>(fx: Effect.Effect<A, E, R>) => lock(state.directory).withPermits(1)(fx)

        const discardLegacy = () =>
          Effect.all(
            ["index", "index.lock", path.join("info", "exclude")].map((item) =>
              fs.remove(path.join(state.gitdir, item)).pipe(Effect.ignore),
            ),
            { concurrency: "unbounded", discard: true },
          )

        yield* discardLegacy()

        let seed: Uint8Array | undefined
        let refreshed = 0
        const withIndex = <A, E, R>(run: (env: Record<string, string>) => Effect.Effect<A, E, R>) =>
          Effect.scoped(
            Effect.gen(function* () {
              const root = path.join(state.gitdir, "tmp")
              yield* fs.ensureDir(root)
              const dir = yield* fs.makeTempDirectoryScoped({ directory: root, prefix: "index-" })
              const index = path.join(dir, "index")
              if (seed) yield* fs.writeFile(index, seed)
              return yield* run({ GIT_INDEX_FILE: index })
            }),
          )

        const enabled = Effect.fnUntraced(function* () {
          if (state.vcs !== "git") return false
          return (yield* config.get()).snapshot !== false
        })

        const ensureRepo = Effect.fnUntraced(function* () {
          return yield* lock(state.gitdir).withPermits(1)(
            Effect.gen(function* () {
              const existed = yield* exists(path.join(state.gitdir, "objects"))
              yield* fs.ensureDir(state.gitdir).pipe(Effect.orDie)
              if (!existed) {
                const result = yield* git(["init"], {
                  env: { GIT_DIR: state.gitdir, GIT_WORK_TREE: state.worktree },
                })
                if (result.code !== 0) return false
                log.info("initialized")
              }
              yield* discardLegacy()
              return true
            }),
          )
        })

        const retain = Effect.fnUntraced(function* (hash: string) {
          // The daily ref's reflog keeps every distinct captured tree reachable until backup expires the bucket.
          const ref = `refs/opencode/snapshot/${Math.floor(Date.now() / day)}`
          const result = yield* lock(state.gitdir).withPermits(1)(
            git([...cfg, ...args(["update-ref", "--create-reflog", ref, hash])], { cwd: state.worktree }),
          )
          if (result.code === 0) return true
          log.warn("failed to retain snapshot", { hash, stderr: result.stderr })
          return false
        })

        const capture = Effect.fnUntraced(function* () {
          if (!(yield* enabled())) return undefined
          if (!(yield* ensureRepo())) return undefined
          const rebuild = !seed || Date.now() - refreshed >= refresh
          if (rebuild) seed = undefined
          return yield* withIndex((env) =>
            Effect.gen(function* () {
              if (!seed) {
                const empty = yield* git([...cfg, ...args(["read-tree", "--empty"])], { cwd: state.worktree, env })
                if (empty.code !== 0) return undefined
              }
              const [changed, found] = yield* Effect.all(
                [
                  git([...quote, ...args(["diff-files", "--name-only", "-z", "--", spec])], {
                    cwd: state.worktree,
                    env,
                  }),
                  git([...quote, ...args(["ls-files", "--others", "--exclude-standard", "-z", "--", spec])], {
                    cwd: state.worktree,
                    env,
                  }),
                ],
                { concurrency: 2 },
              )
              if (changed.code !== 0 || found.code !== 0) return undefined
              const files = Array.from(
                new Set([...changed.text.split("\0").filter(Boolean), ...found.text.split("\0").filter(Boolean)]),
              )
              const ignored = yield* ignore(files)
              if (!ignored) return undefined
              const allow = files.filter((item) => !ignored.has(item))
              const large = new Set(
                (yield* Effect.all(
                  allow.map((item) =>
                    fs.stat(path.join(state.worktree, item)).pipe(
                      Effect.catch(() => Effect.void),
                      Effect.map((stat) => {
                        if (!stat || stat.type !== "File") return undefined
                        const size = typeof stat.size === "bigint" ? Number(stat.size) : stat.size
                        return size > limit ? item : undefined
                      }),
                    ),
                  ),
                  { concurrency: 8 },
                )).filter((item): item is string => Boolean(item)),
              )
              const drop = Array.from(new Set([...ignored, ...large]))
              if (drop.length) {
                const removed = yield* git(
                  [
                    ...cfg,
                    ...args([
                      "rm",
                      "--cached",
                      "-f",
                      "--ignore-unmatch",
                      "--pathspec-from-file=-",
                      "--pathspec-file-nul",
                    ]),
                  ],
                  { cwd: state.worktree, env, stdin: feedSpec(drop) },
                )
                if (removed.code !== 0) return undefined
              }
              const stage = allow.filter((item) => !large.has(item))
              if (stage.length) {
                const added = yield* git(
                  [...cfg, ...args(["add", "--all", "--sparse", "--pathspec-from-file=-", "--pathspec-file-nul"])],
                  { cwd: state.worktree, env, stdin: feedSpec(stage) },
                )
                if (added.code !== 0) return undefined
              }
              const result = yield* git([...cfg, ...args(["write-tree"])], { cwd: state.worktree, env })
              if (result.code !== 0) return undefined
              const hash = result.text.trim()
              if (!hash) return undefined
              if (!(yield* retain(hash))) return undefined
              const next = yield* fs.readFile(env.GIT_INDEX_FILE).pipe(Effect.catch(() => Effect.void))
              if (next) {
                seed = next
                if (rebuild) refreshed = Date.now()
              }
              log.info("tracking", { hash, cwd: state.directory, git: state.gitdir })
              return hash
            }),
          )
        })

        const track = Effect.fnUntraced(function* () {
          return yield* locked(capture()).pipe(
            Effect.catchCause((cause) => {
              log.warn("snapshot capture failed", { cause: String(cause) })
              return Effect.succeed<string | undefined>(undefined)
            }),
          )
        })

        const patch = Effect.fnUntraced(function* (hash: string, to?: string) {
          return yield* locked(
            Effect.gen(function* () {
              const target = to ?? (yield* capture())
              if (!target) return { hash, files: [] }
              const result = yield* git(
                [...quote, ...args(["diff", "--no-ext-diff", "--name-only", "-z", hash, target, "--", spec])],
                { cwd: state.worktree },
              )
              if (result.code !== 0) {
                log.warn("failed to get diff", { hash, target, exitCode: result.code, stderr: result.stderr })
                return { hash, files: [] }
              }
              const files = result.text.split("\0").filter(Boolean)
              const ignored = (yield* ignore(files)) ?? new Set<string>()
              return {
                hash,
                files: files
                  .filter((item) => !ignored.has(item))
                  .map((item) => path.join(state.worktree, item).replaceAll("\\", "/")),
              }
            }),
          ).pipe(
            Effect.catchCause((cause) => {
              log.warn("snapshot patch failed", { hash, cause: String(cause) })
              return Effect.succeed({ hash, files: [] })
            }),
          )
        })

        const apply = Effect.fnUntraced(function* (
          patches: Patch[],
          restoreSnapshot: string | undefined,
          operation: "restore" | "revert",
        ) {
          if (!restoreSnapshot && patches.every((item) => !item.files.length)) return undefined
          const fail = (message: string) => new Error({ operation, message })
          let started = false
          return yield* withIndex((env) =>
            Effect.gen(function* () {
              const ops: { hash: string; file: string; rel: string; have: boolean }[] = []
              const seen = new Set<string>()
              for (const item of patches) {
                for (const input of item.files) {
                  const file = path.resolve(input)
                  if (seen.has(file)) continue
                  const local = path.relative(state.directory, file)
                  if (!local || local === ".." || local.startsWith(`..${path.sep}`) || path.isAbsolute(local)) {
                    return yield* fail(`snapshot path is outside the session directory: ${input}`)
                  }
                  seen.add(file)
                  ops.push({
                    hash: item.hash,
                    file,
                    rel: path.relative(state.worktree, file).replaceAll("\\", "/"),
                    have: false,
                  })
                }
              }

              const read = yield* git(
                [...cfg, ...args(["read-tree", ...(restoreSnapshot ? [restoreSnapshot] : ["--empty"])])],
                { cwd: state.worktree, env },
              )
              if (read.code !== 0)
                return yield* fail(`failed to read snapshot: ${read.stderr.trim() || restoreSnapshot}`)

              const restoreFiles: string[] = []
              const objects = new Set<string>()
              if (restoreSnapshot) {
                const list = yield* git([...quote, ...args(["ls-files", "--stage", "-z", "--", spec])], {
                  cwd: state.worktree,
                  env,
                })
                if (list.code !== 0) return yield* fail(`failed to list snapshot files: ${list.stderr.trim()}`)
                for (const row of list.text.split("\0").filter(Boolean)) {
                  const match = row.match(/^(\d+) ([0-9a-f]+) \d\t(.+)$/s)
                  if (!match) return yield* fail("snapshot index contains an invalid entry")
                  if (match[1] !== "160000") objects.add(match[2])
                  restoreFiles.push(match[3])
                }
              }

              const clash = (a: string, b: string) => a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`)
              const batches: (typeof ops)[] = []
              for (let i = 0; i < ops.length; ) {
                const first = ops[i]
                const run = [first]
                let j = i + 1
                while (j < ops.length && run.length < 100) {
                  const next = ops[j]
                  if (next.hash !== first.hash) break
                  if (run.some((item) => clash(item.rel, next.rel))) break
                  run.push(next)
                  j += 1
                }
                batches.push(run)
                i = j
              }

              for (const run of batches) {
                const first = run[0]
                const result = yield* git(
                  [
                    ...quote,
                    ...args(["ls-tree", "-r", "-z", "--full-tree", first.hash, "--", ...run.map((x) => x.rel)]),
                  ],
                  { cwd: state.worktree },
                )
                if (result.code !== 0) return yield* fail(`failed to inspect snapshot tree: ${first.hash}`)
                for (const row of result.text.split("\0").filter(Boolean)) {
                  const match = row.match(/^(\d+) \w+ ([0-9a-f]+)\t(.+)$/s)
                  if (!match) return yield* fail("snapshot tree contains an invalid entry")
                  if (match[1] !== "160000") objects.add(match[2])
                  const file = match[3]
                  const op = run.find((item) => file === item.rel || file.startsWith(`${item.rel}/`))
                  if (op) op.have = true
                }
              }

              const ids = Array.from(objects)
              if (ids.length) {
                const checked = yield* git([...cfg, ...args(["cat-file", "--batch-check"])], {
                  cwd: state.worktree,
                  stdin: Stream.make(enc.encode(`${ids.join("\n")}\n`)),
                })
                const lines = checked.text.trim().split("\n")
                if (
                  checked.code !== 0 ||
                  lines.length !== ids.length ||
                  lines.some((line, index) => !line.startsWith(`${ids[index]} `) || line.endsWith(" missing"))
                ) {
                  return yield* fail("snapshot objects are unavailable")
                }
              }

              started = true
              if (restoreFiles.length) {
                const checkout = yield* git([...cfg, ...args(["checkout-index", "--force", "-z", "--stdin"])], {
                  cwd: state.worktree,
                  env,
                  stdin: feed(restoreFiles),
                })
                if (checkout.code !== 0) {
                  log.warn("failed to restore snapshot", {
                    snapshot: restoreSnapshot,
                    exitCode: checkout.code,
                    stderr: checkout.stderr,
                  })
                }
              }

              for (const run of batches) {
                const first = run[0]
                const have = run.filter((item) => item.have)
                if (have.length) {
                  const checkout = yield* git(
                    [
                      ...cfg,
                      ...args(["checkout", first.hash, "--", ...have.map((item) => `:(top,literal)${item.rel}`)]),
                    ],
                    { cwd: state.worktree, env },
                  )
                  if (checkout.code !== 0) {
                    log.warn("batched snapshot checkout failed, falling back to individual paths", {
                      hash: first.hash,
                      files: have.length,
                      exitCode: checkout.code,
                      stderr: checkout.stderr,
                    })
                    for (const item of have) {
                      const single = yield* git(
                        [...cfg, ...args(["checkout", first.hash, "--", `:(top,literal)${item.rel}`])],
                        { cwd: state.worktree, env },
                      )
                      if (single.code === 0) continue
                      log.warn("failed to restore snapshot path", {
                        file: item.file,
                        hash: first.hash,
                        exitCode: single.code,
                        stderr: single.stderr,
                      })
                    }
                  }
                }
                for (const item of run) {
                  if (item.have) continue
                  yield* fs
                    .remove(item.file, { recursive: true, force: true })
                    .pipe(
                      Effect.catch((error) => {
                        log.warn("failed to remove snapshot path", { file: item.file, error: String(error) })
                        return Effect.void
                      }),
                    )
                }
              }
              return undefined
            }),
          ).pipe(
            Effect.catch((error) => {
              if (error instanceof Error) return Effect.fail(error)
              if (!started) return Effect.fail(fail(`failed to create scratch index: ${String(error)}`))
              log.warn("snapshot apply failed after worktree updates began", { operation, error: String(error) })
              return Effect.void
            }),
          )
        })

        const restore = Effect.fnUntraced(function* (snapshot: string) {
          return yield* locked(apply([], snapshot, "restore"))
        })

        const revert = Effect.fnUntraced(function* (patches: Patch[], restoreSnapshot?: string) {
          return yield* locked(apply(patches, restoreSnapshot, "revert"))
        })

        const diff = Effect.fnUntraced(function* (hash: string) {
          return yield* locked(
            Effect.gen(function* () {
              const target = yield* capture()
              if (!target) return ""
              const result = yield* git([...quote, ...args(["diff", "--no-ext-diff", hash, target, "--", spec])], {
                cwd: state.worktree,
              })
              if (result.code !== 0) {
                log.warn("failed to get diff", { hash, target, exitCode: result.code, stderr: result.stderr })
                return ""
              }
              return result.text.trim()
            }),
          ).pipe(
            Effect.catchCause((cause) => {
              log.warn("snapshot diff failed", { hash, cause: String(cause) })
              return Effect.succeed("")
            }),
          )
        })

        const diffFull = Effect.fnUntraced(function* (from: string, to: string) {
          return yield* locked(
            Effect.gen(function* () {
              type Row = {
                file: string
                status: "added" | "deleted" | "modified"
                binary: boolean
                additions: number
                deletions: number
              }

              type Ref = {
                file: string
                side: "before" | "after"
                ref: string
              }

              const show = Effect.fnUntraced(function* (row: Row) {
                if (row.binary) return ["", ""]
                if (row.status === "added") {
                  return [
                    "",
                    yield* git([...cfg, ...args(["show", `${to}:${row.file}`])]).pipe(Effect.map((item) => item.text)),
                  ]
                }
                if (row.status === "deleted") {
                  return [
                    yield* git([...cfg, ...args(["show", `${from}:${row.file}`])]).pipe(
                      Effect.map((item) => item.text),
                    ),
                    "",
                  ]
                }
                return yield* Effect.all(
                  [
                    git([...cfg, ...args(["show", `${from}:${row.file}`])]).pipe(Effect.map((item) => item.text)),
                    git([...cfg, ...args(["show", `${to}:${row.file}`])]).pipe(Effect.map((item) => item.text)),
                  ],
                  { concurrency: 2 },
                )
              })

              const load = Effect.fnUntraced(
                function* (rows: Row[]) {
                  const refs = rows.flatMap((row) => {
                    if (row.binary) return []
                    if (row.status === "added")
                      return [{ file: row.file, side: "after", ref: `${to}:${row.file}` } satisfies Ref]
                    if (row.status === "deleted") {
                      return [{ file: row.file, side: "before", ref: `${from}:${row.file}` } satisfies Ref]
                    }
                    return [
                      { file: row.file, side: "before", ref: `${from}:${row.file}` } satisfies Ref,
                      { file: row.file, side: "after", ref: `${to}:${row.file}` } satisfies Ref,
                    ]
                  })
                  if (!refs.length) return new Map<string, { before: string; after: string }>()

                  const proc = ChildProcess.make("git", [...cfg, ...args(["cat-file", "--batch"])], {
                    cwd: state.directory,
                    extendEnv: true,
                    stdin: Stream.make(new TextEncoder().encode(refs.map((item) => item.ref).join("\n") + "\n")),
                  })
                  const handle = yield* spawner.spawn(proc)
                  const [out, err] = yield* Effect.all(
                    [Stream.mkUint8Array(handle.stdout), Stream.mkString(Stream.decodeText(handle.stderr))],
                    { concurrency: 2 },
                  )
                  const code = yield* handle.exitCode
                  if (code !== 0) {
                    log.info("git cat-file --batch failed during snapshot diff, falling back to per-file git show", {
                      stderr: err,
                      refs: refs.length,
                    })
                    return
                  }

                  const fail = (msg: string, extra?: Record<string, string>) => {
                    log.info(msg, { ...extra, refs: refs.length })
                    return undefined
                  }

                  const map = new Map<string, { before: string; after: string }>()
                  const dec = new TextDecoder()
                  let i = 0
                  for (const ref of refs) {
                    let end = i
                    while (end < out.length && out[end] !== 10) end += 1
                    if (end >= out.length) {
                      return fail(
                        "git cat-file --batch returned a truncated header during snapshot diff, falling back to per-file git show",
                      )
                    }

                    const head = dec.decode(out.slice(i, end))
                    i = end + 1
                    const hit = map.get(ref.file) ?? { before: "", after: "" }
                    if (head.endsWith(" missing")) {
                      map.set(ref.file, hit)
                      continue
                    }

                    const match = head.match(/^[0-9a-f]+ blob (\d+)$/)
                    if (!match) {
                      return fail(
                        "git cat-file --batch returned an unexpected header during snapshot diff, falling back to per-file git show",
                        { head },
                      )
                    }

                    const size = Number(match[1])
                    if (!Number.isInteger(size) || size < 0 || i + size >= out.length || out[i + size] !== 10) {
                      return fail(
                        "git cat-file --batch returned truncated content during snapshot diff, falling back to per-file git show",
                        { head },
                      )
                    }

                    const text = dec.decode(out.slice(i, i + size))
                    if (ref.side === "before") hit.before = text
                    if (ref.side === "after") hit.after = text
                    map.set(ref.file, hit)
                    i += size + 1
                  }

                  if (i !== out.length) {
                    return fail(
                      "git cat-file --batch returned trailing data during snapshot diff, falling back to per-file git show",
                    )
                  }

                  return map
                },
                Effect.scoped,
                Effect.catch(() =>
                  Effect.succeed<Map<string, { before: string; after: string }> | undefined>(undefined),
                ),
              )

              const result: FileDiff[] = []
              const status = new Map<string, "added" | "deleted" | "modified">()

              const statuses = yield* git(
                [...quote, ...args(["diff", "--no-ext-diff", "--name-status", "--no-renames", from, to, "--", spec])],
                { cwd: state.directory },
              )

              for (const line of statuses.text.trim().split("\n")) {
                if (!line) continue
                const [code, file] = line.split("\t")
                if (!code || !file) continue
                status.set(file, code.startsWith("A") ? "added" : code.startsWith("D") ? "deleted" : "modified")
              }

              const numstat = yield* git(
                [...quote, ...args(["diff", "--no-ext-diff", "--no-renames", "--numstat", from, to, "--", spec])],
                {
                  cwd: state.directory,
                },
              )

              const rows = numstat.text
                .trim()
                .split("\n")
                .filter(Boolean)
                .flatMap((line) => {
                  const [adds, dels, file] = line.split("\t")
                  if (!file) return []
                  const binary = adds === "-" && dels === "-"
                  const additions = binary ? 0 : parseInt(adds)
                  const deletions = binary ? 0 : parseInt(dels)
                  return [
                    {
                      file,
                      status: status.get(file) ?? "modified",
                      binary,
                      additions: Number.isFinite(additions) ? additions : 0,
                      deletions: Number.isFinite(deletions) ? deletions : 0,
                    } satisfies Row,
                  ]
                })

              // Hide ignored-file removals from the user-facing diff output.
              const ignored = (yield* ignore(rows.map((r) => r.file))) ?? new Set<string>()
              if (ignored.size > 0) {
                const filtered = rows.filter((r) => !ignored.has(r.file))
                rows.length = 0
                rows.push(...filtered)
              }

              const step = 100
              const patch = (file: string, before: string, after: string) =>
                formatPatch(structuredPatch(file, file, before, after, "", "", { context: Number.MAX_SAFE_INTEGER }))

              for (let i = 0; i < rows.length; i += step) {
                const run = rows.slice(i, i + step)
                const text = yield* load(run)

                for (const row of run) {
                  const hit = text?.get(row.file) ?? { before: "", after: "" }
                  const [before, after] = row.binary ? ["", ""] : text ? [hit.before, hit.after] : yield* show(row)
                  result.push({
                    file: row.file,
                    patch: row.binary ? "" : patch(row.file, before, after),
                    additions: row.additions,
                    deletions: row.deletions,
                    status: row.status,
                  })
                }
              }

              return result
            }),
          )
        })

        return { track, patch, restore, revert, diff, diffFull }
      }),
    )

    return Service.of({
      init: Effect.fn("Snapshot.init")(function* () {
        yield* InstanceState.get(state)
      }),
      track: Effect.fn("Snapshot.track")(function* () {
        return yield* InstanceState.useEffect(state, (s) => s.track())
      }),
      patch: Effect.fn("Snapshot.patch")(function* (hash: string, to?: string) {
        return yield* InstanceState.useEffect(state, (s) => s.patch(hash, to))
      }),
      restore: Effect.fn("Snapshot.restore")(function* (snapshot: string) {
        return yield* InstanceState.useEffect(state, (s) => s.restore(snapshot))
      }),
      revert: Effect.fn("Snapshot.revert")(function* (patches: Patch[], restoreSnapshot?: string) {
        return yield* InstanceState.useEffect(state, (s) => s.revert(patches, restoreSnapshot))
      }),
      diff: Effect.fn("Snapshot.diff")(function* (hash: string) {
        return yield* InstanceState.useEffect(state, (s) => s.diff(hash))
      }),
      diffFull: Effect.fn("Snapshot.diffFull")(function* (from: string, to: string) {
        return yield* InstanceState.useEffect(state, (s) => s.diffFull(from, to))
      }),
    })
  }),
)

export const defaultLayer = layer.pipe(
  Layer.provide(CrossSpawnSpawner.defaultLayer),
  Layer.provide(AppFileSystem.defaultLayer),
  Layer.provide(Config.defaultLayer),
)

export * as Snapshot from "."
