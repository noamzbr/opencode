import { afterEach, expect } from "bun:test"
import { $ } from "bun"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import fs from "fs/promises"
import path from "path"
import { Effect, Fiber, Layer } from "effect"
import { Snapshot } from "../../src/snapshot"
import { InstanceState } from "../../src/effect/instance-state"
import { Global } from "@opencode-ai/core/global"
import { Hash } from "@opencode-ai/core/util/hash"
import { disposeAllInstances, provideInstance, TestInstance, tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(Layer.mergeAll(Snapshot.defaultLayer, AppFileSystem.defaultLayer))

// Git always outputs /-separated paths internally. Snapshot.patch() joins them
// with path.join (which produces \ on Windows) then normalizes back to /.
// This helper does the same for expected values so assertions match cross-platform.
const fwd = (...parts: string[]) => path.join(...parts).replaceAll("\\", "/")

afterEach(async () => {
  await disposeAllInstances()
})

const exec = (cwd: string, command: string[]) =>
  Effect.promise(async () => {
    const proc = Bun.spawn(command, { cwd, stdout: "ignore", stderr: "pipe" })
    const code = await proc.exited
    if (code !== 0) throw new Error(`${command.join(" ")} failed: ${await new Response(proc.stderr).text()}`)
  })

const write = (file: string, content: string | Uint8Array) =>
  AppFileSystem.Service.use((fs) => fs.writeWithDirs(file, content))
const readText = (file: string) => AppFileSystem.Service.use((fs) => fs.readFileString(file))
const exists = (file: string) => AppFileSystem.Service.use((fs) => fs.existsSafe(file))
const mkdirp = (dir: string) => AppFileSystem.Service.use((fs) => fs.ensureDir(dir))
const rm = (file: string) =>
  AppFileSystem.Service.use((fs) => fs.remove(file, { recursive: true, force: true }).pipe(Effect.ignore))

const initialize = Effect.fn("SnapshotTest.initialize")(function* (dir: string) {
  const unique = Math.random().toString(36).slice(2)
  const aContent = `A${unique}`
  const bContent = `B${unique}`
  yield* write(`${dir}/a.txt`, aContent)
  yield* write(`${dir}/b.txt`, bContent)
  yield* exec(dir, ["git", "add", "."])
  yield* exec(dir, ["git", "commit", "-m", "init"])
  return { aContent, bContent }
})

type Bootstrapped = { path: string; extra: { aContent: string; bContent: string } }

const bootstrap = Effect.fn("SnapshotTest.bootstrap")(function* () {
  const tmp = yield* TestInstance
  return { path: tmp.directory, extra: yield* initialize(tmp.directory) }
})

const withTrackedSnapshot = <A, E, R>(
  fn: (input: { tmp: Bootstrapped; snapshot: Snapshot.Interface; before: string }) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    return yield* fn({ tmp, snapshot, before: before! })
  })

const bootstrapScoped = Effect.fn("SnapshotTest.bootstrapScoped")(function* () {
  const dir = yield* tmpdirScoped({ git: true }).pipe(Effect.provide(CrossSpawnSpawner.defaultLayer))
  return { path: dir, extra: yield* initialize(dir) }
})

const scopedGitTmpdir = () => tmpdirScoped({ git: true }).pipe(Effect.provide(CrossSpawnSpawner.defaultLayer))

const cleanupWorktree = (repo: string, worktree: string, files: string[] = []) =>
  Effect.promise(async () => {
    await $`git worktree remove --force ${worktree}`.cwd(repo).quiet().nothrow()
    await fs.rm(worktree, { recursive: true, force: true }).catch(() => undefined)
    await Promise.all(files.map((file) => fs.rm(file, { recursive: true, force: true }).catch(() => undefined)))
  })

const withGitConfigGlobal = <A, E, R>(config: string, self: Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const previous = process.env.GIT_CONFIG_GLOBAL
      process.env.GIT_CONFIG_GLOBAL = config
      return previous
    }),
    () => self,
    (previous) =>
      Effect.sync(() => {
        if (previous) process.env.GIT_CONFIG_GLOBAL = previous
        else delete process.env.GIT_CONFIG_GLOBAL
      }),
  )

it.instance(
  "tracks deleted files correctly",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      expect((yield* snapshot.patch(before)).files).toContain(fwd(tmp.path, "a.txt"))
    }),
  ),
  { git: true },
)

it.instance(
  "revert should remove new files",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/new.txt`, "NEW")
      const patch = yield* snapshot.patch(before)
      yield* snapshot.revert([patch])
      expect(yield* exists(`${tmp.path}/new.txt`)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "revert in subdirectory",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/sub`)
      yield* write(`${tmp.path}/sub/file.txt`, "SUB")
      const patch = yield* snapshot.patch(before)
      yield* snapshot.revert([patch])
      expect(yield* exists(`${tmp.path}/sub/file.txt`)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "multiple file operations",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      yield* write(`${tmp.path}/c.txt`, "C")
      yield* mkdirp(`${tmp.path}/dir`)
      yield* write(`${tmp.path}/dir/d.txt`, "D")
      yield* write(`${tmp.path}/b.txt`, "MODIFIED")
      const patch = yield* snapshot.patch(before)
      yield* snapshot.revert([patch])
      expect(yield* readText(`${tmp.path}/a.txt`)).toBe(tmp.extra.aContent)
      expect(yield* exists(`${tmp.path}/c.txt`)).toBe(false)
      expect(yield* readText(`${tmp.path}/b.txt`)).toBe(tmp.extra.bContent)
    }),
  ),
  { git: true },
)

it.instance(
  "empty directory handling",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/empty`)
      expect((yield* snapshot.patch(before)).files.length).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "binary file handling",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/image.png`, new Uint8Array([0x89, 0x50, 0x4e, 0x47]))
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, "image.png"))
      yield* snapshot.revert([patch])
      expect(yield* exists(`${tmp.path}/image.png`)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "symlink handling",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* Effect.promise(() => fs.symlink(`${tmp.path}/a.txt`, `${tmp.path}/link.txt`, "file"))
      expect((yield* snapshot.patch(before)).files).toContain(fwd(tmp.path, "link.txt"))
    }),
  ),
  { git: true },
)

it.instance(
  "revert accepts gitlinks whose commits are outside the snapshot store",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const module = path.join(tmp.path, "module")
    yield* mkdirp(module)
    yield* exec(module, ["git", "init"])
    yield* exec(module, ["git", "config", "user.email", "test@example.com"])
    yield* exec(module, ["git", "config", "user.name", "Test"])
    yield* write(path.join(module, "file.txt"), "before")
    yield* exec(module, ["git", "add", "."])
    yield* exec(module, ["git", "commit", "-m", "before"])
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()

    yield* write(path.join(module, "file.txt"), "after")
    yield* exec(module, ["git", "add", "."])
    yield* exec(module, ["git", "commit", "-m", "after"])
    const patch = yield* snapshot.patch(before!)
    expect(patch.files).toEqual([fwd(module)])
    yield* snapshot.revert([patch])
  }),
  { git: true },
)

it.instance(
  "file under size limit handling",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/large.txt`, "x".repeat(1024 * 1024))
      expect((yield* snapshot.patch(before)).files).toContain(fwd(tmp.path, "large.txt"))
    }),
  ),
  { git: true },
)

it.instance(
  "large added files are skipped",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/huge.txt`, new Uint8Array(2 * 1024 * 1024 + 1))
      expect((yield* snapshot.patch(before)).files).toEqual([])
      expect(yield* snapshot.diff(before)).toBe("")
      expect(yield* snapshot.track()).toBe(before)
    }),
  ),
  { git: true },
)

it.instance(
  "nested directory revert",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/level1/level2/level3`)
      yield* write(`${tmp.path}/level1/level2/level3/deep.txt`, "DEEP")
      const patch = yield* snapshot.patch(before)
      yield* snapshot.revert([patch])
      expect(yield* exists(`${tmp.path}/level1/level2/level3/deep.txt`)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "special characters in filenames",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/file with spaces.txt`, "SPACES")
      yield* write(`${tmp.path}/file-with-dashes.txt`, "DASHES")
      yield* write(`${tmp.path}/file_with_underscores.txt`, "UNDERSCORES")
      const files = (yield* snapshot.patch(before)).files
      expect(files).toContain(fwd(tmp.path, "file with spaces.txt"))
      expect(files).toContain(fwd(tmp.path, "file-with-dashes.txt"))
      expect(files).toContain(fwd(tmp.path, "file_with_underscores.txt"))
    }),
  ),
  { git: true },
)

it.instance(
  "revert with empty patches",
  Effect.gen(function* () {
    yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* snapshot.revert([])
    yield* snapshot.revert([{ hash: "dummy", files: [] }])
  }),
  { git: true },
)

it.instance(
  "patch with invalid hash",
  withTrackedSnapshot(({ tmp, snapshot }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/test.txt`, "TEST")
      const patch = yield* snapshot.patch("invalid-hash-12345")
      expect(patch.files).toEqual([])
      expect(patch.hash).toBe("invalid-hash-12345")
    }),
  ),
  { git: true },
)

it.instance(
  "revert non-existent file",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* snapshot.revert([{ hash: before, files: [`${tmp.path}/nonexistent.txt`] }])
    }),
  ),
  { git: true },
)

it.instance(
  "unicode filenames",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      const unicodeFiles = [
        { path: fwd(tmp.path, "文件.txt"), content: "chinese content" },
        { path: fwd(tmp.path, "🚀rocket.txt"), content: "emoji content" },
        { path: fwd(tmp.path, "café.txt"), content: "accented content" },
        { path: fwd(tmp.path, "файл.txt"), content: "cyrillic content" },
      ]
      yield* Effect.all(
        unicodeFiles.map((file) => write(file.path, file.content)),
        { concurrency: "unbounded" },
      )
      const patch = yield* snapshot.patch(before)
      expect(patch.files.length).toBe(4)
      for (const file of unicodeFiles) expect(patch.files).toContain(file.path)
      yield* snapshot.revert([patch])
      for (const file of unicodeFiles) expect(yield* exists(file.path)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance.skip(
  "unicode filenames modification and restore",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const chineseFile = fwd(tmp.path, "文件.txt")
    const cyrillicFile = fwd(tmp.path, "файл.txt")
    yield* write(chineseFile, "original chinese")
    yield* write(cyrillicFile, "original cyrillic")
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* write(chineseFile, "modified chinese")
    yield* write(cyrillicFile, "modified cyrillic")
    const patch = yield* snapshot.patch(before!)
    expect(patch.files).toContain(chineseFile)
    expect(patch.files).toContain(cyrillicFile)
    yield* snapshot.revert([patch])
    expect(yield* readText(chineseFile)).toBe("original chinese")
    expect(yield* readText(cyrillicFile)).toBe("original cyrillic")
  }),
  { git: true },
)

it.instance(
  "unicode filenames in subdirectories",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/目录/подкаталог`)
      const deepFile = fwd(tmp.path, "目录", "подкаталог", "文件.txt")
      yield* write(deepFile, "deep unicode content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(deepFile)
      yield* snapshot.revert([patch])
      expect(yield* exists(deepFile)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "very long filenames",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      const longFile = fwd(tmp.path, `${"a".repeat(200)}.txt`)
      yield* write(longFile, "long filename content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(longFile)
      yield* snapshot.revert([patch])
      expect(yield* exists(longFile)).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "hidden files",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/.hidden`, "hidden content")
      yield* write(`${tmp.path}/.gitignore`, "*.log")
      yield* write(`${tmp.path}/.config`, "config content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, ".hidden"))
      expect(patch.files).toContain(fwd(tmp.path, ".gitignore"))
      expect(patch.files).toContain(fwd(tmp.path, ".config"))
    }),
  ),
  { git: true },
)

it.instance(
  "nested symlinks",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* mkdirp(`${tmp.path}/sub/dir`)
      yield* write(`${tmp.path}/sub/dir/target.txt`, "target content")
      yield* Effect.promise(() => fs.symlink(`${tmp.path}/sub/dir/target.txt`, `${tmp.path}/sub/dir/link.txt`, "file"))
      yield* Effect.promise(() => fs.symlink(`${tmp.path}/sub`, `${tmp.path}/sub-link`, "dir"))
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, "sub", "dir", "link.txt"))
      expect(patch.files).toContain(fwd(tmp.path, "sub-link"))
    }),
  ),
  { git: true },
)

it.instance(
  "file permissions and ownership changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* Effect.promise(() => fs.chmod(`${tmp.path}/a.txt`, 0o600))
      yield* Effect.promise(() => fs.chmod(`${tmp.path}/a.txt`, 0o755))
      yield* Effect.promise(() => fs.chmod(`${tmp.path}/a.txt`, 0o644))
      expect((yield* snapshot.patch(before)).files.length).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "circular symlinks",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* Effect.promise(() =>
        fs.symlink(`${tmp.path}/circular`, `${tmp.path}/circular`, "dir").catch(() => undefined),
      )
      expect((yield* snapshot.patch(before)).files.length).toBeGreaterThanOrEqual(0)
    }),
  ),
  { git: true },
)

it.live(
  "source project gitignore is respected - ignored files are not snapshotted",
  Effect.gen(function* () {
    const dir = yield* scopedGitTmpdir()
    yield* write(`${dir}/.gitignore`, "*.ignored\nbuild/\nnode_modules/\n")
    yield* write(`${dir}/tracked.txt`, "tracked content")
    yield* write(`${dir}/ignored.ignored`, "ignored content")
    yield* mkdirp(`${dir}/build`)
    yield* write(`${dir}/build/output.js`, "build output")
    yield* write(`${dir}/normal.js`, "normal js")
    yield* exec(dir, ["git", "add", "."])
    yield* exec(dir, ["git", "commit", "-m", "init"])
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      yield* write(`${dir}/tracked.txt`, "modified tracked")
      yield* write(`${dir}/new.ignored`, "new ignored")
      yield* write(`${dir}/new-tracked.txt`, "new tracked")
      yield* write(`${dir}/build/new-build.js`, "new build file")
      const patch = yield* snapshot.patch(before!)
      expect(patch.files).toContain(fwd(dir, "new-tracked.txt"))
      expect(patch.files).toContain(fwd(dir, "tracked.txt"))
      expect(patch.files).not.toContain(fwd(dir, "new.ignored"))
      expect(patch.files).not.toContain(fwd(dir, "ignored.ignored"))
      expect(patch.files).not.toContain(fwd(dir, "build/output.js"))
      expect(patch.files).not.toContain(fwd(dir, "build/new-build.js"))
    }).pipe(provideInstance(dir))
  }),
)

it.instance(
  "gitignore changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/.gitignore`, "*.ignored")
      yield* write(`${tmp.path}/test.ignored`, "ignored content")
      yield* write(`${tmp.path}/normal.txt`, "normal content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, ".gitignore"))
      expect(patch.files).toContain(fwd(tmp.path, "normal.txt"))
      expect(patch.files).not.toContain(fwd(tmp.path, "test.ignored"))
    }),
  ),
  { git: true },
)

it.instance(
  "files tracked in snapshot but now gitignored are filtered out",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/later-ignored.txt`, "initial content")
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* write(`${tmp.path}/later-ignored.txt`, "modified content")
    yield* write(`${tmp.path}/.gitignore`, "later-ignored.txt\n")
    yield* write(`${tmp.path}/still-tracked.txt`, "new tracked file")
    const patch = yield* snapshot.patch(before!)
    expect(patch.files).not.toContain(fwd(tmp.path, "later-ignored.txt"))
    expect(patch.files).toContain(fwd(tmp.path, ".gitignore"))
    expect(patch.files).toContain(fwd(tmp.path, "still-tracked.txt"))
  }),
  { git: true },
)

it.live(
  "concurrent sibling snapshots share objects but isolate trees",
  Effect.gen(function* () {
    const dir = yield* scopedGitTmpdir()
    const first = `${dir}/.sessions/s1/workspaces/w1/first`
    const second = `${dir}/.sessions/s2/workspaces/w1/second`
    yield* mkdirp(first)
    yield* mkdirp(second)
    yield* write(`${first}/flow.script.yaml`, "first: baseline\n")
    yield* write(`${second}/flow.script.yaml`, "second: baseline\n")
    yield* exec(dir, ["git", "add", "."])
    yield* exec(dir, ["git", "commit", "-m", "add sibling sessions"])

    const capture = (directory: string) =>
      Effect.gen(function* () {
        const snapshot = yield* Snapshot.Service
        const hash = yield* snapshot.track()
        expect(hash).toBeTruthy()
        return hash!
      }).pipe(provideInstance(directory))

    const [beforeFirst, beforeSecond] = yield* Effect.all([capture(first), capture(second)], { concurrency: 2 })
    const ctx = yield* InstanceState.context.pipe(provideInstance(first))
    const projectRoot = path.join(Global.Path.data, "snapshot", ctx.project.id)
    const gitdir = path.join(projectRoot, Hash.fast(ctx.worktree))
    expect((yield* Effect.promise(() => fs.readdir(projectRoot))).sort()).toEqual([Hash.fast(ctx.worktree)])

    const legacy = yield* Effect.promise(async () => {
      const env = { ...process.env, GIT_INDEX_FILE: path.join(gitdir, "index") }
      const run = async (args: string[]) => {
        const proc = Bun.spawn(["git", "--git-dir", gitdir, "--work-tree", dir, ...args], {
          cwd: dir,
          env,
          stdout: "pipe",
          stderr: "pipe",
        })
        const [code, stdout, stderr] = await Promise.all([
          proc.exited,
          new Response(proc.stdout).text(),
          new Response(proc.stderr).text(),
        ])
        if (code !== 0) throw new Error(stderr)
        return stdout.trim()
      }
      await run(["read-tree", "--empty"])
      await run(["add", "--", path.relative(dir, first), path.relative(dir, second)])
      const hash = await run(["write-tree"])
      await fs.writeFile(path.join(gitdir, "index.lock"), "legacy")
      await fs.mkdir(path.join(gitdir, "info"), { recursive: true })
      await fs.writeFile(path.join(gitdir, "info", "exclude"), "legacy.tmp\n")
      return hash
    })

    yield* write(`${first}/flow.script.yaml`, "first: changed\n")
    yield* write(`${second}/flow.script.yaml`, "second: changed\n")
    const changed = (directory: string, before: string) =>
      Effect.gen(function* () {
        const snapshot = yield* Snapshot.Service
        const after = yield* snapshot.track()
        if (!after) throw new Error("expected snapshot")
        return yield* snapshot.patch(before, after)
      }).pipe(provideInstance(directory))
    const [firstPatch, secondPatch] = yield* Effect.all([changed(first, beforeFirst), changed(second, beforeSecond)], {
      concurrency: 2,
    })

    expect(firstPatch.files).toEqual([fwd(first, "flow.script.yaml")])
    expect(secondPatch.files).toEqual([fwd(second, "flow.script.yaml")])
    expect(yield* exists(path.join(gitdir, "index"))).toBe(false)
    expect(yield* exists(path.join(gitdir, "index.lock"))).toBe(false)
    expect(yield* exists(path.join(gitdir, "info", "exclude"))).toBe(false)
    expect(yield* Effect.promise(() => fs.readdir(path.join(gitdir, "tmp")))).toEqual([])

    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      yield* snapshot.revert([firstPatch])
    }).pipe(provideInstance(first))
    expect(yield* readText(`${first}/flow.script.yaml`)).toBe("first: baseline\n")
    expect(yield* readText(`${second}/flow.script.yaml`)).toBe("second: changed\n")

    yield* write(`${first}/flow.script.yaml`, "first: latest\n")
    yield* write(`${second}/flow.script.yaml`, "second: latest\n")
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      yield* snapshot.restore(legacy)
    }).pipe(provideInstance(first))
    expect(yield* readText(`${first}/flow.script.yaml`)).toBe("first: baseline\n")
    expect(yield* readText(`${second}/flow.script.yaml`)).toBe("second: latest\n")

    yield* Effect.promise(async () => {
      await fs.rm(path.join(gitdir, "tmp"), { recursive: true, force: true })
      await fs.writeFile(path.join(gitdir, "tmp"), "blocked")
    })
    const failed = yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      return yield* snapshot.restore(legacy).pipe(Effect.flip)
    }).pipe(provideInstance(first))
    expect(failed).toBeInstanceOf(Snapshot.Error)
  }),
)

it.instance(
  "gitignore updated between track calls filters from diff",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/a.txt`, "modified content")
      yield* write(`${tmp.path}/.gitignore`, "a.txt\n")
      yield* write(`${tmp.path}/b.txt`, "also modified")
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.some((x) => x.file === "a.txt")).toBe(false)
      expect(diffs.some((x) => x.file === ".gitignore")).toBe(true)
      expect(diffs.some((x) => x.file === "b.txt")).toBe(true)
    }),
  ),
  { git: true },
)

it.instance(
  "git info exclude changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      const file = `${tmp.path}/.git/info/exclude`
      yield* write(file, `${(yield* Effect.promise(() => Bun.file(file).text())).trimEnd()}\nignored.txt\n`)
      yield* write(`${tmp.path}/ignored.txt`, "ignored content")
      yield* write(`${tmp.path}/normal.txt`, "normal content")
      const patch = yield* snapshot.patch(before)
      expect(patch.files).toContain(fwd(tmp.path, "normal.txt"))
      expect(patch.files).not.toContain(fwd(tmp.path, "ignored.txt"))
      const after = yield* snapshot.track()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.some((x) => x.file === "normal.txt")).toBe(true)
      expect(diffs.some((x) => x.file === "ignored.txt")).toBe(false)
    }),
  ),
  { git: true },
)

it.instance(
  "git info exclude keeps global excludes",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const global = `${tmp.path}/global.ignore`
    const config = `${tmp.path}/global.gitconfig`
    yield* write(global, "global.tmp\n")
    yield* write(config, `[core]\n\texcludesFile = ${global.replaceAll("\\", "/")}\n`)
    yield* withGitConfigGlobal(
      config,
      Effect.gen(function* () {
        const snapshot = yield* Snapshot.Service
        const before = yield* snapshot.track()
        expect(before).toBeTruthy()
        const file = `${tmp.path}/.git/info/exclude`
        yield* write(file, `${(yield* Effect.promise(() => Bun.file(file).text())).trimEnd()}\ninfo.tmp\n`)
        yield* write(`${tmp.path}/global.tmp`, "global content")
        yield* write(`${tmp.path}/info.tmp`, "info content")
        yield* write(`${tmp.path}/normal.txt`, "normal content")
        const patch = yield* snapshot.patch(before!)
        expect(patch.files).toContain(fwd(tmp.path, "normal.txt"))
        expect(patch.files).not.toContain(fwd(tmp.path, "global.tmp"))
        expect(patch.files).not.toContain(fwd(tmp.path, "info.tmp"))
      }),
    )
  }),
  { git: true },
)

it.instance(
  "concurrent file operations during patch",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      const fiber = yield* Effect.gen(function* () {
        for (let i = 0; i < 10; i++) {
          yield* write(`${tmp.path}/concurrent${i}.txt`, `concurrent${i}`)
          yield* Effect.sleep("1 millis")
        }
      }).pipe(Effect.forkScoped)
      const patch = yield* snapshot.patch(before)
      yield* Fiber.join(fiber)
      expect(patch.files.length).toBeGreaterThanOrEqual(0)
    }),
  ),
  { git: true },
)

it.live(
  "snapshot state isolation between projects",
  Effect.gen(function* () {
    const tmp1 = yield* bootstrapScoped()
    const tmp2 = yield* bootstrapScoped()
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before1 = yield* snapshot.track()
      yield* write(`${tmp1.path}/project1.txt`, "project1 content")
      const patch1 = yield* snapshot.patch(before1!)
      expect(patch1.files).toContain(fwd(tmp1.path, "project1.txt"))
    }).pipe(provideInstance(tmp1.path))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before2 = yield* snapshot.track()
      yield* write(`${tmp2.path}/project2.txt`, "project2 content")
      const patch2 = yield* snapshot.patch(before2!)
      expect(patch2.files).toContain(fwd(tmp2.path, "project2.txt"))
      expect(patch2.files).not.toContain(fwd(tmp1.path, "project1.txt"))
    }).pipe(provideInstance(tmp2.path))
  }),
)

it.live(
  "patch detects changes in secondary worktree",
  Effect.gen(function* () {
    const tmp = yield* bootstrapScoped()
    const worktreePath = `${tmp.path}-worktree`
    yield* exec(tmp.path, ["git", "worktree", "add", worktreePath, "HEAD"])
    yield* Effect.addFinalizer(() => cleanupWorktree(tmp.path, worktreePath))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      expect(yield* snapshot.track()).toBeTruthy()
    }).pipe(provideInstance(tmp.path))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      const worktreeFile = fwd(worktreePath, "worktree.txt")
      yield* write(worktreeFile, "worktree content")
      expect((yield* snapshot.patch(before!)).files).toContain(worktreeFile)
    }).pipe(provideInstance(worktreePath))
  }),
)

it.live(
  "revert only removes files in invoking worktree",
  Effect.gen(function* () {
    const tmp = yield* bootstrapScoped()
    const worktreePath = `${tmp.path}-worktree`
    const primaryFile = `${tmp.path}/worktree.txt`
    yield* exec(tmp.path, ["git", "worktree", "add", worktreePath, "HEAD"])
    yield* Effect.addFinalizer(() => cleanupWorktree(tmp.path, worktreePath, [primaryFile]))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      expect(yield* snapshot.track()).toBeTruthy()
    }).pipe(provideInstance(tmp.path))
    yield* write(primaryFile, "primary content")
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      const worktreeFile = fwd(worktreePath, "worktree.txt")
      yield* write(worktreeFile, "worktree content")
      const patch = yield* snapshot.patch(before!)
      yield* snapshot.revert([patch])
      expect(yield* exists(worktreeFile)).toBe(false)
    }).pipe(provideInstance(worktreePath))
    expect(yield* readText(primaryFile)).toBe("primary content")
  }),
)

it.live(
  "diff reports worktree-only/shared edits and ignores primary-only",
  Effect.gen(function* () {
    const tmp = yield* bootstrapScoped()
    const worktreePath = `${tmp.path}-worktree`
    yield* exec(tmp.path, ["git", "worktree", "add", worktreePath, "HEAD"])
    yield* Effect.addFinalizer(() =>
      cleanupWorktree(tmp.path, worktreePath, [`${tmp.path}/shared.txt`, `${tmp.path}/primary-only.txt`]),
    )
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      expect(yield* snapshot.track()).toBeTruthy()
    }).pipe(provideInstance(tmp.path))
    yield* Effect.gen(function* () {
      const snapshot = yield* Snapshot.Service
      const before = yield* snapshot.track()
      expect(before).toBeTruthy()
      yield* write(`${worktreePath}/worktree-only.txt`, "worktree diff content")
      yield* write(`${worktreePath}/shared.txt`, "worktree edit")
      yield* write(`${tmp.path}/shared.txt`, "primary edit")
      yield* write(`${tmp.path}/primary-only.txt`, "primary change")
      const diff = yield* snapshot.diff(before!)
      expect(diff).toContain("worktree-only.txt")
      expect(diff).toContain("shared.txt")
      expect(diff).not.toContain("primary-only.txt")
    }).pipe(provideInstance(worktreePath))
  }),
)

it.instance(
  "track reuses and refreshes the cached index",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/.gitattributes`, "a.txt filter=fail\n")
    yield* Effect.promise(() => fs.utimes(`${tmp.path}/a.txt`, new Date(1_000), new Date(1_000)))
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    const ctx = yield* InstanceState.context
    const gitdir = path.join(Global.Path.data, "snapshot", ctx.project.id, Hash.fast(ctx.worktree))
    for (const [key, value] of [
      ["filter.fail.clean", "git definitely-missing-command"],
      ["filter.fail.required", "true"],
    ]) {
      yield* exec(tmp.path, ["git", "--git-dir", gitdir, "config", key, value])
    }
    expect(yield* snapshot.track()).toBe(before)
    expect(yield* snapshot.track()).toBe(before)

    yield* exec(tmp.path, ["git", "--git-dir", gitdir, "config", "filter.fail.clean", "cat"])
    yield* write(`${tmp.path}/a.txt`, "changed")
    const after = yield* snapshot.track()
    expect(after).toBeTruthy()
    expect(after).not.toBe(before)

    // The current daily ref and its reflog retain every distinct tree captured
    // in the bucket, including trees that are no longer the ref's current value.
    yield* exec(tmp.path, ["git", "--git-dir", gitdir, "gc", "--prune=now"])
    yield* exec(tmp.path, ["git", "--git-dir", gitdir, "cat-file", "-e", `${before}^{tree}`])
    yield* exec(tmp.path, ["git", "--git-dir", gitdir, "cat-file", "-e", `${after}^{tree}`])

    const ref = `refs/opencode/snapshot/${Math.floor(Date.now() / (24 * 60 * 60 * 1000))}`
    yield* exec(tmp.path, ["git", "--git-dir", gitdir, "update-ref", "-d", ref])
    yield* exec(tmp.path, ["git", "--git-dir", gitdir, "gc", "--prune=now"])
    const missing = yield* Effect.promise(async () => {
      const proc = Bun.spawn(["git", "--git-dir", gitdir, "cat-file", "-e", `${after}^{tree}`], {
        cwd: tmp.path,
        stdout: "ignore",
        stderr: "ignore",
      })
      return await proc.exited
    })
    expect(missing).not.toBe(0)

    const now = Date.now
    const refreshed = yield* Effect.acquireUseRelease(
      Effect.sync(() => {
        Date.now = () => now() + 7 * 24 * 60 * 60 * 1000
      }),
      () => snapshot.track(),
      () =>
        Effect.sync(() => {
          Date.now = now
      }),
    )
    expect(refreshed).toBe(after)
    yield* exec(tmp.path, ["git", "--git-dir", gitdir, "cat-file", "-e", `${refreshed}^{tree}`])
    yield* exec(tmp.path, ["git", "--git-dir", gitdir, "cat-file", "-e", `${refreshed}:a.txt`])
  }),
  { git: true },
)

it.instance(
  "diff function with various changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      yield* write(`${tmp.path}/new.txt`, "new content")
      yield* write(`${tmp.path}/b.txt`, "modified content")
      const diff = yield* snapshot.diff(before)
      expect(diff).toContain("a.txt")
      expect(diff).toContain("b.txt")
      expect(diff).toContain("new.txt")
    }),
  ),
  { git: true },
)

it.instance(
  "restore function",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      yield* write(`${tmp.path}/new.txt`, "new content")
      yield* write(`${tmp.path}/b.txt`, "modified")
      yield* snapshot.restore(before)
      expect(yield* exists(`${tmp.path}/a.txt`)).toBe(true)
      expect(yield* readText(`${tmp.path}/a.txt`)).toBe(tmp.extra.aContent)
      expect(yield* exists(`${tmp.path}/new.txt`)).toBe(true)
      expect(yield* readText(`${tmp.path}/b.txt`)).toBe(tmp.extra.bContent)
    }),
  ),
  { git: true },
)

it.instance(
  "restore preserves LF with global autocrlf enabled",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const config = `${tmp.path}/global.gitconfig`
    yield* write(config, "[core]\n\tautocrlf = true\n")
    yield* withGitConfigGlobal(
      config,
      Effect.gen(function* () {
        const snapshot = yield* Snapshot.Service
        const file = `${tmp.path}/lines.txt`
        yield* write(file, "one\ntwo\n")
        const before = yield* snapshot.track()
        expect(before).toBeTruthy()
        yield* write(file, "changed\n")
        yield* snapshot.restore(before!)
        expect(yield* readText(file)).toBe("one\ntwo\n")
      }),
    )
  }),
  { git: true },
)

it.instance(
  "revert should not delete files that existed but were deleted in snapshot",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const snapshot1 = yield* snapshot.track()
    expect(snapshot1).toBeTruthy()
    yield* rm(`${tmp.path}/a.txt`)
    const snapshot2 = yield* snapshot.track()
    expect(snapshot2).toBeTruthy()
    yield* write(`${tmp.path}/a.txt`, "recreated content")
    const patch = yield* snapshot.patch(snapshot2!)
    expect(patch.files).toContain(fwd(tmp.path, "a.txt"))
    yield* snapshot.revert([patch])
    expect(yield* exists(`${tmp.path}/a.txt`)).toBe(false)
  }),
  { git: true },
)

it.instance(
  "revert preserves file that existed in snapshot when deleted then recreated",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/existing.txt`, "original content")
    const hash = yield* snapshot.track()
    expect(hash).toBeTruthy()
    yield* rm(`${tmp.path}/existing.txt`)
    yield* write(`${tmp.path}/existing.txt`, "recreated")
    yield* write(`${tmp.path}/newfile.txt`, "new")
    const patch = yield* snapshot.patch(hash!)
    expect(patch.files).toContain(fwd(tmp.path, "existing.txt"))
    expect(patch.files).toContain(fwd(tmp.path, "newfile.txt"))
    yield* snapshot.revert([patch])
    expect(yield* exists(`${tmp.path}/newfile.txt`)).toBe(false)
    expect(yield* exists(`${tmp.path}/existing.txt`)).toBe(true)
    expect(yield* readText(`${tmp.path}/existing.txt`)).toBe("original content")
  }),
  { git: true },
)

it.instance(
  "diffFull sets status based on git change type",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/grow.txt`, "one\n")
    yield* write(`${tmp.path}/trim.txt`, "line1\nline2\n")
    yield* write(`${tmp.path}/delete.txt`, "gone")
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* write(`${tmp.path}/grow.txt`, "one\ntwo\n")
    yield* write(`${tmp.path}/trim.txt`, "line1\n")
    yield* rm(`${tmp.path}/delete.txt`)
    yield* write(`${tmp.path}/added.txt`, "new")
    const after = yield* snapshot.track()
    expect(after).toBeTruthy()
    const diffs = yield* snapshot.diffFull(before!, after!)
    expect(diffs.length).toBe(4)
    expect(diffs.find((d) => d.file === "added.txt")!.status).toBe("added")
    expect(diffs.find((d) => d.file === "delete.txt")!.status).toBe("deleted")
    const grow = diffs.find((d) => d.file === "grow.txt")!
    expect(grow.status).toBe("modified")
    expect(grow.additions).toBeGreaterThan(0)
    expect(grow.deletions).toBe(0)
    const trim = diffs.find((d) => d.file === "trim.txt")!
    expect(trim.status).toBe("modified")
    expect(trim.additions).toBe(0)
    expect(trim.deletions).toBeGreaterThan(0)
  }),
  { git: true },
)

it.instance(
  "diffFull with new file additions",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/new.txt`, "new content")
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("new.txt")
      expect(diffs[0].patch).toContain("+new content")
      expect(diffs[0].additions).toBe(1)
      expect(diffs[0].deletions).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with a large interleaved mixed diff",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const ids = Array.from({ length: 60 }, (_, i) => i.toString().padStart(3, "0"))
    const mod = ids.map((id) => fwd(tmp.path, "mix", `${id}-mod.txt`))
    const del = ids.map((id) => fwd(tmp.path, "mix", `${id}-del.txt`))
    const add = ids.map((id) => fwd(tmp.path, "mix", `${id}-add.txt`))
    const bin = ids.map((id) => fwd(tmp.path, "mix", `${id}-bin.bin`))
    yield* mkdirp(`${tmp.path}/mix`)
    yield* Effect.all(
      [
        ...mod.map((file, i) => write(file, `before-${ids[i]}-é\n🙂\nline`)),
        ...del.map((file, i) => write(file, `gone-${ids[i]}\n你好`)),
        ...bin.map((file, i) => write(file, new Uint8Array([0, i, 255, i % 251]))),
      ],
      { concurrency: "unbounded" },
    )
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* Effect.all(
      [
        ...mod.map((file, i) => write(file, `after-${ids[i]}-é\n🚀\nline`)),
        ...add.map((file, i) => write(file, `new-${ids[i]}\nこんにちは`)),
        ...bin.map((file, i) => write(file, new Uint8Array([9, i, 8, i % 251]))),
        ...del.map((file) => rm(file)),
      ],
      { concurrency: "unbounded" },
    )
    const after = yield* snapshot.track()
    expect(after).toBeTruthy()
    const diffs = yield* snapshot.diffFull(before!, after!)
    expect(diffs).toHaveLength(ids.length * 4)
    const map = new Map(diffs.map((item) => [item.file, item]))
    for (let i = 0; i < ids.length; i++) {
      const m = map.get(fwd("mix", `${ids[i]}-mod.txt`))
      expect(m).toBeDefined()
      expect(m!.patch).toContain(`-before-${ids[i]}-é`)
      expect(m!.patch).toContain(`+after-${ids[i]}-é`)
      expect(m!.status).toBe("modified")
      const d = map.get(fwd("mix", `${ids[i]}-del.txt`))
      expect(d).toBeDefined()
      expect(d!.patch).toContain(`-gone-${ids[i]}`)
      expect(d!.status).toBe("deleted")
      const a = map.get(fwd("mix", `${ids[i]}-add.txt`))
      expect(a).toBeDefined()
      expect(a!.patch).toContain(`+new-${ids[i]}`)
      expect(a!.status).toBe("added")
      const b = map.get(fwd("mix", `${ids[i]}-bin.bin`))
      expect(b).toBeDefined()
      expect(b!.patch).toBe("")
      expect(b!.additions).toBe(0)
      expect(b!.deletions).toBe(0)
      expect(b!.status).toBe("modified")
    }
  }),
  { git: true },
)

it.instance(
  "diffFull preserves git diff order across batch boundaries",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const ids = Array.from({ length: 140 }, (_, i) => i.toString().padStart(3, "0"))
    yield* mkdirp(`${tmp.path}/order`)
    yield* Effect.all(
      ids.map((id) => write(`${tmp.path}/order/${id}.txt`, `before-${id}`)),
      { concurrency: "unbounded" },
    )
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* Effect.all(
      ids.map((id) => write(`${tmp.path}/order/${id}.txt`, `after-${id}`)),
      { concurrency: "unbounded" },
    )
    const after = yield* snapshot.track()
    expect(after).toBeTruthy()
    expect((yield* snapshot.diffFull(before!, after!)).map((item) => item.file)).toEqual(
      ids.map((id) => `order/${id}.txt`),
    )
  }),
  { git: true },
)

it.instance(
  "diffFull with file modifications",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/b.txt`, "modified content")
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("b.txt")
      expect(diffs[0].patch).toContain(`-${tmp.extra.bContent}`)
      expect(diffs[0].patch).toContain("+modified content")
      expect(diffs[0].additions).toBeGreaterThan(0)
      expect(diffs[0].deletions).toBeGreaterThan(0)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with file deletions",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* rm(`${tmp.path}/a.txt`)
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("a.txt")
      expect(diffs[0].patch).toContain(`-${tmp.extra.aContent}`)
      expect(diffs[0].additions).toBe(0)
      expect(diffs[0].deletions).toBe(1)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with multiple line additions",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/multi.txt`, "line1\nline2\nline3")
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("multi.txt")
      expect(diffs[0].patch).toContain("+line1")
      expect(diffs[0].patch).toContain("+line3")
      expect(diffs[0].additions).toBe(3)
      expect(diffs[0].deletions).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with addition and deletion",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/added.txt`, "added content")
      yield* rm(`${tmp.path}/a.txt`)
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(2)
      const added = diffs.find((d) => d.file === "added.txt")!
      expect(added.patch).toContain("+added content")
      expect(added.additions).toBe(1)
      expect(added.deletions).toBe(0)
      const removed = diffs.find((d) => d.file === "a.txt")!
      expect(removed.patch).toContain(`-${tmp.extra.aContent}`)
      expect(removed.additions).toBe(0)
      expect(removed.deletions).toBe(1)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with multiple additions and deletions",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/multi1.txt`, "line1\nline2\nline3")
      yield* write(`${tmp.path}/multi2.txt`, "single line")
      yield* rm(`${tmp.path}/a.txt`)
      yield* rm(`${tmp.path}/b.txt`)
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(4)
      expect(diffs.find((d) => d.file === "multi1.txt")!.additions).toBe(3)
      expect(diffs.find((d) => d.file === "multi1.txt")!.deletions).toBe(0)
      expect(diffs.find((d) => d.file === "multi2.txt")!.additions).toBe(1)
      expect(diffs.find((d) => d.file === "multi2.txt")!.deletions).toBe(0)
      expect(diffs.find((d) => d.file === "a.txt")!.additions).toBe(0)
      expect(diffs.find((d) => d.file === "a.txt")!.deletions).toBe(1)
      expect(diffs.find((d) => d.file === "b.txt")!.additions).toBe(0)
      expect(diffs.find((d) => d.file === "b.txt")!.deletions).toBe(1)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with no changes",
  withTrackedSnapshot(({ snapshot, before }) =>
    Effect.gen(function* () {
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      expect((yield* snapshot.diffFull(before, after!)).length).toBe(0)
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with binary file changes",
  withTrackedSnapshot(({ tmp, snapshot, before }) =>
    Effect.gen(function* () {
      yield* write(`${tmp.path}/binary.bin`, new Uint8Array([0x00, 0x01, 0x02, 0x03]))
      const after = yield* snapshot.track()
      expect(after).toBeTruthy()
      const diffs = yield* snapshot.diffFull(before, after!)
      expect(diffs.length).toBe(1)
      expect(diffs[0].file).toBe("binary.bin")
      expect(diffs[0].patch).toBe("")
    }),
  ),
  { git: true },
)

it.instance(
  "diffFull with whitespace changes",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/whitespace.txt`, "line1\nline2")
    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* write(`${tmp.path}/whitespace.txt`, "line1\n\nline2\n")
    const after = yield* snapshot.track()
    expect(after).toBeTruthy()
    const diffs = yield* snapshot.diffFull(before!, after!)
    expect(diffs.length).toBe(1)
    expect(diffs[0].file).toBe("whitespace.txt")
    expect(diffs[0].additions).toBeGreaterThan(0)
  }),
  { git: true },
)

it.instance(
  "revert with overlapping files across patches uses first patch hash",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* write(`${tmp.path}/shared.txt`, "v1")
    const snap1 = yield* snapshot.track()
    expect(snap1).toBeTruthy()
    yield* write(`${tmp.path}/shared.txt`, "v2")
    const snap2 = yield* snapshot.track()
    expect(snap2).toBeTruthy()
    yield* write(`${tmp.path}/shared.txt`, "v3")
    const patch1 = yield* snapshot.patch(snap1!)
    const patch2 = yield* snapshot.patch(snap2!)
    expect(patch1.files).toContain(fwd(tmp.path, "shared.txt"))
    expect(patch2.files).toContain(fwd(tmp.path, "shared.txt"))
    yield* snapshot.revert([patch1, patch2])
    expect(yield* readText(`${tmp.path}/shared.txt`)).toBe("v1")
  }),
  { git: true },
)

it.instance(
  "revert preserves patch order when the same hash appears again",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    yield* mkdirp(`${tmp.path}/foo`)
    yield* write(`${tmp.path}/foo/bar`, "v1")
    yield* write(`${tmp.path}/a.txt`, "v1")
    const snap1 = yield* snapshot.track()
    expect(snap1).toBeTruthy()
    yield* rm(`${tmp.path}/foo`)
    yield* write(`${tmp.path}/foo`, "v2")
    yield* write(`${tmp.path}/a.txt`, "v2")
    const snap2 = yield* snapshot.track()
    expect(snap2).toBeTruthy()
    yield* rm(`${tmp.path}/foo`)
    yield* write(`${tmp.path}/a.txt`, "v3")
    yield* snapshot.revert([
      { hash: snap1!, files: [fwd(tmp.path, "a.txt")] },
      { hash: snap2!, files: [fwd(tmp.path, "foo")] },
      { hash: snap1!, files: [fwd(tmp.path, "foo", "bar")] },
    ])
    expect(yield* readText(`${tmp.path}/a.txt`)).toBe("v1")
    expect((yield* Effect.promise(() => fs.stat(`${tmp.path}/foo`))).isDirectory()).toBe(true)
    expect(yield* readText(`${tmp.path}/foo/bar`)).toBe("v1")
  }),
  { git: true },
)

it.instance(
  "revert handles large mixed batches across chunk boundaries",
  Effect.gen(function* () {
    const tmp = yield* bootstrap()
    const snapshot = yield* Snapshot.Service
    const base = Array.from({ length: 140 }, (_, i) => fwd(tmp.path, "batch", `${i}.txt`))
    const fresh = Array.from({ length: 140 }, (_, i) => fwd(tmp.path, "fresh", `${i}.txt`))
    yield* mkdirp(`${tmp.path}/batch`)
    yield* mkdirp(`${tmp.path}/fresh`)
    yield* Effect.all(
      base.map((file, i) => write(file, `base-${i}`)),
      { concurrency: "unbounded" },
    )
    const snap = yield* snapshot.track()
    expect(snap).toBeTruthy()
    yield* Effect.all(
      [...base.map((file, i) => write(file, `next-${i}`)), ...fresh.map((file, i) => write(file, `fresh-${i}`))],
      { concurrency: "unbounded" },
    )
    const patch = yield* snapshot.patch(snap!)
    expect(patch.files.length).toBe(base.length + fresh.length)
    yield* snapshot.revert([patch])
    for (let i = 0; i < base.length; i++) expect(yield* readText(base[i])).toBe(`base-${i}`)
    for (const file of fresh) expect(yield* exists(file)).toBe(false)
  }),
  { git: true },
)
