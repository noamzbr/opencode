import { expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Bus } from "@opencode/core/bus"
import { Credential } from "@opencode/core/credential"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { KV } from "@opencode/core/kv"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionTable } from "@opencode/core/session/sql"
import { Event } from "@opencode/schema/event"
import { Integration } from "@opencode/schema/integration"
import { Shell } from "@opencode/schema/shell"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Hash } from "@opencode/util/hash"
import { Effect, Schema } from "effect"
import { ScriptitDispatcher } from "../src/commands/handlers/scriptit-dispatcher"
import { isolatedEnv } from "./fixture/environment"
import { fakeManager, tmpdir } from "./fixture/execution-manager"

const password = "scriptit-serve-password"

test("scriptit server accepts only the explicit configuration source", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-scriptit-serve-"))
  const database = path.join(root, "opencode.db")
  const config = path.join(root, "opencode.jsonc")
  const manifest = { requests: 0 }
  using origin = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      if (!new URL(request.url).pathname.endsWith("/.well-known/opencode")) return new Response(null, { status: 404 })
      manifest.requests += 1
      return Response.json({
        auth: { command: ["true"], env: "SENTINEL_TOKEN" },
        config: { plugin: ["sentinel-wellknown"] },
      })
    },
  })
  const source = `http://127.0.0.1:${origin.port}`
  await fs.writeFile(config, JSON.stringify({ plugin: ["sentinel-explicit"] }))
  await Effect.runPromise(
    Effect.gen(function* () {
      const kv = yield* KV.Service
      const credentials = yield* Credential.Service
      yield* kv.set("wellknown:sources", [source])
      yield* credentials.create({
        integrationID: Integration.ID.make(source),
        value: { type: "key", key: "sentinel-key" },
        label: "sentinel",
      })
    }).pipe(
      Effect.provide(
        AppNodeBuilder.build(LayerNode.group([KV.node, Credential.node]), [
          Database.node.replace(Database.configured({ path: database })),
        ]),
      ),
      Effect.scoped,
    ),
  )

  const child = spawnServer(root, {
    OPENCODE_CONFIG: config,
    OPENCODE_CONFIG_CONTENT: undefined,
    OPENCODE_DB: database,
  })
  try {
    const url = await serverURL(child)

    const response = await fetch(new URL("/api/config", url), {
      headers: { authorization: "Basic " + btoa(`opencode:${password}`) },
    })
    expect(response.status).toBe(200)
    const entries = await response.text()
    expect(entries).toContain("sentinel-explicit")
    // The well-known origin and its key credential are already in the database.
    expect(entries).not.toContain("sentinel-wellknown")
    expect(manifest.requests).toBe(0)

    const userID = "msg_000000000001aaaaaaaaaaaaaa"
    const compactID = "msg_000000000002aaaaaaaaaaaaaa"
    const summaryID = "msg_000000000003aaaaaaaaaaaaaa"
    const source = {
      session: { id: "ses_test", agent: null, model: null },
      messages: [
        { id: userID, session_id: "ses_test", time_created: 1, time_updated: 1, data: JSON.stringify({ role: "user", time: { created: 1 }, agent: "build", model: { providerID: "provider", modelID: "model" } }) },
        { id: compactID, session_id: "ses_test", time_created: 2, time_updated: 2, data: JSON.stringify({ role: "user", time: { created: 2 }, agent: "build", model: { providerID: "provider", modelID: "model" } }) },
        { id: summaryID, session_id: "ses_test", time_created: 3, time_updated: 4, data: JSON.stringify({ role: "assistant", summary: true, parentID: compactID, time: { created: 3, completed: 4 }, modelID: "model", providerID: "provider", mode: "build", agent: "build", path: { cwd: "/tmp/test", root: "/tmp/test" }, cost: 0, tokens: { total: 0, input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } } }) },
      ],
      parts: [
        { id: "prt_1", message_id: userID, session_id: "ses_test", time_created: 1, time_updated: 1, data: JSON.stringify({ type: "text", text: "Earlier work" }) },
        { id: "prt_2", message_id: compactID, session_id: "ses_test", time_created: 2, time_updated: 2, data: JSON.stringify({ type: "compaction", auto: true, tail_start_id: userID }) },
        { id: "prt_3", message_id: summaryID, session_id: "ses_test", time_created: 3, time_updated: 4, data: JSON.stringify({ type: "text", text: "Summary" }) },
      ],
    }
    const convertURL = new URL("/scriptit/internal/v1-transform", url)
    expect((await fetch(convertURL, { method: "POST", body: JSON.stringify(source) })).status).toBe(401)
    const converted = await fetch(convertURL, {
      method: "POST",
      headers: { authorization: "Basic " + btoa(`opencode:${password}`), "content-type": "application/json" },
      body: JSON.stringify(source),
    })
    expect(converted.status).toBe(200)
    const migration = await converted.json() as { messages: Array<{ id: string; type: string; data: Record<string, unknown> }>; warnings: unknown[] }
    expect(migration.warnings).toEqual([])
    expect(migration.messages.map((row) => [row.id, row.type])).toEqual([[userID, "user"], [compactID, "compaction"]])
    expect(migration.messages[1].data).toMatchObject({ summary: "Summary", reason: "auto" })
    expect(migration.messages[1].data.recent).toContain("Earlier work")

    child.stdin.end()
    expect(await Promise.race([child.exited.then(() => true), Bun.sleep(10_000).then(() => false)])).toBe(true)
  } finally {
    child.kill("SIGKILL")
    await child.exited
    await fs.rm(root, { recursive: true, force: true })
  }
}, 60_000)

test("scriptit server fails interrupted agent blocks at boot and reloads its config in place", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-scriptit-boot-"))
  const database = path.join(root, "opencode.db")
  const config = path.join(root, "opencode.jsonc")
  const directories = [path.join(root, "first"), path.join(root, "second")]
  await Promise.all(directories.map((directory) => fs.mkdir(directory)))
  await fs.writeFile(config, JSON.stringify(modelConfig("probe-before")))
  const chat = Session.ID.make("ses_boot_chat")
  const block = Session.ID.make("ses_boot_block")
  const idle = Session.ID.make("ses_boot_idle")
  const started = Event.ID.create()
  await Effect.runPromise(
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const bus = yield* Bus.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make(directories[0]), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      // Both claimed Sessions were cut mid-turn by the previous server; the chat names a model that
      // does not exist, so its resumed turn fails at once.
      const session = { project_id: Project.ID.global, directory: directories[0], version: "test" }
      yield* db
        .insert(SessionTable)
        .values([
          {
            ...session,
            id: chat,
            slug: chat,
            time_suspended: Date.now(),
            model: { id: "missing", providerID: "missing" },
          },
          {
            ...session,
            id: block,
            slug: block,
            time_suspended: Date.now(),
            metadata: { scriptit: { kind: "block", parentSession: chat } },
          },
          { ...session, id: idle, slug: idle },
        ])
        .run()
        .pipe(Effect.orDie)
      yield* bus.publish(
        SessionEvent.Shell.Started,
        {
          sessionID: idle,
          shell: {
            id: Shell.ID.create(),
            status: "running",
            command: "sleep 600",
            cwd: directories[0],
            shell: "/bin/sh",
            file: path.join(root, "shell.out"),
            metadata: { sessionID: idle, background: true },
            time: { started: Date.now() },
          },
        },
        { id: started },
      )
    }).pipe(
      Effect.provide(
        AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SessionProjector.node]), [
          Database.node.replace(Database.configured({ path: database })),
          Bus.node.replace(Bus.configured({ persist: true })),
        ]),
      ),
      Effect.scoped,
    ),
  )

  const child = spawnServer(root, {
    OPENCODE_CONFIG: config,
    OPENCODE_CONFIG_CONTENT: undefined,
    OPENCODE_DB: database,
  })
  try {
    const url = await serverURL(child)
    const api =(pathname: string, directory = directories[0]) =>
      fetch(new URL(pathname, url), {
        headers: { authorization: "Basic " + btoa(`opencode:${password}`), "x-opencode-directory": directory },
      })
    const log = async (sessionID: Session.ID) =>
      (await (await api(`/api/experimental/session/${sessionID}/log`)).text())
        .split("\n")
        .filter((line) => line.startsWith("data: "))
        .map((line) => JSON.parse(line.slice("data: ".length)) as { type: string; data: Record<string, unknown> })
        .filter((item) => item.type !== "log.synced")

    // Recovery resumes the chat after the listener opens.
    const resumed = await until(async () => (await log(chat)).some((event) => event.type === "session.execution.started"))
    expect(resumed).toBe(true)
    const failed = await log(block)
    expect(failed.map((event) => event.type)).toEqual(["session.execution.failed"])
    expect(failed[0].data.error).toMatchObject({ type: "server_restart" })
    const messages = (await (await api(`/api/session/${idle}/message`)).json()) as {
      data: Array<{ type: string; status?: string; output?: { output: string } }>
    }
    expect(messages.data.find((message) => message.type === "shell")).toMatchObject({
      status: "unavailable",
      output: { output: "Shell command output is no longer available." },
    })

    const info = async () => ((await (await api("/api/info")).json()) as { pid: number }).pid
    // Each loaded Location serves the `auto` model from the config file.
    const autoModels = async (directory: string) =>
      ((await (await api("/api/model", directory)).json()) as { data: Array<{ id: string; modelID: string }> }).data
        .filter((model) => model.id === "auto")
        .map((model) => model.modelID)
    const serves = (modelID: string) =>
      until(async () =>
        (await Promise.all(directories.map(autoModels))).every((ids) => ids.length === 1 && ids[0] === modelID),
      )
    const pid = await info()
    expect(await serves("probe-before")).toBe(true)
    // The bridge's write: a sibling temporary file renamed over the config.
    await fs.writeFile(`${config}.tmp`, JSON.stringify(modelConfig("probe-after")))
    await fs.rename(`${config}.tmp`, config)
    expect(await serves("probe-after")).toBe(true)
    expect(await info()).toBe(pid)
  } finally {
    child.kill("SIGKILL")
    await child.exited
    await fs.rm(root, { recursive: true, force: true })
  }
}, 60_000)

test("scriptit server runs every Location's processes in the execution manager under fixed invariants", async () => {
  await using tmp = await tmpdir()
  const root = tmp.path
  const config = path.join(root, "opencode.jsonc")
  // A workload can plant a repository marker with a cached project id above its Location.
  const locations = await Promise.all(
    [".git", ".hg"].map(async (marker) => {
      const repository = path.join(root, `planted${marker}-repository`)
      await fs.mkdir(path.join(repository, marker), { recursive: true })
      await fs.writeFile(path.join(repository, marker, "opencode"), `planted${marker}`)
      await fs.mkdir(path.join(repository, "session"))
      return path.join(repository, "session")
    }),
  )
  await fs.writeFile(
    config,
    JSON.stringify({
      snapshots: true,
      formatter: { probe: { command: ["touch", path.join(root, "formatted")], extensions: [".txt"] } },
      tool_output: { max_lines: 10, max_bytes: 100 },
      plugins: ["opencode.tool.webfetch", "opencode.tool.websearch", "opencode.tools", "opencode.browser"],
    }),
  )
  await using manager = await fakeManager(async (peer) => {
    peer.reply({ event: "started", pid: 5 })
    peer.send(ScriptitDispatcher.Frame.stdout, "from the manager\n")
    peer.reply({ event: "exit", code: 0, signal: null })
    peer.end()
  })
  const child = spawnServer(root, {
    OPENCODE_CONFIG: config,
    OPENCODE_CONFIG_CONTENT: undefined,
    OPENCODE_DB: path.join(root, "opencode.db"),
    SCRIPTIT_EXEC_SOCKET: manager.socket,
  })
  try {
    const url = await serverURL(child)
    const api = async (pathname: string, directory: string, init: RequestInit = {}) =>
      (
        await fetch(new URL(pathname, url), {
          ...init,
          headers: {
            authorization: "Basic " + btoa(`opencode:${password}`),
            "content-type": "application/json",
            "x-opencode-directory": directory,
          },
        })
      ).json()

    for (const directory of locations) {
      const location = (await api("/api/location", directory)) as { project: { id: string; directory: string } }
      expect(location.project).toMatchObject({ id: Hash.fast(`directory:${directory}`), directory })
    }
    // Neither boot nor the planted markers ran anything.
    expect(manager.requests).toEqual([])

    const entries = (await api("/api/config", locations[0])) as Array<{ type: string; info?: Record<string, unknown> }>
    const latest = (key: string) => entries.findLast((entry) => entry.info?.[key] !== undefined)?.info?.[key]
    expect(latest("snapshots")).toBe(false)
    expect(latest("formatter")).toBe(false)
    // The limits scriptit-tool-output spills at.
    expect(latest("tool_output")).toEqual({ max_lines: 2000, max_bytes: 51200 })
    // The last entry adds the Script.it plugins after every plugin operation of the rendered file.
    expect(entries.at(-1)?.info?.plugins).toEqual(
      expect.arrayContaining([
        "file:///opt/bridge/.opencode/plugins/scriptit-webfetch",
        "file:///opt/bridge/.opencode/plugins/scriptit-tool-output",
      ]),
    )
    // The Location activates its plugins after it answers its first request.
    let ids: string[] = []
    await until(async () => {
      ids = ((await api("/api/plugin", locations[0])) as { data: Array<{ id: string }> }).data.map((plugin) => plugin.id)
      return ids.length > 0
    })
    expect(ids).toContain("opencode.tool.read")
    for (const id of ["opencode.tool.webfetch", "opencode.tool.websearch", "opencode.tools", "opencode.browser"])
      expect(ids).not.toContain(id)

    const shell = (await api("/api/shell", locations[0], {
      method: "POST",
      body: JSON.stringify({ command: "echo hi" }),
    })) as { data: { id: string } }
    expect(
      await until(async () => {
        const info = (await api(`/api/shell/${shell.data.id}`, locations[0])) as { data: { status: string } }
        return info.data.status === "exited"
      }),
    ).toBe(true)
    const output = (await api(`/api/shell/${shell.data.id}/output`, locations[0])) as { data: { output: string } }
    expect(output.data.output).toBe("from the manager\n")
    expect(manager.requests).toMatchObject([{ op: "spawn", location: locations[0], cwd: locations[0] }])
  } finally {
    child.kill("SIGKILL")
    await child.exited
  }
}, 60_000)

function spawnServer(root: string, env: Record<string, string | undefined>) {
  const child = Bun.spawn([process.execPath, path.join(import.meta.dir, "../src/index.ts"), "scriptit-serve"], {
    // Bun reads JSX settings from the tsconfig at the cwd, so the child starts inside the package.
    cwd: path.join(import.meta.dir, ".."),
    env: isolatedEnv(root, env),
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  })
  child.stdin.write(`${password}\n`)
  return child
}

async function serverURL(child: ReturnType<typeof spawnServer>) {
  await child.stdin.flush()
  const line = await Promise.race([readLine(child.stdout, '{"url"'), Bun.sleep(30_000).then(() => undefined)])
  const stderr = line === undefined ? await readAvailable(child.stderr) : ""
  expect(line, stderr).toBeDefined()
  return Schema.decodeUnknownSync(Schema.Struct({ url: Schema.String }))(JSON.parse(line!)).url
}

function modelConfig(modelID: string) {
  return {
    model: "scriptit/auto",
    providers: {
      scriptit: {
        settings: { apiKey: "unused" },
        models: {
          auto: {
            modelID,
            package: "aisdk:@ai-sdk/anthropic",
            capabilities: { tools: true, input: ["text"], output: ["text"] },
            limit: { context: 100_000, input: 80_000, output: 10_000 },
            settings: { baseURL: "http://127.0.0.1:9/v1" },
          },
        },
      },
    },
  }
}

async function until(check: () => Promise<boolean>) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (await check()) return true
    await Bun.sleep(100)
  }
  return false
}

async function readLine(stream: ReadableStream<Uint8Array>, prefix: string) {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  const chunks: string[] = []
  try {
    while (true) {
      const result = await reader.read()
      if (result.done) break
      chunks.push(decoder.decode(result.value, { stream: true }))
      const line = chunks
        .join("")
        .split("\n")
        .find((line) => line.startsWith(prefix))
      if (line) return line
    }
  } finally {
    reader.releaseLock()
  }
  return chunks
    .join("")
    .split("\n")
    .find((line) => line.startsWith(prefix))
}

async function readAvailable(stream: ReadableStream<Uint8Array>) {
  return await Promise.race([new Response(stream).text(), Bun.sleep(1_000).then(() => "")])
}
