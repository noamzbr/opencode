import { expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Credential } from "@opencode/core/credential"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { KV } from "@opencode/core/kv"
import { Integration } from "@opencode/schema/integration"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Effect, Schema } from "effect"
import { isolatedEnv } from "./fixture/environment"

test("scriptit server accepts only the explicit configuration source", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-scriptit-serve-"))
  const database = path.join(root, "opencode.db")
  const config = path.join(root, "opencode.jsonc")
  const password = "scriptit-serve-password"
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

  const child = Bun.spawn([process.execPath, path.join(import.meta.dir, "../src/index.ts"), "scriptit-serve"], {
    // Bun reads JSX settings from the tsconfig at the cwd, so the child starts inside the package.
    cwd: path.join(import.meta.dir, ".."),
    env: isolatedEnv(root, { OPENCODE_CONFIG: config, OPENCODE_CONFIG_CONTENT: undefined, OPENCODE_DB: database }),
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  })
  try {
    child.stdin.write(`${password}\n`)
    await child.stdin.flush()
    const line = await Promise.race([readLine(child.stdout, '{"url"'), Bun.sleep(30_000).then(() => undefined)])
    const stderr = line === undefined ? await readAvailable(child.stderr) : ""
    expect(line, stderr).toBeDefined()
    const { url } = Schema.decodeUnknownSync(Schema.Struct({ url: Schema.String }))(JSON.parse(line!))

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
