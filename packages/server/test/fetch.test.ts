import { expect } from "bun:test"
import fs from "node:fs/promises"
import { createServer } from "node:http"
import path from "node:path"
import { Agent } from "@opencode/schema/agent"
import { Integration } from "@opencode/schema/integration"
import { ServerInfo } from "@opencode/protocol/groups/server"
import { Effect, Layer, Schedule, Schema } from "effect"
import { OpenCode } from "@opencode/client"
import { Config } from "@opencode/core/config"
import { llmClient } from "@opencode/core/effect/app-node-platform"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { LanguageModel, LLMClient } from "../../ai/src"
import { OpenAIChat } from "../../ai/src/protocols/openai-chat"
import { TestLLM } from "../../ai/src/testing"
import { tmpdir } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const options = {
  app: { version: "test-version" },
  database: { path: ":memory:" },
  config: { project: false },
  models: { fetch: false },
  fs: { filewatcher: false },
} as const

type Handler = (request: Request) => Promise<Response>

const TOOL_SHELL_COMMAND =
  process.platform === "win32" ? "Write-Output started; Start-Sleep -Seconds 60" : "echo started; sleep 60"

function occupy(port: number, cancel = false) {
  return Effect.gen(function* () {
    const requests: string[] = []
    // A localhost listener occupies only one family; Bun can bind the other.
    const servers = ["127.0.0.1", "::1"].map((host) => ({
      host,
      server: createServer((request, response) => {
        requests.push(request.url ?? "")
        response.end(cancel ? "cancelled" : "still running", () => {
          if (cancel)
            servers.forEach((item) => {
              // Bun clears its native handle in close(), so force-close connections first.
              item.server.closeAllConnections()
              item.server.close()
            })
        })
      }),
    }))
    yield* Effect.addFinalizer(() =>
      Effect.forEach(servers, (item) =>
        Effect.callback<void>((resume) => {
          item.server.closeAllConnections()
          item.server.close(() => resume(Effect.void))
        }),
      ),
    )
    yield* Effect.forEach(servers, (item) =>
      Effect.callback<void, Error>((resume) => {
        const onError = (error: Error) => resume(Effect.fail(error))
        item.server.once("error", onError)
        item.server.listen(port, item.host, () => {
          item.server.off("error", onError)
          resume(Effect.void)
        })
      }),
    )
    return requests
  })
}

const connectOpenAI = (handler: Handler) =>
  Effect.gen(function* () {
    // Catalog endpoints no longer wait for plugin initialization.
    yield* Effect.gen(function* () {
      const response = yield* Effect.promise(() => handler(new Request("http://opencode.local/api/integration")))
      expect(response.status).toBe(200)
      const body = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Array(Integration.Info) }))(
        yield* Effect.promise(() => response.json()),
      )
      return body.data.some(
        (integration) =>
          integration.id === "openai" &&
          integration.methods.some((method) => method.type === "oauth" && method.id === "chatgpt-browser"),
      )
    }).pipe(
      Effect.filterOrFail((ready) => ready),
      Effect.retry(Schedule.spaced("10 millis")),
      Effect.timeout("2 seconds"),
    )
    return yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/integration/openai/connect/oauth", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ methodID: "chatgpt-browser" }),
        }),
      ),
    )
  })

it.live("serves the HttpApi and enforces Basic auth like the Node server", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make({ ...options, password: "secret" })

    const denied = yield* Effect.promise(() => handler(new Request("http://opencode.local/api/info")))
    expect(denied.status).toBe(401)

    const response = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/info", {
          headers: { authorization: `Basic ${btoa("opencode:secret")}` },
        }),
      ),
    )
    expect(response.status).toBe(200)
    const body = yield* Effect.promise(() => response.json()).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ServerInfo)))
    expect(body.version).toBe("test-version")
    expect(body.paths.tmp).toEndWith("opencode")
  }),
)

it.live("activates credentials through the HttpApi", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make(options)
    const response = yield* Effect.promise(() =>
      handler(new Request("http://opencode.local/api/credential/cred_missing/activate", { method: "POST" })),
    )
    expect(response.status).toBe(204)
  }),
)

it.live("serves unauthenticated and answers CORS preflight when no password is configured", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make(options)

    const response = yield* Effect.promise(() => handler(new Request("http://opencode.local/api/info")))
    expect(response.status).toBe(200)

    const preflight = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/info", {
          method: "OPTIONS",
          headers: {
            origin: "http://localhost:3000",
            "access-control-request-method": "GET",
          },
        }),
      ),
    )
    expect(preflight.headers.get("access-control-allow-origin")).toBe("http://localhost:3000")
  }),
)

it.live("applies custom CORS origins to HTTP responses and PTY ticket checks", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make({
      ...options,
      password: "secret",
      cors: ["http://192.168.1.10:3001"],
    })
    yield* Effect.forEach(
      ["http://192.168.1.10:3001", "http://localhost:3000", "https://untrusted.example.com"],
      (origin) =>
        Effect.gen(function* () {
          const allowed = origin !== "https://untrusted.example.com"
          const preflight = yield* Effect.promise(() =>
            handler(
              new Request("http://opencode.local/api/info", {
                method: "OPTIONS",
                headers: {
                  origin,
                  "access-control-request-method": "GET",
                  "access-control-request-headers": "authorization",
                },
              }),
            ),
          )
          expect(preflight.status).toBe(204)
          expect(preflight.headers.get("access-control-allow-origin")).toBe(allowed ? origin : null)
          expect(preflight.headers.get("access-control-allow-headers")).toBe("authorization")

          const response = yield* Effect.promise(() =>
            handler(
              new Request("http://opencode.local/api/info", {
                headers: { origin, authorization: `Basic ${btoa("opencode:secret")}` },
              }),
            ),
          )
          expect(response.status).toBe(200)
          expect(response.headers.get("access-control-allow-origin")).toBe(allowed ? origin : null)

          const ticket = yield* Effect.promise(() =>
            handler(
              new Request("http://opencode.local/api/experimental/persistent-pty/pty_missing/connect-token", {
                method: "POST",
                headers: { origin, authorization: `Basic ${btoa("opencode:secret")}`, "x-opencode-ticket": "1" },
              }),
            ),
          )
          // Allowed origins pass the ticket guard and reach the missing-terminal lookup.
          expect(ticket.status).toBe(allowed ? 404 : 403)
        }),
    )
  }).pipe(Effect.scoped),
)

it.live("returns 404 when a previously readable file is deleted", () =>
  Effect.acquireUseRelease(
    Effect.promise(() => tmpdir("opencode-fs-read-endpoint-")),
    (tmp) =>
      Effect.gen(function* () {
        const handler = yield* ServerFetch.make(options)
        const file = path.join(tmp.path, "deleted.txt")
        yield* Effect.promise(() => fs.writeFile(file, "content"))
        const url = new URL("http://opencode.local/api/fs/read/deleted.txt")
        url.searchParams.set("location[directory]", tmp.path)

        const readable = yield* Effect.promise(() => handler(new Request(url)))
        expect(readable.status).toBe(200)

        yield* Effect.promise(() => fs.unlink(file))
        const missing = yield* Effect.promise(() => handler(new Request(url)))
        expect(missing.status).toBe(404)
        expect(yield* Effect.promise(() => missing.json())).toEqual({
          _tag: "FileNotFoundError",
          path: "deleted.txt",
          message: "File not found: deleted.txt",
        })
      }),
    (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
  ).pipe(Effect.scoped),
)

it.live("cancels a stale OpenAI OAuth callback server before falling back", () =>
  Effect.gen(function* () {
    const requests = yield* occupy(1455, true)
    const handler = yield* ServerFetch.make(options)
    const response = yield* connectOpenAI(handler)

    expect(response.status).toBe(200)
    expect(requests).toContain("/cancel")
    const body = (yield* Effect.promise(() => response.json())) as { data: { url: string } }
    expect(new URL(body.data.url).searchParams.get("redirect_uri")).toBe("http://localhost:1455/auth/callback")
  }),
)

it.live("falls back to port 1457 when OpenAI OAuth port 1455 remains busy", () =>
  Effect.gen(function* () {
    const requests = yield* occupy(1455)
    const handler = yield* ServerFetch.make(options)
    const response = yield* connectOpenAI(handler)

    expect(response.status).toBe(200)
    expect(requests).toContain("/cancel")
    const body = (yield* Effect.promise(() => response.json())) as { data: { url: string } }
    expect(new URL(body.data.url).searchParams.get("redirect_uri")).toBe("http://localhost:1457/auth/callback")
  }),
)

it.live(
  "explains how to recover when both OpenAI OAuth callback ports are busy",
  () =>
    Effect.gen(function* () {
      yield* occupy(1455)
      yield* occupy(1457)
      const handler = yield* ServerFetch.make(options)
      const response = yield* connectOpenAI(handler)

      expect(response.status).toBe(400)
      expect(yield* Effect.promise(() => response.json())).toEqual({
        _tag: "InvalidRequestError",
        message:
          "OpenAI browser login needs local port 1455 or 1457, but both are already in use. Stop the processes using those ports or choose ChatGPT Pro/Plus (headless), then try again.",
        kind: "integration_authorization",
      })
    }),
  // Real retries wait 18 * 200 ms; startup and scoped cleanup also count toward the deadline.
  { timeout: 10_000 },
)

it.live("serves the session view operation and missing-session error", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make(options)
    const created = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/session", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        }),
      ).then((response) => response.json()),
    )
    if (typeof created !== "object" || created === null || !("data" in created))
      return yield* Effect.die(new Error("Expected a session response"))
    const data = created.data
    if (typeof data !== "object" || data === null || !("id" in data) || typeof data.id !== "string")
      return yield* Effect.die(new Error("Expected a session ID"))

    const viewed = yield* Effect.promise(() =>
      handler(
        new Request(`http://opencode.local/api/session/${data.id}/view`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ idle: 0 }),
        }),
      ),
    )
    expect(viewed.status).toBe(204)

    const invalid = yield* Effect.promise(() =>
      handler(new Request(`http://opencode.local/api/session/${data.id}/view`, { method: "POST" })),
    )
    expect(invalid.status).toBe(400)

    const missing = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/session/ses_missing_view/view", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ idle: 0 }),
        }),
      ),
    )
    expect(missing.status).toBe(404)
  }),
)

it.live(
  "stops a shell command over HTTP without removing it",
  () =>
    Effect.gen(function* () {
      const llm = yield* TestLLM.Test.pipe(Effect.provide(TestLLM.testLayer()))
      const model = SessionRunnerModel.resolved(
        LanguageModel.make({ id: "stop-model", provider: "test", route: OpenAIChat.route }),
        {
          capabilities: { tools: true, input: ["text"], output: ["text"] },
          cost: [],
          limit: { context: 200_000, output: 8_192 },
        },
      )
      const handler = yield* ServerFetch.make(options, {
        overrides: [
          // The machine's global config must not decide what this server allows.
          Config.node.replace(
            Config.configured({
              project: false,
              global: false,
              content: JSON.stringify({ permissions: [{ action: "*", resource: "*", effect: "allow" }] }),
            }),
          ),
          llmClient.replace(Layer.succeed(LLMClient.Service, llm)),
          SessionRunnerModel.node.replace(
            Layer.succeed(SessionRunnerModel.Service, { resolve: () => Effect.succeed(model) }),
          ),
        ],
      })
      const json = (input: Request) => Effect.promise(() => handler(input).then((response) => response.json()))
      const api = OpenCode.make({
        baseUrl: "http://opencode.local",
        fetch: Object.assign((input: string | URL | Request, init?: RequestInit) => handler(new Request(input, init)), {
          preconnect: fetch.preconnect,
        }),
      })
      const created = yield* json(
        new Request("http://opencode.local/api/shell", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            command: process.platform === "win32" ? "Start-Sleep -Seconds 60" : "sleep 60",
            timeout: 0,
          }),
        }),
      )
      const id = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Struct({ id: Schema.String }) }))(created).data
        .id

      const stopped = yield* Effect.promise(() =>
        handler(new Request(`http://opencode.local/api/shell/${id}/stop`, { method: "POST" })),
      )
      expect(stopped.status).toBe(200)
      expect(yield* Effect.promise(() => stopped.json())).toMatchObject({ data: { id, status: "killed" } })
      // Still readable after the stop; a second stop is idempotent.
      expect(yield* json(new Request(`http://opencode.local/api/shell/${id}`))).toMatchObject({
        data: { id, status: "killed" },
      })
      expect(yield* json(new Request(`http://opencode.local/api/shell/${id}/stop`, { method: "POST" }))).toMatchObject({
        data: { id, status: "killed" },
      })

      const missing = yield* Effect.promise(() =>
        handler(new Request("http://opencode.local/api/shell/sh_missing/stop", { method: "POST" })),
      )
      expect(missing.status).toBe(404)

      // A shell a tool started owns a Job. Stop replies only once that Job has
      // settled, so the interrupt that follows cannot cancel it and discard the
      // captured output. The title is explicit so that no summary request takes
      // the queued tool call.
      const session = yield* Effect.promise(() => api.session.create({ title: "Stop" }))
      yield* llm.push(TestLLM.tool("call_stop", "shell", { command: TOOL_SHELL_COMMAND }))
      // The step after the tool never finishes, so the interrupt below always reaches live execution.
      yield* llm.always(TestLLM.hangAfter())
      yield* Effect.promise(() => api.session.prompt({ sessionID: session.id, text: "run one command" }))
      const started = yield* Effect.promise(() => api.shell.list()).pipe(
        Effect.map((page) => page.data.find((shell) => shell.id !== id)),
        Effect.repeat({ until: (shell) => shell !== undefined, schedule: Schedule.spaced("10 millis") }),
      )
      if (!started) return yield* Effect.die(new Error("Expected the tool to start a shell"))
      yield* Effect.promise(() => api.shell.output({ id: started.id })).pipe(
        Effect.repeat({
          until: (page) => page.data.output.includes("started"),
          schedule: Schedule.spaced("10 millis"),
        }),
      )

      const killed = yield* Effect.promise(() => api.shell.stop({ id: started.id }))
      const interrupt = yield* Effect.promise(() => api.session.interrupt({ sessionID: session.id }))
      expect(killed.data).toMatchObject({ id: started.id, status: "killed" })
      // The interrupt reaches live execution, so only an already settled Job survives it.
      expect(interrupt.interrupted).toBe(true)

      const tool = yield* Effect.promise(() => api.message.list({ sessionID: session.id })).pipe(
        Effect.map((page) =>
          page.data
            .flatMap((message) => (message.type === "assistant" ? message.content : []))
            .find((part) => part.type === "tool"),
        ),
        Effect.repeat({
          until: (part) => part !== undefined && part.state.status !== "streaming" && part.state.status !== "running",
          schedule: Schedule.spaced("10 millis"),
        }),
      )
      if (tool?.state.status !== "completed")
        return yield* Effect.die(new Error(`Expected a completed tool result, got ${tool?.state.status}`))
      expect(tool.state.metadata).toMatchObject({ status: "stopped" })
      expect(tool.state.content.map((content) => (content.type === "text" ? content.text : "")).join("")).toContain(
        "started",
      )
      expect((yield* Effect.promise(() => api.shell.get({ id: started.id }))).data).toMatchObject({ status: "killed" })
      expect((yield* Effect.promise(() => api.shell.output({ id: started.id }))).data.output).toContain("started")
    }),
  { timeout: 30_000 },
)

it.live("routes pending requests through the Session's instance", () =>
  Effect.gen(function* () {
    const config = yield* Effect.acquireDisposable(Effect.promise(() => tmpdir("opencode-pending-read-")))
    const handler = yield* ServerFetch.make({
      ...options,
      config: {
        directory: config.path,
        project: false,
        content: JSON.stringify({ permissions: [{ action: "shell", resource: "*", effect: "ask" }] }),
      },
    })
    const created = (yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/session", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        }),
      ).then((response) => response.json()),
    )) as { data: { id: string } }

    const loaded = () =>
      Effect.promise(() =>
        handler(new Request("http://opencode.local/api/debug/location")).then(
          (response) => response.json() as Promise<unknown[]>,
        ),
      )

    // Session routing must ignore the caller's unrelated Location.
    const headers = { "x-opencode-directory": encodeURIComponent(config.path) }
    expect(yield* loaded()).toEqual([])
    for (const resource of ["permission", "form"]) {
      const response = yield* Effect.promise(() =>
        handler(new Request(`http://opencode.local/api/session/${created.data.id}/${resource}`, { headers })),
      )
      expect(response.status).toBe(200)
      expect(yield* Effect.promise(() => response.json())).toEqual({ data: [] })

      const missing = yield* Effect.promise(() =>
        handler(new Request(`http://opencode.local/api/session/ses_missing_pending/${resource}`, { headers })),
      )
      expect(missing.status).toBe(404)
    }
    const global = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/session/global/form", {
          headers: { "x-opencode-directory": encodeURIComponent(process.cwd()) },
        }),
      ),
    )
    expect(global.status).toBe(200)
    expect(yield* Effect.promise(() => global.json())).toEqual({ data: [] })
    expect(yield* loaded()).toEqual([{ directory: process.cwd() }])

    const createdForm = yield* Effect.promise(() =>
      handler(
        new Request(`http://opencode.local/api/session/${created.data.id}/form`, {
          method: "POST",
          headers: { "content-type": "application/json", ...headers },
          body: JSON.stringify({ title: "Test form", fields: [{ key: "answer", type: "string" }] }),
        }),
      ),
    )
    expect(createdForm.status).toBe(200)

    const forms = yield* Effect.promise(() =>
      handler(new Request(`http://opencode.local/api/session/${created.data.id}/form`, { headers })),
    )
    expect(forms.status).toBe(200)
    expect(yield* Effect.promise(() => forms.json())).toMatchObject({
      data: [{ title: "Test form" }],
    })
    expect(yield* loaded()).toHaveLength(1)

    const globalForm = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/session/global/form", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-opencode-directory": encodeURIComponent(process.cwd()),
          },
          body: JSON.stringify({ title: "Global form", fields: [{ key: "answer", type: "string" }] }),
        }),
      ),
    )
    expect(globalForm.status).toBe(200)

    const globalForms = yield* Effect.promise(() =>
      handler(
        new Request("http://opencode.local/api/session/global/form", {
          headers: { "x-opencode-directory": encodeURIComponent(process.cwd()) },
        }),
      ),
    )
    expect(globalForms.status).toBe(200)
    expect(yield* Effect.promise(() => globalForms.json())).toMatchObject({ data: [{ title: "Global form" }] })

    // Agent permission policy is installed by plugin activation.
    yield* Effect.gen(function* () {
      const response = yield* Effect.promise(() => handler(new Request("http://opencode.local/api/agent")))
      expect(response.status).toBe(200)
      const body = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Array(Agent.Info) }))(
        yield* Effect.promise(() => response.json()),
      )
      return body.data.some(
        (agent) =>
          agent.id === "build" &&
          agent.permissions.some((rule) => rule.action === "shell" && rule.resource === "*" && rule.effect === "ask"),
      )
    }).pipe(
      Effect.filterOrFail((ready) => ready),
      Effect.retry(Schedule.spaced("10 millis")),
      Effect.timeout("2 seconds"),
    )
    const createdPermission = yield* Effect.promise(() =>
      handler(
        new Request(`http://opencode.local/api/session/${created.data.id}/permission`, {
          method: "POST",
          headers: { "content-type": "application/json", ...headers },
          body: JSON.stringify({ id: "per_pending_read", action: "shell", resources: ["pwd"] }),
        }),
      ),
    )
    expect(createdPermission.status).toBe(200)
    expect(yield* Effect.promise(() => createdPermission.json())).toEqual({
      data: { id: "per_pending_read", effect: "ask" },
    })

    const permissions = yield* Effect.promise(() =>
      handler(new Request(`http://opencode.local/api/session/${created.data.id}/permission`, { headers })),
    )
    expect(permissions.status).toBe(200)
    expect(yield* Effect.promise(() => permissions.json())).toMatchObject({
      data: [{ id: "per_pending_read", sessionID: created.data.id, action: "shell", resources: ["pwd"] }],
    })
    expect(yield* loaded()).toHaveLength(1)
  }),
)

// Pins the eager-boot guarantee: the application layer is built before the handler returns, so
// an aborted first request cannot interrupt layer construction and wedge every later request
// (the Effect-TS/effect#6319 failure class that lazy first-request builds are prone to).
it.live("stays serviceable when the first request aborts", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make(options)

    const aborted = yield* Effect.promise(() => {
      const controller = new AbortController()
      const first = handler(new Request("http://opencode.local/api/info", { signal: controller.signal }))
      controller.abort()
      return first.then(
        () => "resolved" as const,
        () => "rejected" as const,
      )
    })
    expect(["resolved", "rejected"]).toContain(aborted)

    const second = yield* Effect.promise(() => handler(new Request("http://opencode.local/api/info")))
    expect(second.status).toBe(200)
  }),
)
