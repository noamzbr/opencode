import { describe, expect, it } from "bun:test"
import type { AgentSideConnection } from "@agentclientprotocol/sdk"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import type { Event, Message, OpencodeClient, Part, SessionMessageResponse, ToolPart } from "@opencode-ai/sdk/v2"
import { Effect, ManagedRuntime } from "effect"
import { ACPEvent } from "@/acp/event"
import * as ACPService from "@/acp/service"
import { Directory } from "@/acp/directory"
import { ACPSession } from "@/acp/session"

type SessionUpdateParams = Parameters<AgentSideConnection["sessionUpdate"]>[0]
type ToolSessionUpdateParams = SessionUpdateParams & {
  update: Extract<SessionUpdateParams["update"], { sessionUpdate: "tool_call" | "tool_call_update" }>
}
type GlobalEventEnvelope = {
  payload?: Event
}
type DeltaPartType = Extract<Part, { type: "text" | "reasoning" }>["type"]

const pollUntil = async (
  check: () => boolean | Promise<boolean>,
  message: string,
  opts?: { timeoutMs?: number; intervalMs?: number },
) => {
  const started = Date.now()
  while (true) {
    if (await check()) return
    if (Date.now() - started > (opts?.timeoutMs ?? 2000)) throw new Error(message)
    await new Promise((resolve) => setTimeout(resolve, opts?.intervalMs ?? 5))
  }
}

function makeSessionService() {
  return ManagedRuntime.make(LayerNode.compile(ACPSession.node)).runSync(
    ACPSession.Service.use((service) => Effect.succeed(service)),
  )
}

function createEventStream() {
  const queue: GlobalEventEnvelope[] = []
  const waiters: Array<(value: GlobalEventEnvelope | undefined) => void> = []
  const state = { closed: false }

  const push = (event: GlobalEventEnvelope) => {
    const waiter = waiters.shift()
    if (waiter) {
      waiter(event)
      return
    }
    queue.push(event)
  }

  const close = () => {
    state.closed = true
    for (const waiter of waiters.splice(0)) {
      waiter(undefined)
    }
  }

  const stream = async function* (signal?: AbortSignal) {
    while (true) {
      if (signal?.aborted) return
      const next = queue.shift()
      if (next) {
        yield next
        continue
      }
      if (state.closed) return
      const value = await new Promise<GlobalEventEnvelope | undefined>((resolve) => {
        waiters.push(resolve)
        signal?.addEventListener("abort", () => resolve(undefined), { once: true })
      })
      if (!value) return
      yield value
    }
  }

  return { push, close, stream }
}

function createHarness(messages: Record<string, SessionMessageResponse> = {}, withExtensions = false) {
  const updates: SessionUpdateParams[] = []
  const notifications: Array<{ method: string; params: Record<string, unknown> }> = []
  const calls = {
    eventSubscribe: 0,
    message: 0,
  }
  const events = createEventStream()
  const sdk = {
    global: {
      event: (options?: { signal?: AbortSignal }) => {
        calls.eventSubscribe++
        return Promise.resolve({ stream: events.stream(options?.signal) })
      },
    },
    session: {
      message: (input: { messageID: string }) => {
        calls.message++
        return Promise.resolve({ data: messages[input.messageID] })
      },
      get: () => Promise.resolve({ data: { id: "ses_loaded" } }),
      messages: () => Promise.resolve({ data: [] }),
    },
  } as unknown as OpencodeClient
  const connection = {
    sessionUpdate: (params: SessionUpdateParams) => {
      updates.push(params)
      return Promise.resolve()
    },
    ...(withExtensions && {
      extNotification: (method: string, params: Record<string, unknown>) => {
        notifications.push({ method, params })
        return Promise.resolve()
      },
    }),
  } satisfies Pick<AgentSideConnection, "sessionUpdate"> & Partial<Pick<AgentSideConnection, "extNotification">>
  const session = makeSessionService()
  const subscription = new ACPEvent.Subscription({ sdk, connection, session })

  return { calls, connection, events, notifications, sdk, session, subscription, updates }
}

function textDelta(sessionID: string, messageID: string, partID: string, delta: string): Event {
  return {
    id: `evt_${sessionID}_${messageID}_${partID}_${delta}`,
    type: "message.part.delta",
    properties: {
      sessionID,
      messageID,
      partID,
      field: "text",
      delta,
    },
  }
}

function partUpdated(sessionID: string, messageID: string, partID: string, type: DeltaPartType): Event {
  return {
    id: `evt_${sessionID}_${messageID}_${partID}`,
    type: "message.part.updated",
    properties: {
      sessionID,
      time: Date.now(),
      part:
        type === "text"
          ? {
              id: partID,
              sessionID,
              messageID,
              type: "text",
              text: "",
            }
          : {
              id: partID,
              sessionID,
              messageID,
              type: "reasoning",
              text: "",
              time: { start: Date.now() },
            },
    },
  }
}

function toolUpdated(part: ToolPart): Event {
  return {
    id: `evt_${part.sessionID}_${part.messageID}_${part.id}_${part.state.status}`,
    type: "message.part.updated",
    properties: {
      sessionID: part.sessionID,
      time: Date.now(),
      part,
    },
  }
}

function assistantMessage(sessionID: string, messageID: string, partID: string, type: DeltaPartType) {
  return {
    info: {
      id: messageID,
      sessionID,
      role: "assistant",
      time: { created: Date.now() },
      parentID: "msg_parent",
      modelID: "model",
      providerID: "provider",
      mode: "build",
      agent: "build",
      path: { cwd: "/workspace", root: "/workspace" },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    },
    parts: [
      type === "text"
        ? {
            id: partID,
            sessionID,
            messageID,
            type: "text",
            text: "",
          }
        : {
            id: partID,
            sessionID,
            messageID,
            type: "reasoning",
            text: "",
            time: { start: Date.now() },
          },
    ],
  } satisfies SessionMessageResponse
}

function assistantToolMessage(part: ToolPart) {
  return {
    info: {
      id: part.messageID,
      sessionID: part.sessionID,
      role: "assistant",
      time: { created: Date.now() },
      parentID: "msg_parent",
      modelID: "model",
      providerID: "provider",
      mode: "build",
      agent: "build",
      path: { cwd: "/workspace", root: "/workspace" },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    },
    parts: [part],
  } satisfies SessionMessageResponse
}

function runningTool(
  sessionID: string,
  callID: string,
  output?: string,
  input: Record<string, unknown> = { cmd: "printf hello" },
) {
  return {
    id: `part_${callID}`,
    sessionID,
    messageID: `msg_${callID}`,
    type: "tool",
    callID,
    tool: "bash",
    state: {
      status: "running",
      input,
      title: "bash",
      ...(output !== undefined ? { metadata: { output } } : {}),
      time: { start: Date.now() },
    },
  } satisfies ToolPart
}

function completedTool(
  sessionID: string,
  callID: string,
  output = "done",
  attachments: Extract<ToolPart["state"], { status: "completed" }>["attachments"] = [],
  options: {
    readonly tool?: string
    readonly input?: Record<string, unknown>
    readonly metadata?: Record<string, unknown>
  } = {},
) {
  return {
    id: `part_${callID}`,
    sessionID,
    messageID: `msg_${callID}`,
    type: "tool",
    callID,
    tool: options.tool ?? "bash",
    state: {
      status: "completed",
      input: options.input ?? { cmd: "printf done" },
      output,
      title: "bash",
      metadata: options.metadata ?? { exit: 0 },
      time: { start: Date.now() - 1, end: Date.now() },
      ...(attachments.length ? { attachments } : {}),
    },
  } satisfies ToolPart
}

function errorTool(sessionID: string, callID: string) {
  return {
    id: `part_${callID}`,
    sessionID,
    messageID: `msg_${callID}`,
    type: "tool",
    callID,
    tool: "bash",
    state: {
      status: "error",
      input: { cmd: "exit 1" },
      error: "failed hard",
      metadata: { exit: 1 },
      time: { start: Date.now() - 1, end: Date.now() },
    },
  } satisfies ToolPart
}

function toolUpdates(updates: SessionUpdateParams[]) {
  return updates.filter((item): item is ToolSessionUpdateParams => {
    return item.update.sessionUpdate === "tool_call" || item.update.sessionUpdate === "tool_call_update"
  })
}

async function createKnownSession(
  session: ACPSession.Interface,
  sessionId: string,
  part: { messageId: string; partId: string; partType: Part["type"]; role?: Message["role"] },
) {
  await Effect.runPromise(session.create({ id: sessionId, cwd: "/workspace" }))
  await Effect.runPromise(
    session.recordPartMetadata({
      sessionId,
      messageId: part.messageId,
      partId: part.partId,
      partType: part.partType,
      role: part.role ?? "assistant",
    }),
  )
}

describe("acp event routing", () => {
  it("routes replay and settles each owned lifecycle without clearing other roots", async () => {
    const harness = createHarness({}, true)
    const sessionID = "ses_owned"
    await Effect.runPromise(harness.session.create({ id: sessionID, cwd: "/workspace" }))
    const track = async (id: string, kind: "prompt" | "shell" = "prompt") => {
      harness.subscription.trackOperation(sessionID, id, kind)
      await harness.subscription.acceptOperation(sessionID, id)
    }
    const idle = () =>
      harness.subscription.handle({
        id: `evt_idle_${harness.notifications.length}`,
        type: "session.status",
        properties: { sessionID, status: { type: "idle" } },
      })
    const complete = (
      id: string,
      parentID: string,
      finish: "stop" | "tool-calls" | "error" = "stop",
      error?: Extract<Message, { role: "assistant" }>["error"],
    ) => {
      const info = assistantMessage(sessionID, id, `part_${id}`, "text").info
      Object.assign(info, { parentID, finish, error, time: { created: 1, completed: 2 } })
      return harness.subscription.handle({ id: `evt_${id}`, type: "message.updated", properties: { sessionID, info } })
    }
    const replay = assistantMessage(sessionID, "msg_summary", "part_summary", "text")
    await harness.subscription.replayMessage({
      ...replay,
      info: { ...replay.info, summary: true },
      parts: [{ ...replay.parts[0], text: "summary" }],
    })
    expect(harness.notifications[0]?.params).toMatchObject({
      update: { _meta: { scriptit: { kind: "compaction_summary" } } },
    })

    harness.subscription.trackOperation(sessionID, "msg_fast", "prompt")
    harness.subscription.markOperationSubmitted(sessionID, "msg_fast")
    await complete("msg_fast_done", "msg_fast")
    await idle()
    expect(harness.notifications.map((item) => item.method)).toEqual(["session/replayUpdate"])
    await harness.subscription.acceptOperation(sessionID, "msg_fast")
    expect(harness.notifications.map((item) => item.method)).toEqual([
      "session/replayUpdate",
      "session/messageComplete",
      "session/operationDone",
    ])
    harness.notifications.length = 0

    harness.subscription.trackOperation(sessionID, "msg_not_submitted", "shell")
    await idle()
    await harness.subscription.acceptOperation(sessionID, "msg_not_submitted")
    expect(harness.notifications).toHaveLength(0)
    harness.subscription.rejectOperation(sessionID, "msg_not_submitted")

    await track("msg_root")
    await complete("msg_tool", "msg_root", "tool-calls")
    await track("msg_joined")
    await complete("msg_final", "msg_joined")
    await idle()
    expect(harness.notifications.map((item) => item.method)).toEqual([
      "session/messageComplete",
      "session/operationDone",
    ])
    expect(harness.notifications[0]?.params).toMatchObject({ messageId: "msg_final", parentMessageId: "msg_joined" })
    const failure = { name: "UnknownError", data: { message: "provider failed" } } as const
    await track("msg_error_old")
    await track("msg_error_new")
    await complete("msg_error", "msg_error_new", "error", failure)
    await idle()
    expect(harness.notifications.at(-1)?.params).toMatchObject({ parentMessageId: "msg_error_new", terminal: true })
    await track("msg_shell_failed", "shell")
    await track("msg_after_shell")
    await harness.subscription.failOperation(sessionID, "msg_shell_failed", "shell failed")
    await complete("msg_after", "msg_after_shell")
    await idle()
    expect(harness.notifications.slice(-3).map((item) => item.method)).toEqual([
      "session/operationFailed",
      "session/messageComplete",
      "session/operationDone",
    ])
    expect(harness.notifications.at(-3)?.params).toMatchObject({ parentMessageId: "msg_shell_failed", terminal: false })
    await track("msg_cancelled")
    harness.subscription.markCancelled(sessionID)
    await track("msg_after_cancel")
    await idle()
    await complete("msg_after_cancel_done", "msg_after_cancel")
    await idle()
    expect(harness.notifications.slice(-3).map((item) => item.params.parentMessageId)).toEqual([
      "msg_cancelled",
      "msg_after_cancel",
      "msg_after_cancel",
    ])
  })

  it("routes message.part.delta by sessionID without cross-session pollution", async () => {
    const harness = createHarness()
    await createKnownSession(harness.session, "ses_a", { messageId: "msg_a", partId: "part_a", partType: "text" })
    await createKnownSession(harness.session, "ses_b", { messageId: "msg_b", partId: "part_b", partType: "text" })

    await harness.subscription.handle(textDelta("ses_b", "msg_b", "part_b", "hello"))

    expect(harness.updates.map((update) => update.sessionId)).toEqual(["ses_b"])
    expect(harness.updates[0]?.update.sessionUpdate).toBe("agent_message_chunk")
  })

  it("keeps interleaved sessions isolated for text and reasoning deltas", async () => {
    const harness = createHarness()
    await createKnownSession(harness.session, "ses_a", { messageId: "msg_a", partId: "part_a", partType: "text" })
    await createKnownSession(harness.session, "ses_b", {
      messageId: "msg_b",
      partId: "part_b",
      partType: "reasoning",
    })

    await harness.subscription.handle(textDelta("ses_a", "msg_a", "part_a", "A1"))
    await harness.subscription.handle(textDelta("ses_b", "msg_b", "part_b", "B1"))
    await harness.subscription.handle(textDelta("ses_a", "msg_a", "part_a", "A2"))
    await harness.subscription.handle(textDelta("ses_b", "msg_b", "part_b", "B2"))

    expect(
      harness.updates.filter((update) => update.sessionId === "ses_a").map((update) => update.update.sessionUpdate),
    ).toEqual(["agent_message_chunk", "agent_message_chunk"])
    expect(
      harness.updates.filter((update) => update.sessionId === "ses_b").map((update) => update.update.sessionUpdate),
    ).toEqual(["agent_thought_chunk", "agent_thought_chunk"])
  })

  it("does not create extra subscriptions on repeated loadSession", async () => {
    const harness = createHarness()
    let subscription: ACPEvent.Subscription | undefined
    const service = ACPService.make({
      sdk: harness.sdk,
      connection: harness.connection,
      directory: {
        get: () =>
          Effect.succeed(
            Directory.build({
              directory: "/workspace",
              providers: {},
              modes: [],
              defaultModeID: "build",
              commands: [],
            }),
          ),
        refresh: () =>
          Effect.succeed(
            Directory.build({
              directory: "/workspace",
              providers: {},
              modes: [],
              defaultModeID: "build",
              commands: [],
            }),
          ),
        variants: Directory.variants,
      },
      session: harness.session,
      eventSubscription: (started) => {
        subscription = started
      },
    })

    await pollUntil(() => harness.calls.eventSubscribe === 1, "event subscription did not start")
    await Effect.runPromise(service.loadSession({ cwd: "/workspace", sessionId: "ses_loaded", mcpServers: [] }))
    await Effect.runPromise(service.loadSession({ cwd: "/workspace", sessionId: "ses_loaded", mcpServers: [] }))
    await Effect.runPromise(service.loadSession({ cwd: "/workspace", sessionId: "ses_loaded", mcpServers: [] }))

    expect(harness.calls.eventSubscribe).toBe(1)
    subscription?.stop()
    harness.events.close()
  })

  it("does not call sdk.session.message repeatedly when metadata is known", async () => {
    const harness = createHarness()
    await createKnownSession(harness.session, "ses_a", { messageId: "msg_a", partId: "part_a", partType: "text" })

    for (const delta of ["a", "b", "c", "d", "e"]) {
      await harness.subscription.handle(textDelta("ses_a", "msg_a", "part_a", delta))
    }

    expect(harness.calls.message).toBe(0)
    expect(harness.updates).toHaveLength(5)
  })

  it("fetches unknown part metadata once and reuses it for later deltas", async () => {
    const harness = createHarness({
      msg_a: assistantMessage("ses_a", "msg_a", "part_a", "text"),
    })
    await Effect.runPromise(harness.session.create({ id: "ses_a", cwd: "/workspace" }))

    await harness.subscription.handle(partUpdated("ses_a", "msg_a", "part_a", "text"))
    await harness.subscription.handle(textDelta("ses_a", "msg_a", "part_a", "a"))
    await harness.subscription.handle(textDelta("ses_a", "msg_a", "part_a", "b"))

    expect(harness.calls.message).toBe(1)
    expect(harness.updates).toHaveLength(2)
  })

  it("replays loaded session messages sequentially and continues after update failures", async () => {
    const events = createEventStream()
    const updates: SessionUpdateParams[] = []
    const connection = {
      sessionUpdate: (params: SessionUpdateParams) => {
        if (params.update.sessionUpdate === "tool_call" && params.update.toolCallId === "call_slow") {
          return new Promise<void>((resolve) => {
            setTimeout(() => {
              updates.push(params)
              resolve()
            }, 20)
          })
        }

        if (params.update.sessionUpdate === "tool_call_update" && params.update.toolCallId === "call_slow") {
          return Promise.reject(new Error("replay send failed"))
        }

        updates.push(params)
        return Promise.resolve()
      },
    } satisfies Pick<AgentSideConnection, "sessionUpdate">
    let subscription: ACPEvent.Subscription | undefined
    const service = ACPService.make({
      sdk: {
        global: {
          event: (options?: { signal?: AbortSignal }) => Promise.resolve({ stream: events.stream(options?.signal) }),
        },
        session: {
          get: () => Promise.resolve({ data: { id: "ses_loaded" } }),
          messages: () =>
            Promise.resolve({
              data: [
                assistantToolMessage(completedTool("ses_loaded", "call_slow", "slow")),
                assistantToolMessage(completedTool("ses_loaded", "call_after", "after")),
              ],
            }),
        },
      } as unknown as OpencodeClient,
      connection,
      directory: {
        get: () =>
          Effect.succeed(
            Directory.build({
              directory: "/workspace",
              providers: {},
              modes: [],
              defaultModeID: "build",
              commands: [],
            }),
          ),
        refresh: () =>
          Effect.succeed(
            Directory.build({
              directory: "/workspace",
              providers: {},
              modes: [],
              defaultModeID: "build",
              commands: [],
            }),
          ),
        variants: Directory.variants,
      },
      eventSubscription: (started) => {
        subscription = started
      },
    })

    await Effect.runPromise(service.loadSession({ cwd: "/workspace", sessionId: "ses_loaded", mcpServers: [] }))

    expect(toolUpdates(updates).map((item) => item.update.toolCallId)).toEqual([
      "call_slow",
      "call_after",
      "call_after",
    ])
    subscription?.stop()
    events.close()
  })

  it("ignores unknown sessions and live user parts without user_message_chunk duplication", async () => {
    const harness = createHarness()
    await createKnownSession(harness.session, "ses_user", {
      messageId: "msg_user",
      partId: "part_user",
      partType: "text",
      role: "user",
    })

    await harness.subscription.handle(textDelta("ses_missing", "msg_missing", "part_missing", "ignored"))
    await harness.subscription.handle(partUpdated("ses_user", "msg_user", "part_live", "text"))
    await harness.subscription.handle(textDelta("ses_user", "msg_user", "part_user", "hello"))

    expect(harness.updates).toHaveLength(0)
  })

  it("exposes the shell command on the synthetic pending tool call", async () => {
    const harness = createHarness()
    await Effect.runPromise(harness.session.create({ id: "ses_tool", cwd: "/workspace" }))

    await harness.subscription.handle(toolUpdated(runningTool("ses_tool", "call_1", "hello")))

    expect(toolUpdates(harness.updates).map((item) => item.update.sessionUpdate)).toEqual([
      "tool_call",
      "tool_call_update",
    ])
    expect(harness.updates[0]?.update).toMatchObject({
      status: "pending",
      toolCallId: "call_1",
      title: "printf hello",
      kind: "execute",
      locations: [{ path: "/workspace" }],
      rawInput: { cmd: "printf hello", cwd: "/workspace" },
    })
    expect(harness.updates[1]?.update).toMatchObject({ status: "in_progress", toolCallId: "call_1" })
  })

  it("includes available input in the synthetic pending tool call", async () => {
    const harness = createHarness()
    await Effect.runPromise(harness.session.create({ id: "ses_pending_input", cwd: "/workspace" }))

    await harness.subscription.handle(
      toolUpdated({
        id: "part_call_read",
        sessionID: "ses_pending_input",
        messageID: "msg_call_read",
        type: "tool",
        callID: "call_read",
        tool: "read",
        state: {
          status: "running",
          input: { filePath: "/workspace/file.ts" },
          title: "Read file.ts",
          time: { start: Date.now() },
        },
      } satisfies ToolPart),
    )

    expect(harness.updates[0]?.update).toMatchObject({
      sessionUpdate: "tool_call",
      toolCallId: "call_read",
      status: "pending",
      title: "Read file.ts",
      kind: "read",
      rawInput: { filePath: "/workspace/file.ts" },
      locations: [{ path: "/workspace/file.ts" }],
    })
  })

  it("does not emit duplicate synthetic pending after a replayed running tool", async () => {
    const harness = createHarness()
    await Effect.runPromise(harness.session.create({ id: "ses_replay", cwd: "/workspace" }))

    await harness.subscription.replayMessage(assistantToolMessage(runningTool("ses_replay", "call_replay", "first")))
    await harness.subscription.handle(toolUpdated(runningTool("ses_replay", "call_replay", "second")))

    expect(toolUpdates(harness.updates).filter((item) => item.update.sessionUpdate === "tool_call")).toHaveLength(1)
    expect(toolUpdates(harness.updates).map((item) => item.update.sessionUpdate)).toEqual([
      "tool_call",
      "tool_call_update",
      "tool_call_update",
    ])
  })

  it("dedupes shell output snapshots while still sending status-only running updates", async () => {
    const harness = createHarness()
    await Effect.runPromise(harness.session.create({ id: "ses_shell", cwd: "/workspace" }))

    await harness.subscription.handle(toolUpdated(runningTool("ses_shell", "call_shell", "same")))
    await harness.subscription.handle(toolUpdated(runningTool("ses_shell", "call_shell", "same")))

    const updates = toolUpdates(harness.updates)
    expect(updates).toHaveLength(3)
    expect(updates[1]?.update).toMatchObject({
      sessionUpdate: "tool_call_update",
      content: [{ type: "content", content: { type: "text", text: "same" } }],
    })
    expect(updates[2]?.update).toMatchObject({ sessionUpdate: "tool_call_update", status: "in_progress" })
    expect("content" in updates[2]!.update).toBe(false)
  })

  it("clears shell snapshot marker when a tool returns to pending", async () => {
    const harness = createHarness()
    await Effect.runPromise(harness.session.create({ id: "ses_pending", cwd: "/workspace" }))

    await harness.subscription.handle(toolUpdated(runningTool("ses_pending", "call_pending", "repeat")))
    await harness.subscription.handle(
      toolUpdated({
        id: "part_call_pending",
        sessionID: "ses_pending",
        messageID: "msg_call_pending",
        type: "tool",
        callID: "call_pending",
        tool: "bash",
        state: {
          status: "pending",
          input: { cmd: "printf repeat" },
          raw: '{"cmd":"printf repeat"}',
        },
      }),
    )
    await harness.subscription.handle(toolUpdated(runningTool("ses_pending", "call_pending", "repeat")))

    expect(
      toolUpdates(harness.updates)
        .filter((item) => item.update.sessionUpdate === "tool_call_update")
        .map((item) => ("content" in item.update ? item.update.content : undefined)),
    ).toEqual([
      [{ type: "content", content: { type: "text", text: "repeat" } }],
      [{ type: "content", content: { type: "text", text: "repeat" } }],
    ])
  })

  it("emits response-settled tool output once when delayed live updates arrive", async () => {
    const harness = createHarness()
    await Effect.runPromise(harness.session.create({ id: "ses_done", cwd: "/workspace" }))
    harness.subscription.trackOperation("ses_done", "msg_parent", "shell")
    await harness.subscription.acceptOperation("ses_done", "msg_parent")

    const completed = completedTool("ses_done", "call_done", "finished")
    await harness.subscription.settleShell("ses_done", "msg_parent", assistantToolMessage(completed))
    await harness.subscription.handle(toolUpdated(runningTool("ses_done", "call_done", "running")))
    await harness.subscription.handle(toolUpdated(completed))

    expect(toolUpdates(harness.updates)).toHaveLength(2)

    expect(harness.updates.at(-1)?.update).toMatchObject({
      sessionUpdate: "tool_call_update",
      toolCallId: "call_done",
      status: "completed",
      content: [{ type: "content", content: { type: "text", text: "finished" } }],
      rawOutput: { output: "finished", metadata: { exit: 0 } },
    })
  })

  it("emits clean read display content and preserves rawOutput", async () => {
    const harness = createHarness()
    await Effect.runPromise(harness.session.create({ id: "ses_read", cwd: "/workspace" }))
    const output = [
      "<path>/workspace/file.ts</path>",
      "<type>file</type>",
      "<content>",
      "1: import { value } from './value'",
      "2: export { value }",
      "",
      "(End of file - total 2 lines)",
      "</content>",
    ].join("\n")
    const metadata = {
      display: {
        type: "file",
        path: "/workspace/file.ts",
        text: "import { value } from './value'\nexport { value }",
        lineStart: 1,
        lineEnd: 2,
        totalLines: 2,
        truncated: false,
      },
    }

    await harness.subscription.handle(
      toolUpdated(
        completedTool("ses_read", "call_read", output, [], {
          tool: "read",
          input: { filePath: "/workspace/file.ts" },
          metadata,
        }),
      ),
    )

    expect(harness.updates.at(-1)?.update).toMatchObject({
      sessionUpdate: "tool_call_update",
      toolCallId: "call_read",
      status: "completed",
      content: [
        {
          type: "content",
          content: { type: "text", text: "import { value } from './value'\nexport { value }" },
        },
      ],
      rawOutput: { output, metadata },
    })
  })

  it("emits error tool output", async () => {
    const harness = createHarness()
    await Effect.runPromise(harness.session.create({ id: "ses_error", cwd: "/workspace" }))

    await harness.subscription.handle(toolUpdated(errorTool("ses_error", "call_error")))

    expect(harness.updates.at(-1)?.update).toMatchObject({
      sessionUpdate: "tool_call_update",
      toolCallId: "call_error",
      status: "failed",
      content: [{ type: "content", content: { type: "text", text: "failed hard" } }],
      rawOutput: { error: "failed hard", metadata: { exit: 1 } },
    })
  })

  it("emits image attachments as ACP image content for live and replayed completed tool updates", async () => {
    const harness = createHarness()
    const image = Buffer.from("image-data").toString("base64")
    const attachment = {
      id: "file_image",
      sessionID: "ses_image",
      messageID: "msg_image",
      type: "file",
      mime: "image/png",
      filename: "image.png",
      url: `data:image/png;base64,${image}`,
    } as const
    await Effect.runPromise(harness.session.create({ id: "ses_image", cwd: "/workspace" }))

    await harness.subscription.handle(toolUpdated(completedTool("ses_image", "call_live", "live", [attachment])))
    await harness.subscription.replayMessage(
      assistantToolMessage(completedTool("ses_image", "call_replayed", "replayed", [attachment])),
    )

    expect(
      toolUpdates(harness.updates)
        .filter((item) => item.update.sessionUpdate === "tool_call_update" && item.update.status === "completed")
        .map((item) => ("content" in item.update ? item.update.content : [])),
    ).toEqual([
      [
        { type: "content", content: { type: "text", text: "live" } },
        { type: "content", content: { type: "image", mimeType: "image/png", data: image } },
      ],
      [
        { type: "content", content: { type: "text", text: "replayed" } },
        { type: "content", content: { type: "image", mimeType: "image/png", data: image } },
      ],
    ])
  })
})
