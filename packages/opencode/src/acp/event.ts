import type { AgentSideConnection } from "@agentclientprotocol/sdk"
import type {
  Event,
  EventMessagePartDelta,
  EventMessagePartUpdated,
  OpencodeClient,
  Part,
  SessionMessageResponse,
  ToolPart,
} from "@opencode-ai/sdk/v2"
import { Effect } from "effect"
import { ACPSession } from "./session"
import { ACPPermission } from "./permission"
import { partsToContentChunks, type ReplayPart } from "./content"
import {
  completedToolUpdate,
  duplicateRunningToolUpdate,
  errorToolUpdate,
  pendingToolCall,
  runningToolUpdate,
  shellOutputSnapshot,
} from "./tool"

type Connection = Pick<AgentSideConnection, "sessionUpdate"> &
  Partial<Pick<AgentSideConnection, "extNotification" | "requestPermission" | "writeTextFile">>
type Operation = {
  kind: "prompt" | "shell"
  admitted: boolean
  submitted: boolean
}
type AssistantCompletion = {
  id: string
  parentID: string
  rootID?: string
  stopReason?: string
  error?: unknown
  summary?: boolean
}
type GlobalEventEnvelope = { payload?: Event }
type GlobalEventStream = { stream: AsyncIterable<GlobalEventEnvelope> }

const SCRIPTIT_COMPACTION_SUMMARY_META = { scriptit: { kind: "compaction_summary" } } as const

export function start(input: { sdk: OpencodeClient; connection: Connection; session: ACPSession.Interface }) {
  const subscription = new Subscription(input)
  subscription.start()
  return subscription
}

export class Subscription {
  private readonly abort = new AbortController()
  private readonly shellSnapshots = new Map<string, string>()
  private readonly toolStarts = new Set<string>()
  private readonly connectionWaiters = new Set<() => void>()
  private readonly idleWaiters = new Map<string, Set<ReturnType<typeof signal>>>()
  private readonly operations = new Map<string, Map<string, Operation>>()
  private readonly messageRoots = new Map<string, Map<string, string>>()
  private readonly completions = new Map<string, Map<string, AssistantCompletion>>()
  private readonly completedMessages = new Map<string, Map<string, AssistantCompletion>>()
  private readonly endTurnMessages = new Set<string>()
  private readonly cancelled = new Map<string, string>()
  private readonly pendingErrors = new Map<string, unknown>()
  private readonly settlementQueues = new Map<string, Promise<void>>()
  private readonly idleBeforeAdmission = new Map<string, Set<string>>()
  private readonly compactionSummaries = new Set<string>()
  private readonly responseSettledTools = new Set<string>()
  private readonly permission: ACPPermission.Handler
  private connected = false
  private started = false

  constructor(
    private readonly input: {
      sdk: OpencodeClient
      connection: Connection
      session: ACPSession.Interface
    },
  ) {
    this.permission = new ACPPermission.Handler(input)
  }

  start() {
    if (this.started) return
    this.started = true
    this.run().catch(() => {
      if (this.abort.signal.aborted) return
    })
  }

  stop() {
    this.abort.abort()
    this.disconnected()
    for (const resolve of this.connectionWaiters) resolve()
    this.connectionWaiters.clear()
  }

  ready() {
    return this.waitUntilConnected()
  }

  async runUntilIdle<A>(sessionId: string, request: () => Promise<A>) {
    await this.waitUntilConnected()
    const waiter = signal()
    const waiters = this.idleWaiters.get(sessionId) ?? new Set()
    waiters.add(waiter)
    this.idleWaiters.set(sessionId, waiters)

    try {
      void waiter.promise.catch(() => {})
      const response = await request()
      await waiter.promise
      return response
    } finally {
      waiters.delete(waiter)
      if (waiters.size === 0) this.idleWaiters.delete(sessionId)
    }
  }

  trackOperation(sessionId: string, parentMessageId: string, kind: Operation["kind"]) {
    const operations = this.operations.get(sessionId) ?? new Map<string, Operation>()
    operations.set(parentMessageId, { kind, admitted: false, submitted: false })
    this.operations.set(sessionId, operations)
    this.roots(sessionId).set(parentMessageId, parentMessageId)
  }

  markOperationSubmitted(sessionId: string, parentMessageId: string) {
    const operation = this.operations.get(sessionId)?.get(parentMessageId)
    if (operation) operation.submitted = true
  }

  async acceptOperation(sessionId: string, parentMessageId: string) {
    const operation = this.operations.get(sessionId)?.get(parentMessageId)
    if (!operation) return
    operation.admitted = true
    const deferred = this.idleBeforeAdmission.get(sessionId)
    if (!deferred?.delete(parentMessageId)) return
    if (deferred.size === 0) this.idleBeforeAdmission.delete(sessionId)
    await this.enqueue(sessionId, () => this.settle(sessionId))
  }

  rejectOperation(sessionId: string, parentMessageId: string) {
    const operations = this.operations.get(sessionId)
    operations?.delete(parentMessageId)
    this.completions.get(sessionId)?.delete(parentMessageId)
    const deferred = this.idleBeforeAdmission.get(sessionId)
    deferred?.delete(parentMessageId)
    if (deferred?.size === 0) this.idleBeforeAdmission.delete(sessionId)
    if (operations?.size === 0) this.clearOperation(sessionId)
  }

  markCancelled(sessionId: string) {
    const parentMessageId = [...(this.operations.get(sessionId)?.keys() ?? [])].at(-1)
    if (parentMessageId) this.cancelled.set(sessionId, parentMessageId)
  }

  settleShell(sessionId: string, parentMessageId: string, message: SessionMessageResponse) {
    return this.enqueue(sessionId, async () => {
      if (!this.operations.get(sessionId)?.has(parentMessageId)) return
      const session = await Effect.runPromise(this.input.session.tryGet(sessionId))
      if (!session) return
      for (const part of message.parts) {
        await this.recordFetchedPart(sessionId, message, part)
        if (part.type !== "tool") continue
        if (part.state.status !== "completed" && part.state.status !== "error") continue
        await this.handleToolPart(sessionId, part, session.cwd)
        this.responseSettledTools.add(part.callID)
      }
      if (message.info.role === "assistant") {
        this.recordAssistantCompletion(sessionId, {
          id: message.info.id,
          parentID: message.info.parentID,
          rootID: parentMessageId,
          stopReason: message.info.finish,
          error: message.info.error,
          summary: message.info.summary,
        })
      }
      await this.settle(sessionId)
    })
  }

  failOperation(sessionId: string, parentMessageId: string, message: string) {
    return this.enqueue(sessionId, async () => {
      const operations = this.operations.get(sessionId)
      if (!operations) return
      const operation = operations.get(parentMessageId)
      if (!operation) return
      operations.delete(parentMessageId)
      this.completions.get(sessionId)?.delete(parentMessageId)
      this.pendingErrors.delete(sessionId)
      const terminal = operations.size === 0
      if (terminal) this.clearOperation(sessionId)
      await this.sendExtension("session/operationFailed", {
        sessionId,
        parentMessageId,
        op: operation.kind,
        message,
        terminal,
      })
    })
  }

  async handle(event: Event) {
    switch (event.type) {
      case "session.status":
        if (event.properties.status.type === "idle") {
          const sessionId = event.properties.sessionID
          this.rememberUnadmitted(sessionId)
          await this.enqueue(sessionId, () => this.settle(sessionId))
          this.idle(sessionId)
        }
        return
      case "permission.asked":
        this.permission.handle(event)
        return
      case "session.error": {
        const sessionId = event.properties.sessionID
        if (sessionId && this.operations.has(sessionId)) {
          this.pendingErrors.set(sessionId, event.properties.error ?? "Session error")
        }
        return
      }
      case "message.part.updated":
        return this.handlePartUpdated(event)
      case "message.part.delta":
        return this.handlePartDelta(event)
      case "message.updated": {
        const info = event.properties.info
        if (info.role === "user") {
          this.recordUserMessage(info.sessionID, info.id)
          return
        }
        if (info.summary === true) this.compactionSummaries.add(info.id)
        if (!info.time.completed) return
        this.recordAssistantCompletion(info.sessionID, {
          id: info.id,
          parentID: info.parentID,
          stopReason: info.finish,
          error: info.error,
          summary: info.summary,
        })
        return
      }
    }
  }

  async replayMessage(message: SessionMessageResponse) {
    if (message.info.role !== "assistant" && message.info.role !== "user") return

    const cwd = message.info.role === "assistant" ? message.info.path?.cwd : undefined
    for (const part of message.parts) {
      await this.recordFetchedPart(message.info.sessionID, message, part)
      if (part.type === "tool") {
        await this.handleToolPart(message.info.sessionID, part, cwd ?? process.cwd(), true)
        continue
      }
      await this.replayContentPart(message, part)
    }
  }

  private async replayContentPart(message: SessionMessageResponse, part: Part) {
    if (part.type !== "text" && part.type !== "file" && part.type !== "reasoning") return

    const sessionUpdate =
      part.type === "reasoning"
        ? "agent_thought_chunk"
        : message.info.role === "user"
          ? "user_message_chunk"
          : "agent_message_chunk"

    for (const chunk of partsToContentChunks([part as ReplayPart])) {
      await this.sendUpdate(
        {
          sessionId: message.info.sessionID,
          update: {
            sessionUpdate,
            messageId: message.info.id,
            ...(message.info.role === "assistant" && message.info.summary === true
              ? { _meta: SCRIPTIT_COMPACTION_SUMMARY_META }
              : {}),
            ...chunk,
          },
        },
        true,
      )
    }
  }

  private async run() {
    while (!this.abort.signal.aborted) {
      await this.consume().catch(() => {})
      this.disconnected()
      if (!this.abort.signal.aborted) await new Promise((resolve) => setTimeout(resolve, 1000))
    }
  }

  private async consume() {
    const events = (await this.input.sdk.global.event({ signal: this.abort.signal })) as GlobalEventStream

    for await (const event of events.stream) {
      if (this.abort.signal.aborted) return
      if (!event.payload) continue
      if (event.payload.type === "server.connected") {
        this.connected = true
        for (const resolve of this.connectionWaiters) resolve()
        this.connectionWaiters.clear()
        continue
      }
      if (!this.connected) continue
      await this.handle(event.payload).catch(() => {})
    }
  }

  private async waitUntilConnected() {
    while (!this.connected) {
      if (this.abort.signal.aborted) throw new Error("ACP event subscription stopped")
      await new Promise<void>((resolve) => this.connectionWaiters.add(resolve))
    }
  }

  private disconnected() {
    if (!this.connected) return
    this.connected = false
    const error = new Error("ACP event stream disconnected")
    for (const waiters of this.idleWaiters.values()) {
      for (const waiter of waiters) waiter.reject(error)
    }
    this.idleWaiters.clear()
  }

  private idle(sessionId: string) {
    const waiters = this.idleWaiters.get(sessionId)
    if (!waiters) return
    this.idleWaiters.delete(sessionId)
    for (const waiter of waiters) waiter.resolve()
  }

  private recordUserMessage(sessionId: string, messageId: string) {
    const operations = this.operations.get(sessionId)
    if (!operations) return
    if (operations.has(messageId)) {
      this.roots(sessionId).set(messageId, messageId)
      return
    }
    const root = [...operations.entries()].findLast(([, operation]) => operation.kind === "prompt")?.[0]
    if (root) this.roots(sessionId).set(messageId, root)
  }

  private recordAssistantCompletion(sessionId: string, message: AssistantCompletion) {
    const operations = this.operations.get(sessionId)
    if (!operations) return
    const rootID =
      message.rootID ??
      (operations.has(message.parentID) ? message.parentID : this.messageRoots.get(sessionId)?.get(message.parentID))
    const completed = this.completedMessages.get(sessionId) ?? new Map<string, AssistantCompletion>()
    const recorded = { ...message, rootID }
    completed.set(message.id, recorded)
    this.completedMessages.set(sessionId, completed)
    if (!rootID || (message.summary && message.error === undefined)) return
    if (message.stopReason === "tool-calls" && !this.endTurnMessages.has(message.id)) return

    const completions = this.completions.get(sessionId) ?? new Map<string, AssistantCompletion>()
    completions.set(rootID, this.endTurnMessages.has(message.id) ? { ...recorded, stopReason: "end_turn" } : recorded)
    this.completions.set(sessionId, completions)
  }

  private async settle(sessionId: string) {
    const operations = this.operations.get(sessionId)
    if (!operations) return
    const cancelledParentMessageId = this.cancelled.get(sessionId)
    if (cancelledParentMessageId) {
      operations.delete(cancelledParentMessageId)
      this.completions.get(sessionId)?.delete(cancelledParentMessageId)
      this.cancelled.delete(sessionId)
      this.pendingErrors.delete(sessionId)
      if (operations.size === 0) this.clearOperation(sessionId)
      await this.sendExtension("session/operationDone", { sessionId, parentMessageId: cancelledParentMessageId })
      return
    }

    const completions = this.completions.get(sessionId)
    let lastCompletedRoot: string | undefined
    let settledOperation = false
    for (const [parentMessageId, operation] of [...operations]) {
      if (!operation.admitted) continue
      const message = completions?.get(parentMessageId)
      if (!message) continue
      if (operation.kind === "prompt") {
        for (const [joinedMessageId, joinedOperation] of operations) {
          if (joinedMessageId === parentMessageId) break
          if (joinedOperation.kind !== "prompt") continue
          operations.delete(joinedMessageId)
          completions?.delete(joinedMessageId)
        }
      }
      this.pendingErrors.delete(sessionId)
      if (message.error !== undefined) {
        settledOperation = true
        const terminal = operations.size === 1
        await this.sendExtension("session/operationFailed", {
          sessionId,
          parentMessageId,
          op: operation.kind,
          message: errorMessage(message.error),
          terminal,
        })
        operations.delete(parentMessageId)
        completions?.delete(parentMessageId)
        if (terminal) {
          this.clearOperation(sessionId)
          return
        }
        continue
      }

      await this.sendExtension("session/messageComplete", {
        sessionId,
        messageId: message.id,
        parentMessageId,
        ...(operation.kind === "prompt" ? { stopReason: message.stopReason ?? "end_turn" } : {}),
      })
      settledOperation = true
      operations.delete(parentMessageId)
      completions?.delete(parentMessageId)
      lastCompletedRoot = parentMessageId
    }

    if (operations.size === 0 && lastCompletedRoot) {
      this.clearOperation(sessionId)
      await this.sendExtension("session/operationDone", { sessionId, parentMessageId: lastCompletedRoot })
      return
    }

    const unresolved = [...operations.entries()].filter(([, operation]) => operation.admitted)
    if (!settledOperation && unresolved.length === 1 && !completions?.size) {
      const [parentMessageId, operation] = unresolved[0]
      const error = this.pendingErrors.get(sessionId)
      this.clearOperation(sessionId)
      await this.sendExtension("session/operationFailed", {
        sessionId,
        parentMessageId,
        op: operation.kind,
        message: error === undefined ? "Session became idle before completion" : errorMessage(error),
        terminal: true,
      })
    }
  }

  private async handlePartUpdated(event: EventMessagePartUpdated) {
    const part = event.properties.part
    const sessionId = part.sessionID || event.properties.sessionID
    const session = await Effect.runPromise(this.input.session.tryGet(sessionId))
    if (!session) return

    await Effect.runPromise(
      this.input.session.recordPartMetadata({
        sessionId: session.id,
        messageId: part.messageID,
        partId: part.id,
        partType: part.type,
        role: part.type === "reasoning" ? "assistant" : undefined,
        summary: this.compactionSummaries.has(part.messageID),
        ignored: part.type === "text" ? part.ignored : undefined,
        toolCallId: part.type === "tool" ? part.callID : undefined,
        metadata: "metadata" in part ? part.metadata : undefined,
      }),
    )
    if (part.type === "tool") await this.handleToolPart(session.id, part, session.cwd)
  }

  private async handlePartDelta(event: EventMessagePartDelta) {
    const props = event.properties
    const session = await Effect.runPromise(this.input.session.tryGet(props.sessionID))
    if (!session) return

    const known = await Effect.runPromise(
      this.input.session.tryGetPartMetadata({
        sessionId: session.id,
        messageId: props.messageID,
        partId: props.partID,
      }),
    )
    const metadata =
      known?.role && known.partType
        ? known
        : await this.fetchPartMetadata(session.id, session.cwd, props.messageID, props.partID)
    if (metadata?.role !== "assistant") return
    if (metadata.partType === "text" && props.field === "text" && metadata.ignored !== true) {
      await this.sendUpdate({
        sessionId: session.id,
        update: {
          sessionUpdate: "agent_message_chunk",
          messageId: props.messageID,
          ...(metadata.summary ? { _meta: SCRIPTIT_COMPACTION_SUMMARY_META } : {}),
          content: { type: "text", text: props.delta },
        },
      })
      return
    }

    if (metadata.partType === "reasoning" && props.field === "text") {
      await this.sendUpdate({
        sessionId: session.id,
        update: {
          sessionUpdate: "agent_thought_chunk",
          messageId: props.messageID,
          ...(metadata.summary ? { _meta: SCRIPTIT_COMPACTION_SUMMARY_META } : {}),
          content: { type: "text", text: props.delta },
        },
      })
    }
  }

  private async fetchPartMetadata(sessionId: string, cwd: string, messageId: string, partId: string) {
    const message = await this.input.sdk.session
      .message({ sessionID: sessionId, messageID: messageId, directory: cwd }, { throwOnError: true })
      .then((response) => response.data)
      .catch(() => undefined)
    if (!message) return
    const part = message.parts.find((item) => item.id === partId)
    if (!part) return
    return await this.recordFetchedPart(sessionId, message, part)
  }

  private async recordFetchedPart(sessionId: string, message: SessionMessageResponse, part: Part) {
    return await Effect.runPromise(
      this.input.session.recordPartMetadata({
        sessionId,
        messageId: part.messageID,
        partId: part.id,
        partType: part.type,
        role: message.info.role,
        summary: message.info.role === "assistant" && message.info.summary === true,
        ignored: part.type === "text" ? part.ignored : undefined,
        toolCallId: part.type === "tool" ? part.callID : undefined,
        metadata: "metadata" in part ? part.metadata : undefined,
      }),
    )
  }

  private async handleToolPart(sessionId: string, part: ToolPart, cwd: string, replay = false) {
    if (!replay && this.responseSettledTools.has(part.callID)) {
      if (part.state.status === "completed" || part.state.status === "error") {
        this.responseSettledTools.delete(part.callID)
      }
      return
    }
    await this.toolStart(sessionId, part, cwd, replay)

    switch (part.state.status) {
      case "pending":
        this.shellSnapshots.delete(part.callID)
        return
      case "running":
        await this.runningTool(sessionId, part, cwd, replay)
        return
      case "completed": {
        if (!replay && endTurn(part)) {
          this.endTurnMessages.add(part.messageID)
          const message = this.completedMessages.get(sessionId)?.get(part.messageID)
          if (message) this.recordAssistantCompletion(sessionId, { ...message, stopReason: "end_turn" })
        }
        this.clearTool(part.callID)
        await this.sendUpdate(
          {
            sessionId,
            update: {
              sessionUpdate: "tool_call_update",
              ...completedToolUpdate({
                toolCallId: part.callID,
                messageId: part.messageID,
                toolName: part.tool,
                state: part.state,
                cwd,
              }),
            },
          },
          replay,
        )
        return
      }
      case "error":
        this.clearTool(part.callID)
        await this.sendUpdate(
          {
            sessionId,
            update: {
              sessionUpdate: "tool_call_update",
              ...errorToolUpdate({
                toolCallId: part.callID,
                messageId: part.messageID,
                toolName: part.tool,
                state: part.state,
                cwd,
              }),
            },
          },
          replay,
        )
        return
    }
  }

  private async runningTool(sessionId: string, part: ToolPart, cwd: string, replay: boolean) {
    if (part.state.status !== "running") return
    const output = part.tool === "bash" ? shellOutputSnapshot(part.state) : undefined
    if (output !== undefined) {
      if (this.shellSnapshots.get(part.callID) === output) {
        await this.sendUpdate(
          {
            sessionId,
            update: {
              sessionUpdate: "tool_call_update",
              ...duplicateRunningToolUpdate({
                toolCallId: part.callID,
                messageId: part.messageID,
                toolName: part.tool,
                state: part.state,
                cwd,
              }),
            },
          },
          replay,
        )
        return
      }
      this.shellSnapshots.set(part.callID, output)
    }

    await this.sendUpdate(
      {
        sessionId,
        update: {
          sessionUpdate: "tool_call_update",
          ...runningToolUpdate({
            toolCallId: part.callID,
            messageId: part.messageID,
            toolName: part.tool,
            state: part.state,
            output,
            cwd,
          }),
        },
      },
      replay,
    )
  }

  private async toolStart(sessionId: string, part: ToolPart, cwd: string, replay: boolean) {
    if (this.toolStarts.has(part.callID)) return
    this.toolStarts.add(part.callID)
    await this.sendUpdate(
      {
        sessionId,
        update: {
          sessionUpdate: "tool_call",
          ...pendingToolCall({
            toolCallId: part.callID,
            messageId: part.messageID,
            toolName: part.tool,
            state: part.state,
            cwd,
          }),
        },
      },
      replay,
    )
  }

  private roots(sessionId: string) {
    const roots = this.messageRoots.get(sessionId) ?? new Map<string, string>()
    this.messageRoots.set(sessionId, roots)
    return roots
  }

  private rememberUnadmitted(sessionId: string) {
    const operations = this.operations.get(sessionId)
    if (!operations) return
    const deferred = this.idleBeforeAdmission.get(sessionId) ?? new Set<string>()
    for (const [parentMessageId, operation] of operations) {
      if (operation.submitted && !operation.admitted) deferred.add(parentMessageId)
    }
    if (deferred.size > 0) this.idleBeforeAdmission.set(sessionId, deferred)
  }

  private clearOperation(sessionId: string) {
    this.operations.delete(sessionId)
    this.messageRoots.delete(sessionId)
    this.completions.delete(sessionId)
    this.completedMessages.delete(sessionId)
    this.cancelled.delete(sessionId)
    this.pendingErrors.delete(sessionId)
    this.idleBeforeAdmission.delete(sessionId)
  }

  private enqueue(sessionId: string, run: () => Promise<void>) {
    const previous = this.settlementQueues.get(sessionId) ?? Promise.resolve()
    const next = previous
      .catch(() => {})
      .then(run)
      .finally(() => {
        if (this.settlementQueues.get(sessionId) === next) this.settlementQueues.delete(sessionId)
      })
    this.settlementQueues.set(sessionId, next)
    return next
  }

  private sendUpdate(params: Parameters<AgentSideConnection["sessionUpdate"]>[0], replay = false) {
    if (replay && this.input.connection.extNotification) {
      return this.input.connection.extNotification("session/replayUpdate", {
        sessionId: params.sessionId,
        update: params.update,
      })
    }
    return this.input.connection.sessionUpdate(params)
  }

  private sendExtension(method: string, params: Record<string, unknown>) {
    if (!this.input.connection.extNotification) return Promise.resolve()
    return this.input.connection.extNotification(method, params)
  }

  private clearTool(toolCallId: string) {
    this.toolStarts.delete(toolCallId)
    this.shellSnapshots.delete(toolCallId)
  }
}

function errorMessage(error: unknown): string {
  if (typeof error === "string") return error
  if (!error || typeof error !== "object") return "Session error"
  if ("message" in error && typeof error.message === "string") return error.message
  if ("data" in error) return errorMessage(error.data)
  if ("error" in error) return errorMessage(error.error)
  return "Session error"
}

function endTurn(part: ToolPart) {
  if (part.state.status !== "completed") return false
  if (!part.state.metadata || typeof part.state.metadata !== "object") return false
  return "endTurn" in part.state.metadata && part.state.metadata.endTurn === true
}

function signal() {
  const state: { resolve: () => void; reject: (reason?: unknown) => void } = {
    resolve: () => {},
    reject: () => {},
  }
  const promise = new Promise<void>((resolve, reject) => {
    state.resolve = resolve
    state.reject = reject
  })
  return {
    promise,
    resolve: () => state.resolve(),
    reject: (reason?: unknown) => state.reject(reason),
  }
}

export * as ACPEvent from "./event"
