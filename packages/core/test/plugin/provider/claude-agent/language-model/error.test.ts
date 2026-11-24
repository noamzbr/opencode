import { describe, expect, test } from "bun:test"
import type { LanguageModelV3StreamPart } from "@ai-sdk/provider"
import type { SDKAssistantMessage, SDKMessage, SDKRateLimitEvent } from "@anthropic-ai/claude-agent-sdk"
import { AIError } from "@opencode/ai/effect"
import { ClaudeProbe } from "@opencode/core/plugin/provider/claude-agent/claude/probe"
import { ClaudeAgentError } from "@opencode/core/plugin/provider/claude-agent/language-model/claude-agent-error"
import { ConvertClaudeStream } from "@opencode/core/plugin/provider/claude-agent/language-model/convert-claude-stream"
import { toSessionError } from "@opencode/core/session/to-session-error"
import { Recorder } from "../lib/recorder"

function assistant() {
  const line = Recorder.load("api-error").find(
    (line) => line.from === "claude" && line.message.type === "assistant" && line.message.error,
  )!
  if (line.from !== "claude") throw new Error("Expected a recorded assistant error")
  return line.message as SDKAssistantMessage
}

describe("terminal errors", () => {
  test("a recorded failed turn ends in an error after its text, without a finish", () => {
    const convert = ConvertClaudeStream.make([])
    const parts: LanguageModelV3StreamPart[] = []
    for (const line of Recorder.load("text-cancel")) {
      if (line.from !== "claude" || line.message.type === "control_response") continue
      parts.push(...convert(line.message as SDKMessage))
      if (line.message.type === "result") break
    }
    expect(parts.some((part) => part.type === "text-delta")).toBe(true)
    expect(parts.at(-1)?.type).toBe("error")
    expect(parts.some((part) => part.type === "finish")).toBe(false)
  })

  test("a recorded API error is an error, not answer text", () => {
    const convert = ConvertClaudeStream.make([])
    expect(convert(assistant())).toEqual([])
    const parts = convert(Recorder.result("api-error"))
    expect(parts).toEqual([{ type: "error", error: expect.any(Error) }])
    if (parts[0]?.type === "error") expect(String(parts[0].error)).toContain("claude-does-not-exist")
  })

  test("a rejected weekly window gives its reset date, while allowed windows do not fail", () => {
    const info = {
      type: "rate_limit_event",
      uuid: crypto.randomUUID(),
      session_id: crypto.randomUUID(),
      rate_limit_info: { status: "rejected", rateLimitType: "seven_day_sonnet", resetsAt: 1792800000 },
    } satisfies SDKRateLimitEvent
    const convert = ConvertClaudeStream.make([])
    expect(convert(info)).toEqual([])
    expect(convert({ ...assistant(), error: "rate_limit" } as const)).toEqual([])
    const failed = {
      ...Recorder.result("text"),
      subtype: "success",
      is_error: true,
      result: "Weekly limit reached.",
    } as const
    const parts = convert(failed)
    if (parts[0]?.type !== "error") throw new Error("Expected a limit error")
    expect(String(parts[0].error)).toContain(new Date(info.rate_limit_info.resetsAt * 1000).toISOString())
    for (const status of ["allowed", "allowed_warning", "rejected"] as const) {
      const successful = ConvertClaudeStream.make([])
      successful({ ...info, rate_limit_info: { ...info.rate_limit_info, status, overageStatus: "allowed" } } as const)
      expect(successful(Recorder.result("text")).at(-1)?.type).toBe("finish")
    }
    const recovered = ConvertClaudeStream.make([])
    recovered(info)
    recovered({ ...info, rate_limit_info: { status: "allowed", overageStatus: "rejected" } } as const)
    const later = recovered({ ...failed, api_error_status: 429 })
    if (later[0]?.type !== "error") throw new Error("Expected a terminal error")
    expect(String(later[0].error)).not.toContain("Resets at")
  })

  test("empty successes after API retries report the terminal HTTP status", () => {
    for (const status of [500, 529]) {
      const convert = ConvertClaudeStream.make([])
      const result = {
        ...Recorder.result("text"),
        subtype: "success",
        is_error: false,
        result: "",
        api_error_status: status,
      } as const
      const parts = convert(result)
      if (parts[0]?.type !== "error") throw new Error("Expected an API error")
      expect(String(parts[0].error)).toContain(String(status))
      expect(String(parts[0].error)).toContain("retries")
      expect(parts.some((part) => part.type === "finish")).toBe(false)
    }
    const empty = {
      ...Recorder.result("text"),
      subtype: "success",
      result: "",
      is_error: false,
      api_error_status: null,
    } as const
    expect(ConvertClaudeStream.make([])(empty).at(-1)?.type).toBe("finish")
  })
})

describe("session errors", () => {
  // What a session records for a failed Claude request, as the wire carries it.
  const recorded = (failure: ClaudeAgentError.Failure) => toSessionError(ClaudeAgentError.toAIError(failure))
  const body = (error: ReturnType<typeof recorded>) => JSON.parse(error.response?.body ?? "null")

  test("a plan limit is a rate limit that names its window and reset", () => {
    const resetsAt = Math.floor(Date.now() / 1000) + 3600
    const convert = ConvertClaudeStream.make([])
    convert({
      type: "rate_limit_event",
      uuid: crypto.randomUUID(),
      session_id: crypto.randomUUID(),
      rate_limit_info: { status: "rejected", rateLimitType: "five_hour", resetsAt },
    } satisfies SDKRateLimitEvent)
    convert({ ...assistant(), error: "rate_limit" } as const)
    const parts = convert({ ...Recorder.result("text"), subtype: "success", is_error: true, result: "" } as const)
    if (parts[0]?.type !== "error") throw new Error("Expected a limit error")
    const error = toSessionError(parts[0].error)
    expect(error).toMatchObject({ type: "provider.rate-limit", status: 429 })
    expect(error.message).toContain("Claude Code subscription limit reached (five hour).")
    expect(body(error)).toEqual({
      claude_agent: { code: "rate_limited", window: "five_hour", resetsAt: new Date(resetsAt * 1000).toISOString() },
    })
    expect(error.retryAfterMs).toBeGreaterThan(3_500_000)
  })

  test("an Anthropic server error is a provider error with its status", () => {
    for (const status of [500, 529]) {
      const parts = ConvertClaudeStream.make([])({
        ...Recorder.result("text"),
        subtype: "success",
        is_error: false,
        result: "",
        api_error_status: status,
      } as const)
      if (parts[0]?.type !== "error") throw new Error("Expected an API error")
      const error = toSessionError(parts[0].error)
      expect(error).toMatchObject({ type: "provider.internal", status })
      expect(body(error)).toEqual({ claude_agent: { code: "upstream_error", status } })
    }
  })

  test("a refused login, a missing claude and an old claude name their cause", () => {
    const login = recorded(new ClaudeAgentError.Failure(ClaudeProbe.loginMessage(undefined), false, { code: "not_logged_in" }))
    expect(login).toMatchObject({
      type: "provider.auth",
      status: 401,
      message: "Claude Code is not logged in. Run `claude auth login` in a terminal.",
    })
    expect(body(login)).toEqual({ claude_agent: { code: "not_logged_in" } })

    const missing = recorded(
      new ClaudeAgentError.Failure("Install Claude Code and run `claude auth login` first", false, { code: "not_installed" }),
    )
    expect(missing).toMatchObject({ type: "provider.unknown" })
    expect(missing).not.toHaveProperty("status")
    expect(body(missing)).toEqual({ claude_agent: { code: "not_installed" } })

    const old = ClaudeProbe.versionError("2.1.100")
    if (!(old instanceof ClaudeAgentError.Failure)) throw new Error("Expected a version failure")
    const outdated = recorded(old)
    expect(outdated).toMatchObject({
      type: "provider.unknown",
      message: "Claude Code 2.1.100 is too old; 2.1.248 or later is required. Run `claude update`.",
    })
    expect(body(outdated)).toEqual({ claude_agent: { code: "too_old", version: "2.1.100", minimum: "2.1.248" } })
    expect(ClaudeProbe.versionError("2.1.301")).toBeUndefined()
  })

  test("a refused login without an HTTP status is still an auth failure, and no cause asks for a retry", () => {
    const convert = ConvertClaudeStream.make([])
    // Claude Code reports an expired login on the assistant message, with no api_error_status of its own.
    convert({ ...assistant(), error: "authentication_failed" } as const)
    const parts = convert({
      ...Recorder.result("text"),
      subtype: "success",
      is_error: true,
      result: "Please run /login",
      api_error_status: null,
    } as const)
    if (parts[0]?.type !== "error") throw new Error("Expected an auth error")
    const error = toSessionError(parts[0].error)
    expect(error).toMatchObject({ type: "provider.auth", status: 401 })
    expect(error.message).toContain("Run `claude auth login` in a terminal")
    expect(body(error)).toEqual({ claude_agent: { code: "not_logged_in" } })
    // Claude Code owns every retry before this result, so none of these asks OpenCode for another one.
    for (const failure of [
      new ClaudeAgentError.Failure("limited", true, { code: "rate_limited" }),
      new ClaudeAgentError.Failure("overloaded", false, { code: "upstream_error", status: 529 }),
      new ClaudeAgentError.Failure("login", false, { code: "not_logged_in" }),
    ]) {
      const aiError = ClaudeAgentError.toAIError(failure)
      if (!(aiError instanceof AIError)) throw new Error("Expected an AI error")
      expect(aiError.reason.http?.headers["x-should-retry"]).toBe("false")
      expect(aiError.isRetryable).toBe(false)
    }
  })

  test("a failure that names no cause keeps its own classification", () => {
    const parts = ConvertClaudeStream.make([])({
      ...Recorder.result("text"),
      subtype: "success",
      is_error: true,
      result: "Prompt is too long",
      api_error_status: 400,
    } as const)
    if (parts[0]?.type !== "error") throw new Error("Expected a terminal error")
    expect(parts[0].error).toBeInstanceOf(ClaudeAgentError.Failure)
    expect(parts[0].error).toMatchObject({ message: "Prompt is too long", body: undefined })
  })
})
