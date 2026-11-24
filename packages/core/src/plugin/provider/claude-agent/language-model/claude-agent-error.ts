export * as ClaudeAgentError from "./claude-agent-error.js"

import type { SDKAssistantMessageError, SDKMessage, SDKRateLimitInfo } from "@anthropic-ai/claude-agent-sdk"
import {
  AIError,
  AuthenticationError,
  HttpContext,
  ProviderInternalError,
  RateLimitError,
  UnknownProviderError,
} from "@opencode/ai/effect"

/**
 * Why a Claude request failed, for a host to act on. The session error carries it in `response.body` as
 * `{ "claude_agent": Body }`.
 */
export type Body =
  | { readonly code: "not_installed" }
  | { readonly code: "too_old"; readonly version?: string; readonly minimum: string }
  | { readonly code: "not_logged_in" }
  | { readonly code: "rate_limited"; readonly window?: string; readonly resetsAt?: string }
  | { readonly code: "upstream_error"; readonly status: number }

export class Failure extends Error {
  constructor(
    message: string,
    readonly limited: boolean,
    readonly body?: Body,
  ) {
    super(message)
  }
}

/**
 * The provider error a session records for a failure that names its cause: a login refusal, a plan limit with
 * its reset, an Anthropic server error, or a `claude` that is missing or too old. Any other failure is returned
 * as it is, so the provider classification still recognises it, such as a prompt over the context window.
 */
export function toAIError(error: unknown) {
  if (!(error instanceof Failure) || error.body === undefined) return error
  const details = { message: error.message, body: JSON.stringify({ claude_agent: error.body }), cause: error }
  const body = error.body
  if (body.code === "rate_limited") {
    const retryAfterMs = body.resetsAt === undefined ? undefined : Math.max(0, Date.parse(body.resetsAt) - Date.now())
    return new AIError({
      reason: new RateLimitError({
        ...details,
        http: http(429),
        ...(retryAfterMs === undefined || Number.isNaN(retryAfterMs) ? {} : { retryAfterMs: Math.round(retryAfterMs) }),
      }),
    })
  }
  if (body.code === "upstream_error")
    return new AIError({ reason: new ProviderInternalError({ ...details, http: http(body.status) }) })
  if (body.code === "not_logged_in")
    return new AIError({ reason: new AuthenticationError({ ...details, http: http(401) }) })
  return new AIError({ reason: new UnknownProviderError(details) })
}

// Claude Code makes the request, so the failure names no URL of its own. It owns every retry before this result,
// so the status never asks for another one.
const http = (status: number) =>
  new HttpContext({ url: "claude-agent:", status, headers: { "x-should-retry": "false" } })

// Keep request-local diagnostics until the terminal result. A usage warning alone
// is not a failed request, and Claude Code owns any retries before this result.
export function make() {
  let code: SDKAssistantMessageError | undefined
  let rejected: SDKRateLimitInfo | undefined
  return (message: SDKMessage): Failure | undefined => {
    if (message.type === "rate_limit_event") {
      rejected = message.rate_limit_info.status === "rejected" ? message.rate_limit_info : undefined
    }
    if (message.type === "assistant" && message.parent_tool_use_id === null && message.error) code = message.error
    if (message.type !== "result") return

    const status = message.subtype === "success" ? (message.api_error_status ?? undefined) : undefined
    const httpError = status !== undefined && status >= 400
    if (message.subtype === "success" && !message.is_error && !httpError) return
    const result = (message.subtype === "success" ? message.result : message.errors.join("\n")).replaceAll(
      "Please run /login",
      "Run `claude auth login` in a terminal",
    )
    const failure =
      status !== undefined && status >= 500
        ? (status === 529 ? "Anthropic is overloaded (529)." : `Anthropic returned HTTP ${status}.`) +
          " Claude Code exhausted its retries. Try again shortly."
        : result
    const limited = code === "rate_limit" || (rejected !== undefined && status === 429)
    const body: Body | undefined = limited
      ? {
          code: "rate_limited",
          ...(rejected?.rateLimitType ? { window: rejected.rateLimitType } : {}),
          ...(rejected?.resetsAt ? { resetsAt: new Date(rejected.resetsAt * 1000).toISOString() } : {}),
        }
      : status !== undefined && status >= 500
        ? { code: "upstream_error", status }
        : code === "authentication_failed" || status === 401 || status === 403
          ? { code: "not_logged_in" }
          : undefined
    const window = rejected?.rateLimitType?.replaceAll("_", " ")
    const limit =
      limited && !failure ? `Claude Code subscription limit reached${window ? ` (${window})` : ""}.` : failure
    const reset = limited && rejected?.resetsAt ? ` Resets at ${new Date(rejected.resetsAt * 1000).toISOString()}.` : ""
    const text = limit + reset

    if (text) return new Failure(text, limited, body)
    if (status) return new Failure(`Claude Code could not finish its reply (HTTP ${status})`, limited, body)
    return new Failure("Claude Code could not finish its reply", limited, body)
  }
}
