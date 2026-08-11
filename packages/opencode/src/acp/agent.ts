import {
  RequestError,
  type Agent as ACPAgent,
  type AgentSideConnection,
  type AuthenticateRequest,
  type CancelNotification,
  type CloseSessionRequest,
  type ForkSessionRequest,
  type InitializeRequest,
  type ListSessionsRequest,
  type LoadSessionRequest,
  type NewSessionRequest,
  type PromptRequest,
  type ResumeSessionRequest,
  type SetSessionConfigOptionRequest,
  type SetSessionModelRequest,
  type SetSessionModeRequest,
} from "@agentclientprotocol/sdk"
import { zPromptRequest } from "@agentclientprotocol/sdk/dist/schema/zod.gen.js"
import { Effect } from "effect"
import type { OpencodeClient } from "@opencode-ai/sdk/v2"
import { z } from "zod"
import * as ACPError from "./error"
import * as ACPService from "./service"

const AsyncPromptParams = zPromptRequest.pick({ sessionId: true, prompt: true }).extend({
  messageId: z.string().min(1),
})
const ShellParams = z.object({
  sessionId: z.string().min(1),
  messageId: z.string().min(1),
  command: z.string().min(1),
  agent: z.string().optional(),
  model: z.object({ providerID: z.string().min(1), modelID: z.string().min(1) }).optional(),
  fireAndForget: z.boolean().optional().default(true),
})

export function init({ sdk: _sdk }: { sdk: OpencodeClient }) {
  return {
    create: (connection: AgentSideConnection) => {
      return new Agent(ACPService.make({ sdk: _sdk, connection }))
    },
  }
}

export class Agent implements ACPAgent {
  constructor(private readonly service: ACPService.Interface) {}

  initialize(params: InitializeRequest) {
    return run(this.service.initialize(params))
  }

  authenticate(params: AuthenticateRequest) {
    return run(this.service.authenticate(params))
  }

  newSession(params: NewSessionRequest) {
    return run(this.service.newSession(params))
  }

  loadSession(params: LoadSessionRequest) {
    return run(this.service.loadSession(params))
  }

  listSessions(params: ListSessionsRequest) {
    return run(this.service.listSessions(params))
  }

  resumeSession(params: ResumeSessionRequest) {
    return run(this.service.resumeSession(params))
  }

  closeSession(params: CloseSessionRequest) {
    return run(this.service.closeSession(params))
  }

  unstable_forkSession(params: ForkSessionRequest) {
    return run(this.service.forkSession(params))
  }

  setSessionConfigOption(params: SetSessionConfigOptionRequest) {
    return run(this.service.setSessionConfigOption(params))
  }

  setSessionMode(params: SetSessionModeRequest) {
    return run(this.service.setSessionMode(params))
  }

  unstable_setSessionModel(params: SetSessionModelRequest) {
    return run(this.service.setSessionModel(params))
  }

  prompt(params: PromptRequest) {
    return run(this.service.prompt(params))
  }

  extMethod(method: string, params: Record<string, unknown>) {
    if (method === "session/asyncPrompt") {
      const parsed = AsyncPromptParams.safeParse(params)
      if (!parsed.success) throw RequestError.invalidParams(parsed.error.flatten(), "invalid async prompt parameters")
      return run(this.service.asyncPrompt(parsed.data))
    }
    if (method === "session/shell") {
      const parsed = ShellParams.safeParse(params)
      if (!parsed.success) throw RequestError.invalidParams(parsed.error.flatten(), "invalid shell parameters")
      return run(this.service.shell(parsed.data))
    }
    throw RequestError.methodNotFound(method)
  }

  cancel(params: CancelNotification) {
    return run(this.service.cancel(params))
  }
}

function run<A>(effect: Effect.Effect<A, ACPService.Error>) {
  return Effect.runPromise(effect.pipe(Effect.mapError(ACPError.toRequestError))).catch((defect: unknown) => {
    if (defect instanceof RequestError) throw defect
    throw ACPError.toRequestError(ACPError.fromUnknownDefect(defect))
  })
}

export * as ACP from "./agent"
