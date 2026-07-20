import { describe, expect, test } from "bun:test"
import type { OpencodeClient } from "@opencode-ai/sdk/v2"
import { ACPSessionManager } from "../../src/acp/session"
import { ModelID, ProviderID } from "../../src/provider/schema"

function session(model?: { id: string; providerID: string; variant?: string }, agent?: string) {
  return {
    id: "ses_test",
    time: { created: 1_700_000_000_000, updated: 1_700_000_000_000 },
    model,
    agent,
  }
}

function manager(get: () => Promise<unknown>) {
  const sdk = {
    session: {
      get,
    },
  } as unknown as OpencodeClient
  return new ACPSessionManager(sdk)
}

describe("ACP session selection state", () => {
  test("cold load hydrates model, variant, and mode from the native session row", async () => {
    const sessions = manager(async () => ({
      data: session({ id: "model-a", providerID: "provider-a", variant: "high" }, "chat"),
    }))

    const state = await sessions.load("ses_test", "/workspace", [])

    expect(state.model && {
      providerID: String(state.model.providerID),
      modelID: String(state.model.modelID),
    }).toEqual({ providerID: "provider-a", modelID: "model-a" })
    expect(state.variant).toBe("high")
    expect(state.modeId).toBe("chat")
  })

  test("native default variant hydrates as the ACP undefined representation", async () => {
    const sessions = manager(async () => ({
      data: session({ id: "model-a", providerID: "provider-a", variant: "default" }, "agent"),
    }))

    expect((await sessions.load("ses_test", "/workspace", [])).variant).toBeUndefined()
  })

  test("warm load preserves a selection changed while native validation is pending", async () => {
    let resolve!: (value: unknown) => void
    let current = new Promise<unknown>((done) => {
      resolve = done
    })
    const sessions = manager(() => current)

    resolve({ data: session() })
    await sessions.load("ses_test", "/workspace", [])

    current = new Promise<unknown>((done) => {
      resolve = done
    })
    const load = sessions.load("ses_test", "/new-workspace", [])

    sessions.setModelSelection(
      "ses_test",
      {
        providerID: ProviderID.make("provider-new"),
        modelID: ModelID.make("model-new"),
      },
      "max",
    )
    sessions.setMode("ses_test", "chat")
    resolve({
      data: session({ id: "model-stale", providerID: "provider-stale", variant: "low" }, "agent"),
    })

    const state = await load
    expect(String(state.model?.modelID)).toBe("model-new")
    expect(state.variant).toBe("max")
    expect(state.modeId).toBe("chat")
    expect(state.cwd).toBe("/new-workspace")
  })
})
