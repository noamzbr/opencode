export * as SubagentJob from "./subagent-job.js"

import { Context, Effect, Layer, Scope } from "effect"
import { Node } from "@opencode/util/effect/app-node"
import type { LayerNode } from "@opencode/util/effect/layer-node"
import { Job } from "../job.js"
import { Session } from "../session.js"
import { BackgroundNotice } from "./background-notice.js"
import { SubagentOutcome } from "./subagent-outcome.js"
import type { SessionSchema } from "./schema.js"

type Recovery = Extract<Job.Recovery, { kind: "subagent" }>

export interface Interface {
  start: (recovery: Recovery) => Effect.Effect<Job.Info>
  background: (childID: SessionSchema.ID) => Effect.Effect<void>
  notify: (job: Job.Info) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SubagentJob") {}

const make: Effect.Effect<Interface, never, Session.Service | Job.Service | Scope.Scope> = Effect.gen(function* () {
  const sessions = yield* Session.Service
  const jobs = yield* Job.Service
  const scope = yield* Scope.Scope
  // Jobs and their completion observers outlive the plugin that launched them.
  // Fork with the global context too, rather than retaining the caller's location.
  const context = yield* Effect.context<never>()
  // One observer per job generation, including continuations of the same child.
  const notifications = new Set<string>()

  const notify = Effect.fn("SubagentJob.notify")(function* (job: Job.Info) {
    const key = `${job.id}:${job.started_at}`
    if (notifications.has(key)) return
    notifications.add(key)
    yield* Effect.gen(function* () {
      const info = (yield* jobs.wait({ id: job.id })).info
      if (info?.recovery && info.status !== "running")
        yield* BackgroundNotice.deliver(sessions, jobs, { ...info, recovery: info.recovery })
    }).pipe(
      Effect.ensuring(Effect.sync(() => notifications.delete(key))),
      Effect.forkIn(scope, { startImmediately: true }),
      Effect.setContext(context),
    )
  })

  return Service.of({
    start: (recovery: Recovery) =>
      jobs
        .start({
          id: recovery.childSessionID,
          type: "subagent",
          title: recovery.description,
          metadata: {},
          recovery,
          run: SubagentOutcome.run(sessions, recovery.childSessionID),
        })
        .pipe(Effect.setContext(context)),
    background: Effect.fn("SubagentJob.background")(function* (childID: SessionSchema.ID) {
      const info = yield* jobs.background(childID)
      if (info) yield* notify(info)
    }),
    notify,
  })
})

export const node: LayerNode.Provider<Service, never, typeof Node.tags.values.global> = Node.makeGlobalNode({
  service: Service,
  layer: Layer.effect(Service, make),
  deps: [Session.node, Job.node],
})
