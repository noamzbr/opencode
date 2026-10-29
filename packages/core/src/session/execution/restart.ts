export * as SessionRestart from "./restart.js"

import { Context, Effect, Layer } from "effect"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Bus } from "../../bus.js"
import { Job } from "../../job.js"
import { Session } from "../../session.js"
import { SessionEvent } from "../event.js"
import { SessionExecution } from "../execution.js"
import { SessionSchema } from "../schema.js"
import { SessionStore } from "../store.js"
import { BackgroundNotice } from "../background-notice.js"
import { SubagentOutcome } from "../subagent-outcome.js"

const CONTINUE_AFTER_SERVER_RESTART =
  "The server restarted while you were working. Continue from where you left off without repeating completed work."

const RESUME_EXHAUSTED = {
  type: "aborted",
  message: "Execution was interrupted repeatedly and will not be resumed automatically.",
} as const

export interface Options {
  /**
   * Times a single turn may be resumed before it is terminalized instead.
   * The counter is durable and only a terminal event resets it, so a turn
   * that keeps dying cannot crash-loop across restarts. Turns that complete
   * never accumulate: the budget is per-turn, not per-session.
   */
  readonly maxAttempts?: number
}

const DEFAULT_MAX_ATTEMPTS = 10

export interface Interface {
  /**
   * Resumes Sessions whose execution claim was never released — turns orphaned
   * by a process that died without teardown, or interrupted by a graceful
   * shutdown (which preserves the claim on purpose). The claim is never
   * cleared here: only a terminal event releases it, so a death anywhere in
   * the resume path leaves the same orphaned claim for the next boot.
   */
  readonly resumeSuspendedSessions: Effect.Effect<void>
}

/**
 * Recovery for orphaned executions. Claims are written at turn start by
 * SessionExecution, so this sweep needs no cooperation from the previous
 * process: crash, SIGKILL, isolate eviction, and graceful restart all leave
 * the same durable signature.
 *
 * Recovery is at-least-once: local coordination prevents concurrent drains,
 * not repeated external side effects after a crash.
 *
 * The sweep assumes every orphaned claim's owner is dead. The managed-server
 * protocol guarantees this: a successor is only spawned after the previous
 * process is confirmed dead (client service `kill`/`evict` poll the PID), the
 * registration lock admits one managed server at a time, and unregistered
 * servers sharing the database never sweep. The service is inert until called
 * — the managed server invokes it at boot; embedders may call it from their
 * own start-up.
 */
export class Service extends Context.Service<Service, Interface>()("@opencode/SessionRestart") {}

export const layer = (options?: Options) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const store = yield* SessionStore.Service
      const execution = yield* SessionExecution.Service
      const bus = yield* Bus.Service
      const jobs = yield* Job.Service
      const sessions = yield* Session.Service
      const scope = yield* Effect.scope
      const maxAttempts = options?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS

      const prepareResume = Effect.fnUntraced(function* (sessionID: SessionSchema.ID) {
        // Durable before the resume runs, so a crash inside the resumed turn is
        // counted by the next sweep and the budget cannot be dodged.
        const attempts = yield* store.countResume(sessionID)
        if (attempts === undefined) return false
        if (attempts > maxAttempts) {
          // Terminalize instead: the release hook clears the claim and resets the
          // counter atomically with the terminal event.
          yield* bus.publish(
            SessionEvent.Execution.Failed,
            { sessionID, error: RESUME_EXHAUSTED },
            { commit: () => store.release(sessionID) },
          )
          return false
        }
        yield* bus.publish(SessionEvent.Synthetic, {
          sessionID,
          text: CONTINUE_AFTER_SERVER_RESTART,
          description: "Continuing after restart",
          metadata: { notice: "restart" },
        })
        return true
      })

      const notify = Effect.fnUntraced(function* (
        background: Job.Background,
        terminal: Job.Terminal,
        suspended: ReadonlySet<SessionSchema.ID>,
        recovery: BackgroundNotice.Input["recovery"],
      ) {
        const recipient =
          recovery.kind === "shell" ? recovery.sessionID : recovery.parentSessionID
        yield* BackgroundNotice.deliver(sessions, jobs, {
          ...terminal,
          id: background.id,
          recovery,
          notificationID: background.notificationID,
          // Recovered shell notices never revive idle owners of long-lived shells. Interrupted
          // executions resume separately after their notices are admitted.
          resume: recovery.kind === "shell" || suspended.has(recipient) ? false : undefined,
        }).pipe(
          // A deleted recipient has nothing to be told; drop its marker with the notice.
          Effect.catchTag("Session.NotFoundError", () => jobs.completeBackground(background.notificationID)),
          Effect.orDie,
        )
      })

      const recoverImage = Effect.fnUntraced(function* (
        background: Job.Background,
        recovery: Extract<Job.Recovery, { kind: "image" }>,
      ) {
        const text =
          background.status === "running"
            ? `Image request for ${recovery.path} was cancelled because the server restarted.`
            : background.status === "completed"
              ? (background.result?.kind === "image"
                  ? background.result.text
                  : (background.output ?? `Image request finished. Saved image: ${recovery.path} (${recovery.model}).`))
              : background.status === "error"
                ? (background.error ?? `Image request failed for ${recovery.path}.`)
                : `Image request for ${recovery.path} was cancelled.`
        yield* sessions
          .synthetic({
            id: background.notificationID,
            sessionID: recovery.sessionID,
            description: "Image generation",
            text,
            metadata: {
              source: "image_generate",
              jobID: background.id,
              state: background.status === "running" ? "cancelled" : background.status,
              path: recovery.path,
            },
            resume: false,
          })
          .pipe(
            Effect.catchTag("Session.NotFoundError", () => Effect.void),
            Effect.orDie,
          )
        yield* jobs.completeBackground(background.notificationID)
      })

      const recoverSubagent = Effect.fnUntraced(function* (
        background: Job.Background,
        recovery: Extract<Job.Recovery, { kind: "subagent" }>,
        suspended: ReadonlySet<SessionSchema.ID>,
      ) {
        const child = yield* store.get(recovery.childSessionID)
        if (!child || child.parentID !== recovery.parentSessionID || !(yield* store.get(recovery.parentSessionID))) {
          yield* jobs.completeBackground(background.notificationID)
          return
        }
        if (background.status !== "running") {
          yield* notify(background, background, suspended, recovery)
          return
        }
        if (yield* execution.isActive(recovery.childSessionID)) return
        if (!(yield* prepareResume(recovery.childSessionID))) {
          yield* notify(background, { status: "error", error: RESUME_EXHAUSTED.message }, suspended, recovery)
          return
        }

        yield* jobs.start({
          id: background.id,
          type: "subagent",
          title: recovery.description,
          notificationID: background.notificationID,
          recovery,
          run: SubagentOutcome.run(sessions, recovery.childSessionID),
        })
        yield* jobs.background(background.id)
        yield* jobs.wait({ id: background.id }).pipe(
          Effect.flatMap((result) =>
            result.info && result.info.status !== "running" ? notify(background, result.info, suspended, recovery) : Effect.void,
          ),
          Effect.forkIn(scope),
        )
      })

      return Service.of({
        resumeSuspendedSessions: Effect.gen(function* () {
          const active = yield* execution.active
          const pending = yield* jobs.pendingBackground
          const children = pending.flatMap((background) =>
            background.status === "running" && background.recovery.kind === "subagent"
              ? [background.recovery.childSessionID]
              : [],
          )
          // Early notices wait for recovery's accounting, including Sessions that exhaust their budget.
          const suspended = new Set(
            [...(yield* store.listSuspended()), ...children].filter((sessionID) => !active.has(sessionID)),
          )
          yield* store.releaseChildClaims(children)
          yield* Effect.forEach(
            // Admit shell outcomes before a recovered child can start its first model request.
            pending.toSorted((a, b) => Number(a.recovery.kind === "subagent") - Number(b.recovery.kind === "subagent")),
            Effect.fnUntraced(function* (background) {
              if ((yield* jobs.get(background.id))?.status === "running") return
              const recovery = background.recovery
              if (recovery.kind === "image") return yield* recoverImage(background, recovery)
              // A shell cannot be rerun after its process died: deliver the marker's terminal as-is,
              // which the notice reports as cancelled when the command was still running.
              yield* recovery.kind === "shell"
                ? notify(background, background, suspended, recovery)
                : recoverSubagent(background, recovery, suspended)
            }),
            { discard: true },
          )

          // Background completion can wake a parent, so inspect local ownership only after recovery.
          const resumed = yield* execution.active
          yield* Effect.forEach(
            (yield* store.listSuspended()).filter((sessionID) => !resumed.has(sessionID)),
            (sessionID) =>
              execution
                .resume(sessionID)
                .pipe(Effect.ignore, Effect.forkIn(scope), Effect.when(prepareResume(sessionID))),
            { concurrency: "unbounded", discard: true },
          )
          // Async observers consult this set at delivery; later completions wake parents normally.
          suspended.clear()
        }),
      })
    }),
  )

export const node = makeGlobalNode({
  service: Service,
  layer: layer(),
  deps: [SessionStore.node, SessionExecution.node, Bus.node, Job.node, Session.node],
})
