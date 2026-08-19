import { Context, Effect, Layer, LayerMap, RcMap } from "effect"
import { LayerNode } from "./effect/layer-node"
import { Node } from "./effect/app-node"
import { FSUtil } from "./fs-util"
import { Location } from "./location"
import type { LocationError, LocationServices } from "./location-services"

export type Instance = LayerMap.LayerMap<Location.Ref, LocationServices, LocationError>

export class Service extends Context.Service<Service, Instance>()("@opencode/example/LocationServiceMap") {
  static get(ref: Location.Ref) {
    return Layer.unwrap(Effect.map(Service, (locations) => locations.get(ref)))
  }
}

// A process holds several separately built maps — one per route group, one for
// the ACP agent, one per AppNodeBuilder root — and a directory can be cached in
// any of them.
const live = new Set<(resolved: string) => Effect.Effect<void>>()

const invalidator = <I, E>(map: LayerMap.LayerMap<Location.Ref, I, E>) =>
  Effect.fnUntraced(function* (resolved: string) {
    for (const key of yield* RcMap.keys(map.rcMap)) {
      if (FSUtil.resolve(key.directory) === resolved) yield* map.invalidate(key)
    }
  })

/** Keeps `map` reachable from `invalidateDirectory` for the lifetime of its scope. */
export const track = <I, E>(map: LayerMap.LayerMap<Location.Ref, I, E>) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const invalidate = invalidator(map)
      live.add(invalidate)
      return invalidate
    }),
    (invalidate) => Effect.sync(() => live.delete(invalidate)),
  )

/**
 * Drops the cached location services for a directory. Entries are refcounted and
 * kept for `idleTimeToLive` after the last release, so without this an instance
 * disposal leaves a full per-directory service graph — file watcher and search
 * index included — resident for a directory nothing is using. An entry still in
 * use is only unpublished, and closes when its last holder releases it.
 */
export const invalidateDirectory = Effect.fn("LocationServiceMap.invalidateDirectory")(function* (directory: string) {
  const resolved = FSUtil.resolve(directory)
  for (const invalidate of live) yield* invalidate(resolved)
})

export const node = LayerNode.unbound(Service, Node.tags.values.global)

export * as LocationServiceMap from "./location-service-map"
