export * as BlockTree from "./block-tree.js"

import { Schema } from "effect"

/**
 * Script.it agent block metadata. Native children inherit it, so it marks every Session of a block's tree. Only a
 * request of the Script.it bridge starts work in a block tree, so nothing in it wakes on its own.
 */
export const is = Schema.is(Schema.Struct({ scriptit: Schema.Struct({ kind: Schema.Literal("block") }) }))
