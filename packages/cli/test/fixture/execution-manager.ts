import fs from "node:fs/promises"
import net from "node:net"
import os from "node:os"
import path from "node:path"
import { ScriptitDispatcher } from "../../src/commands/handlers/scriptit-dispatcher"

interface Frame {
  readonly type: number
  readonly payload: Buffer
}

export interface Peer<Request = Record<string, unknown>> {
  readonly request: Request
  /** The client's next frame after its request; undefined once the client closed. */
  readonly next: () => Promise<Frame | undefined>
  readonly send: (type: number, payload?: string | Uint8Array) => void
  readonly reply: (value: object) => void
  readonly end: () => void
}

/** The server side of the execution manager protocol, answering each connection with `serve`. */
export async function fakeManager<Request = Record<string, unknown>>(serve: (peer: Peer<Request>) => unknown) {
  const tmp = await tmpdir()
  const socket = path.join(tmp.path, "manager.sock")
  const requests: Request[] = []
  const server = net.createServer((client) => {
    const frames: Frame[] = []
    const waiters: Array<(frame: Frame | undefined) => void> = []
    let closed = false
    let pending = Buffer.alloc(0)
    let peer: Peer<Request> | undefined
    const send = (type: number, payload: string | Uint8Array = new Uint8Array()) => {
      const bytes = Buffer.from(payload)
      const header = Buffer.alloc(5)
      header.writeUInt32BE(bytes.length + 1, 0)
      header[4] = type
      client.write(Buffer.concat([header, bytes]))
    }
    const deliver = (frame: Frame) => {
      if (peer) return waiters.length > 0 ? waiters.shift()!(frame) : frames.push(frame)
      peer = {
        request: JSON.parse(frame.payload.toString()),
        next: () =>
          frames.length > 0 || closed
            ? Promise.resolve(frames.shift())
            : new Promise((resolve) => waiters.push(resolve)),
        send,
        reply: (value) => send(ScriptitDispatcher.Frame.json, JSON.stringify(value)),
        end: () => client.end(),
      }
      requests.push(peer.request)
      void serve(peer)
    }
    client.on("data", (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk])
      while (pending.length >= 4 && pending.length >= 4 + pending.readUInt32BE(0)) {
        const length = pending.readUInt32BE(0)
        deliver({ type: pending[4], payload: pending.subarray(5, 4 + length) })
        pending = pending.subarray(4 + length)
      }
    })
    client.on("close", () => {
      closed = true
      waiters.splice(0).forEach((waiter) => waiter(undefined))
    })
    client.on("error", () => {})
  })
  await new Promise<void>((resolve) => server.listen(socket, resolve))
  return {
    socket,
    requests,
    async [Symbol.asyncDispose]() {
      server.close()
      await tmp[Symbol.asyncDispose]()
    },
  }
}

export async function tmpdir() {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "scriptit-exec-"))
  return {
    path: await fs.realpath(directory),
    async [Symbol.asyncDispose]() {
      await fs.rm(directory, { recursive: true, force: true })
    },
  }
}
