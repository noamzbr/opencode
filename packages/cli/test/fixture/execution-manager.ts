import fs from "node:fs/promises"
import net from "node:net"
import os from "node:os"
import path from "node:path"

/** The protocol's frame types, apart from the client's own table so that a wrong number in either fails the tests. */
export const Frame = { json: 1, stdout: 2, stderr: 3, stdin: 4, stdinEnd: 5, body: 6 } as const

interface Received {
  readonly type: number
  readonly payload: Buffer
}

export interface Peer<Request = Record<string, unknown>> {
  readonly request: Request
  /** The client's next frame after its request; undefined once the client closed. */
  readonly next: () => Promise<Received | undefined>
  /** Resolves once the socket can take more, so a sender that awaits it honors the client's backpressure. */
  readonly send: (type: number, payload?: string | Uint8Array) => Promise<void>
  readonly reply: (value: object) => void
  /** Stops and restarts reading the client's frames, as a manager does when a job's stdin is full. */
  readonly pause: () => void
  readonly resume: () => void
  readonly end: () => void
}

/** The server side of the execution manager protocol, answering each connection with `serve`. */
export async function fakeManager<Request = Record<string, unknown>>(serve: (peer: Peer<Request>) => unknown) {
  const tmp = await tmpdir()
  const socket = path.join(tmp.path, "manager.sock")
  const requests: Request[] = []
  // The encoded size of each request frame's payload.
  const sizes: number[] = []
  const server = net.createServer((client) => {
    const frames: Received[] = []
    const waiters: Array<(frame: Received | undefined) => void> = []
    let closed = false
    let pending = Buffer.alloc(0)
    let peer: Peer<Request> | undefined
    const send = (type: number, payload: string | Uint8Array = new Uint8Array()) => {
      const bytes = Buffer.from(payload)
      const header = Buffer.alloc(5)
      header.writeUInt32BE(bytes.length + 1, 0)
      header[4] = type
      if (client.write(Buffer.concat([header, bytes]))) return Promise.resolve()
      return new Promise<void>((resolve) => {
        const done = () => {
          client.off("drain", done)
          client.off("close", done)
          resolve()
        }
        client.on("drain", done)
        client.on("close", done)
      })
    }
    const deliver = (frame: Received) => {
      if (peer) return waiters.length > 0 ? waiters.shift()!(frame) : frames.push(frame)
      peer = {
        request: JSON.parse(frame.payload.toString()),
        next: () =>
          frames.length > 0 || closed
            ? Promise.resolve(frames.shift())
            : new Promise((resolve) => waiters.push(resolve)),
        send,
        reply: (value) => void send(Frame.json, JSON.stringify(value)),
        pause: () => client.pause(),
        resume: () => client.resume(),
        end: () => client.end(),
      }
      requests.push(peer.request)
      sizes.push(frame.payload.length)
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
    sizes,
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
