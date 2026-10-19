import { socketFaultInjection as fault } from "bun:internal-for-testing";
import { afterEach, describe, expect, test } from "bun:test";
import { bunEnv, bunExe, tls as certs, isWindows } from "harness";
import { AsyncLocalStorage } from "node:async_hooks";
import { spawn } from "node:child_process";
import { once } from "node:events";
import http from "node:http";
import net from "node:net";
import tls from "node:tls";

// Windows uses the libuv eventing backend; bsd_recv/bsd_send are still the
// chokepoints there but errno semantics differ. Land POSIX coverage first.
const skip = !fault.available() || isWindows;

afterEach(() => fault.clear());

async function connectedPair(onServerSocket?: (s: net.Socket) => void) {
  const server = net.createServer();
  server.on("connection", s => onServerSocket?.(s));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = (server.address() as net.AddressInfo).port;

  // Register the server-side listener before initiating connect so the
  // 'connection' event cannot be missed.
  const serverSockP = once(server, "connection") as Promise<[net.Socket]>;
  const client = net.connect({ port, host: "127.0.0.1" });
  const [, [serverSock]] = await Promise.all([once(client, "connect"), serverSockP]);

  return {
    server,
    client,
    serverSock,
    [Symbol.dispose]() {
      client.destroy();
      serverSock.destroy();
      server.close();
    },
  };
}

describe.skipIf(skip)("node:net under injected syscall faults", () => {
  test("recv → ECONNRESET surfaces as 'error' and destroys the socket", async () => {
    using p = await connectedPair();
    fault.set({ syscall: "recv", action: "errno", errno: "ECONNRESET", repeat: 1 });
    // Trigger a recv() on the client by writing from the server.
    const errP = once(p.client, "error");
    p.serverSock.write("hello");
    const [err] = (await errP) as [NodeJS.ErrnoException];
    expect(err.code).toBe("ECONNRESET");
    expect(p.client.destroyed).toBe(true);
  });

  test("recv → ETIMEDOUT surfaces as 'error' with code ETIMEDOUT", async () => {
    using p = await connectedPair();
    p.serverSock.on("error", () => {});
    fault.set({ syscall: "recv", action: "errno", errno: "ETIMEDOUT", repeat: 1 });
    const errP = once(p.client, "error");
    p.serverSock.write("hello");
    const [err] = (await errP) as [NodeJS.ErrnoException];
    expect(err.code).toBe("ETIMEDOUT");
    expect(p.client.destroyed).toBe(true);
  });

  test("recv → 0 (peer closed) emits 'end' without 'error'", async () => {
    using p = await connectedPair();
    fault.set({ syscall: "recv", action: "zero", repeat: 1 });
    let gotError: unknown = null;
    p.client.on("error", e => (gotError = e));
    const endP = once(p.client, "end");
    p.serverSock.write("hello");
    await endP;
    expect(gotError).toBeNull();
  });

  test("recv → short reads still deliver complete payload", async () => {
    using p = await connectedPair();
    // Clamp every recv to 1 byte: the loop should keep draining until EAGAIN
    // and the application must still observe the full payload.
    fault.set({ syscall: "recv", action: "short", bytes: 1, repeat: -1 });
    const chunks: Buffer[] = [];
    p.client.on("data", (c: Buffer) => chunks.push(c));
    const payload = Buffer.from(Array.from({ length: 256 }, (_, i) => i & 0xff));
    p.serverSock.write(payload);
    p.serverSock.end();
    await once(p.client, "end");
    expect(Buffer.concat(chunks).equals(payload)).toBe(true);
  });

  test("send → EAGAIN forever: data is buffered, then flushed after disarm", async () => {
    let received = Buffer.alloc(0);
    using p = await connectedPair(s => {
      s.on("data", (c: Buffer) => (received = Buffer.concat([received, c])));
    });
    fault.set({ syscall: "send", action: "errno", errno: "EAGAIN", repeat: -1 });
    p.client.write(Buffer.alloc(256, "x"));
    fault.clear();
    p.client.end();
    await once(p.serverSock, "end");
    expect(received.length).toBe(256);
  });

  test("send → short writes still deliver complete payload to peer", async () => {
    let received = Buffer.alloc(0);
    using p = await connectedPair(s => {
      s.on("data", (c: Buffer) => (received = Buffer.concat([received, c])));
    });
    fault.set({ syscall: "send", action: "short", bytes: 1, repeat: -1 });
    const payload = Buffer.alloc(512, "b");
    p.client.write(payload);
    p.client.end();
    await once(p.serverSock, "end");
    fault.clear();
    expect(received.length).toBe(payload.length);
    expect(received.equals(payload)).toBe(true);
  });

  // A short send leaves the remainder in the native buffered_data_for_node_net;
  // the next writable event drives internal_flush(), and if that retry's send()
  // fails with a transient errno (ENOBUFS/ENOMEM on a healthy connection) the
  // bytes must stay buffered and be retried, not dropped. Dropping them fired
  // 'drain' on a socket that had silently lost a span of the application's
  // byte stream. SocketBody is shared by connecting and accepted sockets, so
  // both the client writer and the net.createServer response writer are
  // exercised.
  for (const side of ["client", "server"] as const) {
    test.each(["ENOBUFS", "ENOMEM"] as const)(
      `send → %s on the drain-path flush of buffered data does not drop bytes (${side} writer)`,
      async errno => {
        using p = await connectedPair();
        const writer = side === "client" ? p.client : p.serverSock;
        const reader = side === "client" ? p.serverSock : p.client;
        let received = Buffer.alloc(0);
        reader.on("data", (c: Buffer) => (received = Buffer.concat([received, c])));

        const writerFd = (writer as any)._handle.fd as number;
        expect(writerFd).toBeGreaterThanOrEqual(0);

        const head = Buffer.from(Array.from({ length: 200 }, (_, i) => i & 0xff));
        const body = Buffer.alloc(4096, 0xee);

        // send #0: clamp to 1 byte so the other 199 bytes of `head` land in
        // buffered_data_for_node_net and the writable poll is armed.
        fault.set({ syscall: "send", action: "short", bytes: 1, repeat: 1, fd: writerFd });
        const headWritten = new Promise<void>((resolve, reject) =>
          writer.write(head, err => (err ? reject(err) : resolve())),
        );

        // send #1: the drain-path flush of the buffered remainder. Inject a
        // one-shot transient errno; once it fires the rule self-disarms and the
        // next retry must deliver the bytes.
        fault.set({ syscall: "send", action: "errno", errno, repeat: 1, fd: writerFd });

        // A follow-up write on the same connection after the first write's
        // callback fires lets the peer observe a mid-stream gap (head[0]
        // followed by body) if the buffered remainder was dropped.
        await headWritten;
        writer.write(body);
        writer.end();
        await once(reader, "end");
        fault.clear();

        const expected = Buffer.concat([head, body]);
        expect({ length: received.length, intact: received.equals(expected) }).toEqual({
          length: expected.length,
          intact: true,
        });
      },
    );
  }

  // us_socket_write_check_error gives send() errnos that are neither
  // would-block/transient nor known peer-gone (EPROTOTYPE: macOS returns it
  // racily from send() on healthy sockets) a bounded retry through the
  // writable rearm machinery instead of failing the first write.
  test("send → EPROTOTYPE burst: bytes are retried and delivered intact", async () => {
    let received = Buffer.alloc(0);
    using p = await connectedPair(s => {
      s.on("data", (c: Buffer) => (received = Buffer.concat([received, c])));
    });
    const fd = (p.client as any)._handle.fd as number;
    expect(fd).toBeGreaterThanOrEqual(0);
    fault.set({ syscall: "send", action: "errno", errno: "EPROTOTYPE" as any, repeat: 8, fd });
    const payload = Buffer.alloc(256, "p");
    p.client.write(payload);
    p.client.end();
    await once(p.serverSock, "end");
    fault.clear();
    expect(received.equals(payload)).toBe(true);
  });

  // A sustained unclassified errno exhausts the bounded retry and must fail
  // like a dead transport: 'error' carrying the real errno on the write
  // syscall, then close. Before the fix the buffered bytes were silently
  // dropped, the write was acknowledged as flushed and the peer saw a clean
  // FIN terminating a truncated stream.
  test("send → sustained EPROTOTYPE surfaces as 'error' + close, not silent truncation", async () => {
    using p = await connectedPair();
    const fd = (p.client as any)._handle.fd as number;
    expect(fd).toBeGreaterThanOrEqual(0);
    fault.set({ syscall: "send", action: "errno", errno: "EPROTOTYPE" as any, repeat: -1, fd });
    const errP = once(p.client, "error") as Promise<[NodeJS.ErrnoException]>;
    // Not events.once(): that helper rejects its promise when 'error' fires
    // first, and 'error' arriving before 'close' is exactly this contract.
    const closeP = new Promise<void>(resolve => p.client.once("close", () => resolve()));
    p.client.write(Buffer.alloc(64, "x"));
    const [err] = await errP;
    fault.clear();
    expect({ code: err.code, syscall: err.syscall }).toEqual({ code: "EPROTOTYPE", syscall: "write" });
    await closeP;
    expect(p.client.destroyed).toBe(true);
  });

  // Server-side twin of the above. On kqueue the fatal flush from on_writable
  // runs before the read dispatch (EV_EOF sets eof, not error), and its close
  // short-circuits the read-error path, so ServerHandlers.error is the only
  // place the errno is visible. With the plain-TCP delegation swallowing it,
  // the server socket's 'error' never fired and 'close' was stuck behind an
  // un-failed pending write (test-net-stream.js timeout on darwin).
  test("server: send → sustained fatal flush surfaces 'error' + 'close' on the accepted socket", async () => {
    using p = await connectedPair();
    const fd = (p.serverSock as any)._handle.fd as number;
    expect(fd).toBeGreaterThanOrEqual(0);
    fault.set({ syscall: "send", action: "errno", errno: "EPROTOTYPE" as any, repeat: -1, fd });
    const errP = once(p.serverSock, "error") as Promise<[NodeJS.ErrnoException]>;
    const closeP = new Promise<void>(resolve => p.serverSock.once("close", () => resolve()));
    p.serverSock.write(Buffer.alloc(64, "s"));
    const [err] = await errP;
    fault.clear();
    expect({ code: err.code, syscall: err.syscall }).toEqual({ code: "EPROTOTYPE", syscall: "write" });
    await closeP;
    expect(p.serverSock.destroyed).toBe(true);
  });

  // Node hands a send the kernel rejects at once to the stream inside write()
  // itself, so write() returns false and the socket is errored in the same
  // call. The stream runs the write callbacks and destroys it on the next tick.
  // https://github.com/nodejs/node/blob/v26.3.0/lib/internal/stream_base_commons.js#L158-L159
  for (const side of ["client", "server"] as const) {
    test.each(["ECONNRESET", "EPIPE"] as const)(
      `send → %s on the first attempt: write() returns false and the socket is errored in the same call (${side} writer)`,
      async errno => {
        using p = await connectedPair();
        const writer = side === "client" ? p.client : p.serverSock;
        const fd = (writer as any)._handle.fd as number;
        expect(fd).toBeGreaterThanOrEqual(0);
        fault.set({ syscall: "send", action: "errno", errno, repeat: 1, fd });

        const events: string[] = [];
        writer.on("error", (e: NodeJS.ErrnoException) => events.push(`error ${e.code} ${e.syscall}`));
        const closed = new Promise<void>(resolve => {
          writer.on("close", hadError => {
            events.push(`close ${hadError}`);
            resolve();
          });
        });
        const first = writer.write("x", e => events.push(`write#1 ${(e as NodeJS.ErrnoException)?.code}`));
        const afterFirst = {
          destroyed: writer.destroyed,
          errored: (writer.errored as NodeJS.ErrnoException | null)?.code,
          writable: writer.writable,
          writableLength: writer.writableLength,
        };
        // The stream is errored, so this write never reaches send().
        const second = writer.write("y", e => events.push(`write#2 ${(e as NodeJS.ErrnoException)?.code}`));
        await closed;

        expect({ first, afterFirst, second, events }).toEqual({
          first: false,
          afterFirst: { destroyed: false, errored: errno, writable: false, writableLength: 0 },
          second: false,
          events: [`write#1 ${errno}`, `write#2 ${errno}`, `error ${errno} write`, "close true"],
        });
      },
    );
  }

  test("connect → ECONNREFUSED is reported on connecting socket", async () => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const port = (server.address() as net.AddressInfo).port;
    try {
      fault.set({ syscall: "connect", action: "errno", errno: "ECONNREFUSED", repeat: 1 });
      const client = net.connect({ port, host: "127.0.0.1" });
      const [err] = (await once(client, "error")) as [NodeJS.ErrnoException];
      expect(err.code).toBe("ECONNREFUSED");
      expect(client.destroyed).toBe(true);
    } finally {
      server.close();
    }
  });

  test("fd targeting: rule on the server fd does not affect the client", async () => {
    using p = await connectedPair();
    // The server socket's recv should error; the client should still receive
    // data normally because the rule only matches the server fd.
    const serverFd = (p.serverSock as any)._handle.fd;
    expect(serverFd).toBeGreaterThanOrEqual(0);
    const serverErrP = once(p.serverSock, "error") as Promise<[NodeJS.ErrnoException]>;
    fault.set({
      syscall: "recv",
      action: "errno",
      errno: "ECONNRESET",
      repeat: -1,
      fd: serverFd,
    });
    const dataP = once(p.client, "data");
    p.serverSock.write("from-server");
    p.client.write("from-client");
    const [chunk] = (await dataP) as [Buffer];
    expect(chunk.toString()).toBe("from-server");
    const [serverErr] = await serverErrP;
    expect(serverErr.code).toBe("ECONNRESET");
  });

  test("accept → EMFILE-style failure: server keeps listening, client sees connect error", async () => {
    const server = net.createServer();
    server.on("error", () => {});
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const port = (server.address() as net.AddressInfo).port;
    try {
      // Block one accept (repeat:1 self-disarms). The first client's TCP
      // connect succeeds (kernel backlog) but the server-side accept fails;
      // await c1's 'connect' so the accept has definitively been attempted
      // before connecting c2.
      fault.set({ syscall: "accept", action: "errno", errno: "ENOMEM", repeat: 1 });
      const c1 = net.connect({ port, host: "127.0.0.1" });
      c1.on("error", () => {});
      await once(c1, "connect");
      c1.destroy();

      const okP = once(server, "connection") as Promise<[net.Socket]>;
      const c2 = net.connect({ port, host: "127.0.0.1" });
      const [[serverSock]] = await Promise.all([okP, once(c2, "connect")]);
      expect(c2.readyState).toBe("open");
      serverSock.destroy();
      c2.destroy();
    } finally {
      server.close();
    }
  });

  test("two rules can be armed simultaneously (recv short + send short)", async () => {
    let received = Buffer.alloc(0);
    using p = await connectedPair(s => {
      s.on("data", (c: Buffer) => (received = Buffer.concat([received, c])));
    });
    fault.set({ syscall: "recv", action: "short", bytes: 3, repeat: -1 });
    fault.set({ syscall: "send", action: "short", bytes: 3, repeat: -1 });
    const payload = Buffer.alloc(300, "k");
    p.client.write(payload);
    p.client.end();
    await once(p.serverSock, "end");
    expect(received.equals(payload)).toBe(true);
  });

  test("send → short writes deliver a payload larger than the kernel send buffer", async () => {
    let received = Buffer.alloc(0);
    using p = await connectedPair(s => {
      s.on("data", (c: Buffer) => (received = Buffer.concat([received, c])));
    });
    fault.set({ syscall: "send", action: "short", bytes: 1024, repeat: -1 });
    const payload = Buffer.alloc(128 * 1024, 0x41);
    p.client.write(payload);
    p.client.end();
    await once(p.serverSock, "end");
    expect(received.length).toBe(payload.length);
    expect(received.equals(payload)).toBe(true);
  });

  test("after: N skips the first N matching calls", async () => {
    using p = await connectedPair();
    // Skip the first recv (the readable notification arms one), fail the second.
    fault.set({ syscall: "recv", action: "errno", errno: "ECONNRESET", after: 1, repeat: 1 });
    let firstChunk: Buffer | null = null;
    p.client.once("data", (c: Buffer) => (firstChunk = c));
    const errP = once(p.client, "error");
    p.serverSock.write("first");
    await new Promise<void>(r => p.client.once("data", () => r()));
    p.serverSock.write("second");
    const [err] = (await errP) as [NodeJS.ErrnoException];
    expect(firstChunk!.toString()).toBe("first");
    expect(err.code).toBe("ECONNRESET");
  });
});

// A write the kernel takes only part of waits for the socket's writable event. Node keeps the async context with the
// write request, so whatever completes the write (the drain, a flush that fails, the close), its callback and what
// follows from it run where the write was issued. Each socket here is opened in a context of its own: a callback that
// runs where its socket was opened sees "opened".
describe.skipIf(skip)("a write that waited for the socket completes in the async context of the write", () => {
  const storage = new AsyncLocalStorage<string>();
  const fdOf = (socket: net.Socket) => (socket as any)._handle.fd as number;
  // The next send on this socket takes one byte: the rest of the write waits.
  const takeOneByte = (socket: net.Socket) =>
    fault.set({ syscall: "send", action: "short", bytes: 1, repeat: 1, fd: fdOf(socket) });

  // The lifecycle of `socket` after a write that waited fails, with the store each event ran in.
  function failedWrite(socket: net.Socket, fail: () => void) {
    const events: string[] = [];
    socket.on("error", (e: NodeJS.ErrnoException) => events.push(`error ${e.code} in ${storage.getStore()}`));
    const { promise, resolve } = Promise.withResolvers<string[]>();
    socket.on("close", () => resolve(events));
    takeOneByte(socket);
    storage.run("write", () =>
      socket.write(Buffer.alloc(200, "w"), err =>
        events.push(`write ${(err as NodeJS.ErrnoException)?.code} in ${storage.getStore()}`),
      ),
    );
    fail();
    return promise;
  }

  describe.each(["client", "server"] as const)("%s", side => {
    async function pair() {
      const p = await storage.run("opened", connectedPair);
      const [writer, reader] = side === "client" ? [p.client, p.serverSock] : [p.serverSock, p.client];
      return { p, writer, reader };
    }

    test("the write callback, after the drain", async () => {
      const { p, writer } = await pair();
      using _ = p;
      takeOneByte(writer);
      const { promise, resolve } = Promise.withResolvers<{ error: Error | null; store: string | undefined }>();
      storage.run("write", () =>
        writer.write(Buffer.alloc(200, "w"), err => resolve({ error: err ?? null, store: storage.getStore() })),
      );
      expect(await promise).toEqual({ error: null, store: "write" });
    });

    test("'drain', after a write over the high water mark", async () => {
      const { p, writer, reader } = await pair();
      using _ = p;
      reader.resume();
      takeOneByte(writer);
      const { promise, resolve } = Promise.withResolvers<string | undefined>();
      writer.once("drain", () => resolve(storage.getStore()));
      const flushed = storage.run("write", () => writer.write(Buffer.alloc(2 * writer.writableHighWaterMark, "w")));
      expect({ flushed, drain: await promise }).toEqual({ flushed: false, drain: "write" });
    });

    test("the end(chunk) callback and 'finish'", async () => {
      const { p, writer } = await pair();
      using _ = p;
      takeOneByte(writer);
      const stores: Record<string, string | undefined> = {};
      const { promise, resolve } = Promise.withResolvers<void>();
      writer.once("finish", () => (stores.finish = storage.getStore()));
      storage.run("end", () =>
        writer.end(Buffer.alloc(200, "w"), () => {
          stores.callback = storage.getStore();
          resolve();
        }),
      );
      await promise;
      expect(stores).toEqual({ finish: "end", callback: "end" });
    });

    test("the write callback and 'error', when the flush from the writable event fails", async () => {
      const { p, writer } = await pair();
      using _ = p;
      const events = await failedWrite(writer, () =>
        fault.set({ syscall: "send", action: "errno", errno: "EPIPE", repeat: 1, fd: fdOf(writer) }),
      );
      expect(events).toEqual(["write EPIPE in write", "error EPIPE in write"]);
    });

    test("the write callback, when destroy() closes the socket first", async () => {
      const { p, writer } = await pair();
      using _ = p;
      const events = await failedWrite(writer, () => storage.run("destroy", () => writer.destroy()));
      expect(events).toEqual(["write ERR_SOCKET_CLOSED in write"]);
    });
  });

  // The extra pipe of a child process is a socket over an fd it adopts, which has a handler table of its own.
  describe("a socket over an adopted fd", () => {
    async function childPipe() {
      const child = storage.run("opened", () =>
        spawn(bunExe(), ["-e", "setInterval(() => {}, 60_000)"], {
          env: bunEnv,
          stdio: ["ignore", "ignore", "inherit", "pipe"],
        }),
      );
      return {
        writer: child.stdio[3] as net.Socket,
        async [Symbol.asyncDispose]() {
          child.kill();
          await once(child, "exit");
        },
      };
    }

    test("the write callback, after the drain", async () => {
      await using pipe = await childPipe();
      takeOneByte(pipe.writer);
      const { promise, resolve } = Promise.withResolvers<{ error: Error | null; store: string | undefined }>();
      storage.run("write", () =>
        pipe.writer.write(Buffer.alloc(200, "w"), err => resolve({ error: err ?? null, store: storage.getStore() })),
      );
      expect(await promise).toEqual({ error: null, store: "write" });
    });

    test("the write callback and one 'error', when the flush from the writable event fails", async () => {
      await using pipe = await childPipe();
      const events = await failedWrite(pipe.writer, () =>
        fault.set({ syscall: "send", action: "errno", errno: "EPIPE", repeat: 1, fd: fdOf(pipe.writer) }),
      );
      expect(events).toEqual(["write EPIPE in write", "error EPIPE in write"]);
    });
  });

  async function connectedTLSPair() {
    const server = tls.createServer({ key: certs.key, cert: certs.cert });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const accepted = once(server, "secureConnection") as Promise<[tls.TLSSocket]>;
    const client = tls.connect({
      port: (server.address() as net.AddressInfo).port,
      host: "127.0.0.1",
      rejectUnauthorized: false,
    });
    const [[serverSock]] = await Promise.all([accepted, once(client, "secureConnect")]);
    return {
      client,
      serverSock,
      [Symbol.dispose]() {
        client.destroy();
        serverSock.destroy();
        server.close();
      },
    };
  }

  describe.each(["client", "server"] as const)("TLS %s", side => {
    async function pair() {
      const p = await storage.run("opened", connectedTLSPair);
      const [writer, reader] = side === "client" ? [p.client, p.serverSock] : [p.serverSock, p.client];
      reader.on("error", () => {});
      return { p, writer, reader };
    }

    test("the write callback and 'drain', when the rest of the ciphertext leaves from the writable event", async () => {
      const { p, writer, reader } = await pair();
      using _ = p;
      reader.resume();
      const stores: Record<string, string | undefined> = {};
      const { promise, resolve } = Promise.withResolvers<void>();
      writer.once("drain", () => {
        stores.drain = storage.getStore();
        resolve();
      });
      fault.set({ syscall: "send", action: "short", bytes: 1024, repeat: 1, fd: fdOf(writer) });
      storage.run("write", () =>
        writer.write(Buffer.alloc(4 * writer.writableHighWaterMark, "w"), () => (stores.callback = storage.getStore())),
      );
      await promise;
      expect(stores).toEqual({ callback: "write", drain: "write" });
    });

    // The engine holds a write whose ciphertext the kernel rejected until the close that fails it.
    test("the write callback and 'error', when the kernel rejects the ciphertext at once", async () => {
      const { p, writer } = await pair();
      using _ = p;
      const events: string[] = [];
      writer.on("error", (e: NodeJS.ErrnoException) => events.push(`error ${e.code} in ${storage.getStore()}`));
      const closed = new Promise<void>(resolve => writer.once("close", () => resolve()));
      fault.set({ syscall: "send", action: "errno", errno: "ECONNRESET", repeat: -1, fd: fdOf(writer) });
      storage.run("write", () =>
        writer.write("hello", err =>
          events.push(`write ${(err as NodeJS.ErrnoException)?.code} in ${storage.getStore()}`),
        ),
      );
      await closed;
      expect(events).toEqual(["write ECONNRESET in write", "error ECONNRESET in write"]);
    });
  });

  // Node's handle takes that write inside write(). Here the wrap holds it until the handle is attached.
  test("TLS: the callback of a write issued in the tick of a server-side wrap", async () => {
    const raw = net.createServer();
    raw.listen(0, "127.0.0.1");
    await once(raw, "listening");
    const written = Promise.withResolvers<string | undefined>();
    let secured: tls.TLSSocket | undefined;
    raw.on("connection", socket => {
      secured = storage.run("wrap", () => new tls.TLSSocket(socket, { isServer: true, ...certs }));
      secured.on("error", written.reject);
      storage.run("write", () => secured!.write("banner", () => written.resolve(storage.getStore())));
    });
    const client = tls.connect({
      port: (raw.address() as net.AddressInfo).port,
      host: "127.0.0.1",
      rejectUnauthorized: false,
    });
    try {
      client.on("error", written.reject);
      client.resume();
      expect(await written.promise).toBe("write");
    } finally {
      client.destroy();
      secured?.destroy();
      raw.close();
    }
  });

  test("TLS: the end() callback and 'finish' of a socket that ends before its handshake", async () => {
    const server = tls.createServer({ key: certs.key, cert: certs.cert }, socket => {
      socket.on("error", () => {}).resume();
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const client = storage.run("opened", () =>
      tls.connect({ port: (server.address() as net.AddressInfo).port, host: "127.0.0.1", rejectUnauthorized: false }),
    );
    try {
      const stores: Record<string, string | undefined> = {};
      const { promise, resolve, reject } = Promise.withResolvers<void>();
      client.on("error", reject);
      client.once("finish", () => (stores.finish = storage.getStore()));
      client.once("connect", () => {
        // The empty write completes at once, and the stream ends from inside its completion: end() only marks the end.
        storage.run("write", () => client.write(""));
        storage.run("end", () =>
          client.end(() => {
            stores.callback = storage.getStore();
            resolve();
          }),
        );
      });
      await promise;
      expect(stores).toEqual({ finish: "write", callback: "write" });
    } finally {
      client.destroy();
      server.close();
    }
  });

  test("node:http: the write callback and 'drain' of the socket an 'upgrade' listener gets", async () => {
    const stores: Record<string, string | undefined> = {};
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    const server = http.createServer();
    server.on("upgrade", (_request, socket) => {
      socket.on("error", reject);
      socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: test\r\nConnection: Upgrade\r\n\r\n");
      socket.once("drain", () => {
        stores.drain = storage.getStore();
        resolve();
      });
      // The client has sent all it sends, so the next send in the process is this write's.
      fault.set({ syscall: "send", action: "short", bytes: 1, repeat: 1 });
      storage.run("write", () =>
        socket.write(Buffer.alloc(2 * socket.writableHighWaterMark, "w"), () => (stores.callback = storage.getStore())),
      );
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const client = net.connect((server.address() as net.AddressInfo).port, "127.0.0.1");
    try {
      client.on("error", reject);
      client.resume();
      client.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n");
      await promise;
      expect(stores).toEqual({ callback: "write", drain: "write" });
    } finally {
      client.destroy();
      server.close();
    }
  });

  test("node:http: the callback of an 'upgrade' socket's write that the close of the connection fails", async () => {
    // More than the kernel takes while the client does not read.
    const big = Buffer.alloc(8 * 1024 * 1024);
    const { promise, resolve } = Promise.withResolvers<{ failed: boolean; store: string | undefined }>();
    const server = http.createServer();
    server.on("upgrade", (_request, socket) => {
      socket.on("error", () => {});
      socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: test\r\nConnection: Upgrade\r\n\r\n");
      storage.run("write", () => socket.write(big, err => resolve({ failed: err != null, store: storage.getStore() })));
      client.destroy();
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const client = net.connect((server.address() as net.AddressInfo).port, "127.0.0.1");
    try {
      client.on("error", () => {});
      client.pause();
      client.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n");
      expect(await promise).toEqual({ failed: true, store: "write" });
    } finally {
      client.destroy();
      server.close();
    }
  });

  // A Bun.ModuleGraph context rides the async context too. A write on a socket that another script opened
  // completes as that script: the rest of the socket's life must not go with a graph that only wrote.
  describe("and Bun.ModuleGraph", () => {
    const contextOf = (graph: Bun.ModuleGraph) =>
      Bun.ModuleGraph.current === graph ? "the graph" : Bun.ModuleGraph.current ? "another graph" : "the host";

    // The events of `socket` as they come, with the script each listener ran as.
    function eventsOf(socket: net.Socket, graph: Bun.ModuleGraph) {
      const events: string[] = [];
      for (const event of ["drain", "finish", "close"]) {
        socket.on(event, () => events.push(`'${event}' as ${contextOf(graph)}`));
      }
      return events;
    }

    test("on a socket of the graph's own: in the async context of the write", async () => {
      using graph = new Bun.ModuleGraph();
      using p = await graph.run(() => storage.run("opened", connectedPair));
      takeOneByte(p.client);
      const { promise, resolve } = Promise.withResolvers<{ store: string | undefined; context: string }>();
      graph.run(() =>
        storage.run("write", () =>
          p.client.write(Buffer.alloc(200, "w"), () =>
            resolve({ store: storage.getStore(), context: contextOf(graph) }),
          ),
        ),
      );
      expect(await promise).toEqual({ store: "write", context: "the graph" });
    });

    test("on a socket of the host's: 'drain' and the callback of a disposed graph's write run as the host", async () => {
      using p = await connectedPair();
      p.serverSock.resume();
      const graph = new Bun.ModuleGraph();
      const events = eventsOf(p.client, graph);
      const drained = once(p.client, "drain");
      takeOneByte(p.client);
      graph.run(() =>
        p.client.write(Buffer.alloc(2 * p.client.writableHighWaterMark, "w"), () =>
          events.push("write callback as " + contextOf(graph)),
        ),
      );
      graph.dispose();
      await drained;
      expect(events).toEqual(["'drain' as the host", "write callback as the host"]);
    });

    test("on a socket of the host's: the end(chunk) of a disposed graph still finishes and closes the socket", async () => {
      // The peer's FIN comes first, so the end of the write side is what closes the socket.
      const server = net.createServer({ allowHalfOpen: true }, socket => {
        socket.on("error", () => {});
        socket.end();
        socket.resume();
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const client = net.connect({
        port: (server.address() as net.AddressInfo).port,
        host: "127.0.0.1",
        allowHalfOpen: true,
      });
      try {
        client.resume();
        await once(client, "end");
        const graph = new Bun.ModuleGraph();
        const events = eventsOf(client, graph);
        const closed = once(client, "close");
        takeOneByte(client);
        graph.run(() => client.end(Buffer.alloc(200, "w"), () => events.push("end callback as " + contextOf(graph))));
        graph.dispose();
        await closed;
        expect(events).toEqual(["end callback as the host", "'finish' as the host", "'close' as the host"]);
      } finally {
        client.destroy();
        server.close();
      }
    });

    test("on a socket of the host's: a disposed graph's write that the host's destroy() fails", async () => {
      using p = await connectedPair();
      const graph = new Bun.ModuleGraph();
      const events = eventsOf(p.client, graph);
      const closed = once(p.client, "close");
      takeOneByte(p.client);
      graph.run(() =>
        p.client.write(Buffer.alloc(200, "w"), err =>
          events.push(`write callback (${(err as NodeJS.ErrnoException)?.code}) as ${contextOf(graph)}`),
        ),
      );
      graph.dispose();
      p.client.destroy();
      await closed;
      expect(events).toEqual(["write callback (ERR_SOCKET_CLOSED) as the host", "'close' as the host"]);
    });

    test("on a connection of the host's Agent: the Agent serves its next request after the graph is disposed", async () => {
      // More than the kernel takes while the peer does not read.
      const body = Buffer.alloc(8 * 1024 * 1024);
      let connections = 0;
      const upload = Promise.withResolvers<net.Socket>();
      // Speaks HTTP by hand: it answers the first request, stops reading at the second, and answers on later connections.
      const origin = net.createServer({ allowHalfOpen: true }, socket => {
        const first = ++connections === 1;
        let chunks = 0;
        socket.on("error", () => {});
        socket.on("data", chunk => {
          if (!first) {
            if (chunk.includes("\r\n\r\n")) {
              socket.end("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
            }
          } else if (++chunks === 1) {
            socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok");
          } else if (chunks === 2) {
            socket.pause();
            upload.resolve(socket);
          }
        });
      });
      origin.listen(0, "127.0.0.1");
      await once(origin, "listening");
      const port = (origin.address() as net.AddressInfo).port;
      const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
      const get = (path: string) => {
        const { promise, resolve, reject } = Promise.withResolvers<number | undefined>();
        http
          .get({ agent, port, host: "127.0.0.1", path }, response => {
            response.resume();
            response.on("end", () => resolve(response.statusCode));
          })
          .on("error", reject);
        return promise;
      };
      try {
        const pooled = once(agent, "free");
        const first = await get("/first");
        await pooled;

        // The graph's upload goes out on the connection the host's request opened, and waits on it.
        const graph = new Bun.ModuleGraph();
        graph.run(() => {
          const request = http.request({
            agent,
            port,
            host: "127.0.0.1",
            method: "POST",
            path: "/upload",
            headers: { "content-length": body.length },
          });
          request.on("error", () => {});
          request.end(body);
        });
        const peer = await upload.promise;
        graph.dispose();

        // The origin refuses the upload and closes. The Agent has one connection: the next request needs a new one.
        peer.end("HTTP/1.1 413 Payload Too Large\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
        peer.resume();
        expect({ first, next: await get("/next"), connections }).toEqual({ first: 200, next: 200, connections: 2 });
      } finally {
        agent.destroy();
        origin.close();
      }
    });
  });
});

describe.skipIf(skip)("node:net torture loop (exhaustive Nth-call failure)", () => {
  // curl/SQLite torture pattern: for i in 1..N, clamp the i-th and later
  // recv()s to 1 byte and assert the full payload is always reassembled —
  // proves there is no position i at which a short read corrupts framing.
  test("recv → 1-byte short starting at every position i in 1..N delivers intact payload", async () => {
    const payload = Buffer.alloc(64, "T");
    for (let i = 0; i < 10; i++) {
      using p = await connectedPair(s => s.on("error", () => {}));
      let received = Buffer.alloc(0);
      p.client.on("data", (c: Buffer) => (received = Buffer.concat([received, c])));
      fault.set({ syscall: "recv", action: "short", bytes: 1, after: i, repeat: -1 });
      p.serverSock.write(payload);
      p.serverSock.end();
      await once(p.client, "end");
      fault.clear();
      expect(received.equals(payload)).toBe(true);
    }
  });
});

describe.skipIf(skip)("node:net seeded syscall fuzz", () => {
  // Deterministic xorshift seeded from env so CI failures are reproducible.
  const seed = Number(process.env.BUN_SOCKET_FUZZ_SEED ?? 0x2b1a) >>> 0 || 1;
  function makePrng(s: number) {
    return () => {
      s ^= s << 13;
      s ^= s >>> 17;
      s ^= s << 5;
      return (s >>> 0) / 0x1_0000_0000;
    };
  }

  // The fuzz exercises chunking boundaries: every plan is a "short" clamp
  // that must still deliver the full payload. errno injection is covered by
  // the deterministic tests above; mixing it here would require per-fd
  // targeting (both client and server live in this process and share the
  // process-wide rule table).
  const PLANS = [
    { syscall: "recv", action: "short", bytes: 1 },
    { syscall: "recv", action: "short", bytes: 3 },
    { syscall: "recv", action: "short", bytes: 7 },
    { syscall: "recv", action: "short", bytes: 13 },
    { syscall: "send", action: "short", bytes: 1 },
    { syscall: "send", action: "short", bytes: 5 },
    { syscall: "send", action: "short", bytes: 11 },
  ] as const;

  test("randomized short-read/short-write echo round-trips deliver intact and never crash", async () => {
    const rand = makePrng(seed);
    for (let i = 0; i < 24; i++) {
      const plan = PLANS[Math.floor(rand() * PLANS.length)]!;
      const after = Math.floor(rand() * 3);

      let echoed = Buffer.alloc(0);
      using p = await connectedPair(s => {
        s.on("data", c => s.write(c));
        s.on("error", () => {});
      });
      p.client.on("error", () => {});
      p.client.on("data", (c: Buffer) => (echoed = Buffer.concat([echoed, c])));

      fault.set({ ...plan, after, repeat: -1 } as any);

      const payload = Buffer.alloc(64, i & 0xff);
      p.client.write(payload);
      while (echoed.length < payload.length) {
        await once(p.client, "data");
      }
      fault.clear();
      expect(echoed.equals(payload)).toBe(true);
      p.client.destroy();
      await once(p.client, "close").catch(() => {});
      expect(p.client.destroyed).toBe(true);
    }
  });
});
