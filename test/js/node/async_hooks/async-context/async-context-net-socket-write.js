process.exitCode = 1;
const { AsyncLocalStorage } = require("async_hooks");
const net = require("net");

const asyncLocalStorage = new AsyncLocalStorage();
let failed = false;

const server = net.createServer(socket => {
  socket.on("data", () => {
    socket.end();
  });
});

server.listen(0, () => {
  const port = server.address().port;

  asyncLocalStorage.run({ test: "net.Socket.write" }, () => {
    const client = net.connect(port);

    client.on("connect", () => {
      // Test write callback
      client.write("test data", err => {
        if (asyncLocalStorage.getStore()?.test !== "net.Socket.write") {
          console.error("FAIL: net.Socket write callback lost context");
          failed = true;
        }
      });

      // Test end callback
      client.end("final data", err => {
        if (asyncLocalStorage.getStore()?.test !== "net.Socket.write") {
          console.error("FAIL: net.Socket end callback lost context");
          failed = true;
        }
      });
    });

    client.on("close", () => {
      server.close();
      asyncLocalStorage.exit(writesThatWait).then(() => process.exit(failed ? 1 : 0));
    });
  });
});

// More than the kernel takes in one call while the peer does not read: the write has to wait for the socket.
const big = Buffer.alloc(8 * 1024 * 1024);

function expectStore(what, expected) {
  const store = asyncLocalStorage.getStore();
  if (store !== expected) {
    console.error(`FAIL: ${what} ran in ${JSON.stringify(store)}, expected ${JSON.stringify(expected)}`);
    failed = true;
  }
}

// A connected pair. listen() and connect() each run in a context of their own: a callback that runs where the socket
// was opened, and not where the write was issued, sees "listen" or "connect". Neither side reads until it is resumed.
function pair() {
  return new Promise((resolve, reject) => {
    const accepted = [];
    let client;
    let ready = 0;
    const connected = () => ++ready === 2 && resolve({ server, client, accepted });
    const server = net.createServer(socket => {
      socket.pause();
      socket.on("error", () => {});
      accepted.push(socket);
      connected();
    });
    server.on("error", reject);
    asyncLocalStorage.run("listen", () =>
      server.listen(0, "127.0.0.1", () => {
        client = asyncLocalStorage.run("connect", () => net.connect(server.address().port, "127.0.0.1"));
        client.pause();
        client.on("error", () => {});
        client.once("connect", connected);
      }),
    );
  });
}

// Runs `start(sockets, done)` on a new pair and waits until `done` was called `count` times.
async function scenario(count, start) {
  const sockets = await pair();
  await new Promise(resolve => start(sockets, () => --count === 0 && resolve()));
  sockets.client.destroy();
  for (const socket of sockets.accepted) socket.destroy();
  sockets.server.close();
}

async function writesThatWait() {
  await scenario(2, ({ client, accepted: [accepted] }, done) => {
    client.once("drain", () => {
      expectStore("'drain' after a write that waited", "write");
      done();
    });
    asyncLocalStorage.run("write", () =>
      client.write(big, () => {
        expectStore("the callback of a write that waited", "write");
        done();
      }),
    );
    accepted.resume();
  });

  await scenario(2, ({ client, accepted: [accepted] }, done) => {
    client.once("finish", () => {
      expectStore("'finish' after an end(chunk) that waited", "end");
      done();
    });
    asyncLocalStorage.run("end", () =>
      client.end(big, () => {
        expectStore("the callback of an end(chunk) that waited", "end");
        done();
      }),
    );
    accepted.resume();
  });

  // The stream hands a queued write to the socket when the write before it completes, so it inherits that context.
  await scenario(2, ({ client, accepted: [accepted] }, done) => {
    asyncLocalStorage.run("first write", () =>
      client.write(big, () => {
        expectStore("the callback of the first of two writes", "first write");
        done();
      }),
    );
    // Windows takes a write of any size at once: nothing is queued behind it then.
    const queued = client.writableLength > 0;
    asyncLocalStorage.run("second write", () =>
      client.write("second", () => {
        expectStore(
          `the callback of a write behind one that ${queued ? "waited" : "did not wait"}`,
          queued ? "first write" : "second write",
        );
        done();
      }),
    );
    accepted.resume();
  });

  // uncork() is what hands a corked write to the socket.
  await scenario(1, ({ client, accepted: [accepted] }, done) => {
    client.cork();
    asyncLocalStorage.run("write", () =>
      client.write(big, () => {
        expectStore("the callback of a corked write that waited", "uncork");
        done();
      }),
    );
    asyncLocalStorage.run("uncork", () => client.uncork());
    accepted.resume();
  });

  await scenario(1, ({ client, accepted: [accepted] }, done) => {
    asyncLocalStorage.run("write", () =>
      accepted.write(big, () => {
        expectStore("the callback of an accepted socket's write that waited", "write");
        done();
      }),
    );
    client.resume();
  });

  await scenario(1, ({ client, accepted: [accepted] }, done) => {
    client.write(big, () => {
      expectStore("the callback of a write issued outside any context", undefined);
      done();
    });
    accepted.resume();
  });

  // The peer never reads: destroy() is what completes the write.
  await scenario(1, ({ client }, done) => {
    asyncLocalStorage.run("write", () =>
      client.write(big, () => {
        expectStore("the callback of a waiting write that destroy() completed", "write");
        done();
      }),
    );
    client.destroy();
  });

  // A socket that is still connecting has nothing to write to: the write is issued from 'connect'.
  await scenario(1, ({ server }, done) => {
    const connecting = asyncLocalStorage.run("connect", () => net.connect(server.address().port, "127.0.0.1"));
    connecting.on("error", () => {});
    asyncLocalStorage.run("write", () =>
      connecting.write("early", () => {
        expectStore("the callback of a write issued while connecting", "connect");
        connecting.destroy();
        done();
      }),
    );
  });
}
