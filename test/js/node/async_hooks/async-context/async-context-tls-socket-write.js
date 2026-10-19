process.exitCode = 1;
const { AsyncLocalStorage } = require("async_hooks");
const fs = require("fs");
const path = require("path");
const tls = require("tls");

const asyncLocalStorage = new AsyncLocalStorage();
let failed = false;

function expectStore(what, expected) {
  const store = asyncLocalStorage.getStore();
  if (store !== expected) {
    console.error(`FAIL: ${what} ran in ${JSON.stringify(store)}, expected ${JSON.stringify(expected)}`);
    failed = true;
  }
}

const fixtures = path.join(__dirname, "..", "..", "tls", "fixtures");
const server = tls.createServer(
  {
    key: fs.readFileSync(path.join(fixtures, "agent1-key.pem")),
    cert: fs.readFileSync(path.join(fixtures, "agent1-cert.pem")),
  },
  socket => {
    socket.on("error", () => {});
    socket.resume();
  },
);

// Connects in a context of its own and runs `start(socket, done)` in the socket's 'connect' event. The TLS handshake
// has not completed then, so what the socket is given to write has to wait for it.
function beforeHandshake(count, start) {
  return new Promise(resolve => {
    const socket = asyncLocalStorage.run("connect", () =>
      tls.connect({ port: server.address().port, host: "127.0.0.1", rejectUnauthorized: false }),
    );
    socket.on("error", () => {});
    socket.once("connect", () =>
      asyncLocalStorage.exit(start, socket, () => {
        if (--count > 0) return;
        socket.destroy();
        resolve();
      }),
    );
  });
}

server.listen(0, "127.0.0.1", async () => {
  await beforeHandshake(1, (socket, done) => {
    asyncLocalStorage.run("write", () =>
      socket.write("x", () => {
        expectStore("the callback of a write before the handshake", "write");
        done();
      }),
    );
  });

  await beforeHandshake(2, (socket, done) => {
    socket.once("finish", () => {
      expectStore("'finish' after an end(chunk) before the handshake", "end");
      done();
    });
    asyncLocalStorage.run("end", () =>
      socket.end("x", () => {
        expectStore("the callback of an end(chunk) before the handshake", "end");
        done();
      }),
    );
  });

  // The empty write completes at once, so the stream ends from inside its completion: end() only marks the end.
  await beforeHandshake(3, (socket, done) => {
    socket.once("finish", () => {
      expectStore("'finish' after an empty write and end() before the handshake", "write");
      done();
    });
    asyncLocalStorage.run("write", () =>
      socket.write("", () => {
        expectStore("the callback of an empty write before the handshake", "write");
        done();
      }),
    );
    asyncLocalStorage.run("end", () =>
      socket.end(() => {
        expectStore("the callback of an end() behind an empty write before the handshake", "write");
        done();
      }),
    );
  });

  server.close();
  process.exit(failed ? 1 : 0);
});
