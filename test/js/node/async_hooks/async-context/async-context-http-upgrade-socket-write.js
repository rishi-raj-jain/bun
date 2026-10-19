process.exitCode = 1;
const { AsyncLocalStorage } = require("async_hooks");
const http = require("http");
const net = require("net");

const asyncLocalStorage = new AsyncLocalStorage();
let failed = false;

function expectStore(what, expected) {
  const store = asyncLocalStorage.getStore();
  if (store !== expected) {
    console.error(`FAIL: ${what} ran in ${JSON.stringify(store)}, expected ${JSON.stringify(expected)}`);
    failed = true;
  }
}

// More than the kernel takes in one call while the peer does not read: the write has to wait for the socket.
const big = Buffer.alloc(8 * 1024 * 1024);

const server = http.createServer();
let client;

// The socket an 'upgrade' listener gets is the connection itself, with the HTTP server out of the way.
server.on("upgrade", (req, socket) => {
  socket.on("error", () => {});
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: test\r\nConnection: Upgrade\r\n\r\n");

  let pending = 2;
  function done() {
    if (--pending > 0) return;
    client.destroy();
    socket.destroy();
    server.close();
    process.exit(failed ? 1 : 0);
  }

  socket.once("drain", () => {
    expectStore("'drain' after a write that waited on an upgraded socket", "write");
    done();
  });
  asyncLocalStorage.run("write", () =>
    socket.write(big, () => {
      expectStore("the callback of a write that waited on an upgraded socket", "write");
      done();
    }),
  );
  client.resume();
});

server.listen(0, "127.0.0.1", () => {
  client = net.connect(server.address().port, "127.0.0.1");
  client.pause();
  client.on("error", () => {});
  client.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n");
});
