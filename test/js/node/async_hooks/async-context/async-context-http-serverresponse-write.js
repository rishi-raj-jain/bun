process.exitCode = 1;
const { AsyncLocalStorage } = require("async_hooks");
const http = require("http");
const net = require("net");

const asyncLocalStorage = new AsyncLocalStorage();
let failed = false;

let checked = 0;
function expectStore(what, expected) {
  checked++;
  const store = asyncLocalStorage.getStore();
  if (store !== expected) {
    console.error(`FAIL: ${what} ran in ${JSON.stringify(store)}, expected ${JSON.stringify(expected)}`);
    failed = true;
  }
}

// More than the kernel takes in one call while the peer does not read: the write has to wait for the socket.
const big = Buffer.alloc(8 * 1024 * 1024);

let client;
const server = http.createServer((request, response) => {
  // The response ends once the three callbacks ran, in whichever order they come.
  const ran = (what, expected) => {
    expectStore(what, expected);
    if (checked === 3) response.end();
  };
  response.setHeader("Content-Length", big.length + 1);
  response.once("drain", () => ran("'drain' of a response whose write waited", "first write"));
  asyncLocalStorage.run("first write", () =>
    response.write(big, () => ran("the callback of a response write that waited", "first write")),
  );
  // A turn of the loop later the first write is with the socket. A write that is queued behind it goes to the socket
  // when the first one completes, so it inherits that context.
  setImmediate(() => {
    // Windows takes a write of any size at once: nothing is queued behind it then.
    const queued = request.socket.writableLength > 0;
    asyncLocalStorage.run("second write", () =>
      response.write("z", () =>
        ran(
          `the callback of a response write behind one that ${queued ? "waited" : "did not wait"}`,
          queued ? "first write" : "second write",
        ),
      ),
    );
    client.resume();
  });
});

// The connection reaches the server through emit("connection"), as a proxy or a test harness hands one over.
const acceptor = net.createServer(socket => server.emit("connection", socket));
asyncLocalStorage.run("listen", () =>
  acceptor.listen(0, "127.0.0.1", () => {
    client = net.connect(acceptor.address().port, "127.0.0.1");
    client.pause();
    client.on("error", () => {});
    let received = 0;
    client.on("data", chunk => {
      received += chunk.length;
    });
    client.on("end", () => {
      if (checked !== 3) console.error(`FAIL: ${checked} of 3 callbacks ran`);
      if (received < big.length) console.error(`FAIL: the client got ${received} bytes`);
      acceptor.close();
      process.exit(failed || checked !== 3 || received < big.length ? 1 : 0);
    });
    client.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  }),
);
