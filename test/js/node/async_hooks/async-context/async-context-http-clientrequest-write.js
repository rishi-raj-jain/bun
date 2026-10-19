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

// Speaks HTTP by hand: it answers each request once all of its body came.
let peerSocket;
const peer = net.createServer(socket => {
  peerSocket = socket;
  let head = "";
  let bodyLeft = -1;
  socket.on("error", () => {});
  socket.on("data", chunk => {
    if (bodyLeft < 0) {
      head += chunk.toString("latin1");
      const end = head.indexOf("\r\n\r\n");
      if (end < 0) return;
      bodyLeft = Number(/content-length: (\d+)/i.exec(head)[1]) - (Buffer.byteLength(head, "latin1") - (end + 4));
      head = "";
    } else {
      bodyLeft -= chunk.length;
    }
    if (bodyLeft === 0) {
      bodyLeft = -1;
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok");
    }
  });
});

peer.listen(0, "127.0.0.1", () => {
  const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
  const options = { agent, port: peer.address().port, host: "127.0.0.1", method: "POST" };

  // The first request opens the connection, in a context of its own.
  asyncLocalStorage.run("first request", () => {
    const first = http.request({ ...options, headers: { "Content-Length": 5 } }, response => {
      response.resume();
    });
    first.end("hello");
  });

  // The second request goes out on that connection, once the Agent has it back.
  agent.once("free", () => {
    peerSocket.pause();
    const second = asyncLocalStorage.run("second request", () =>
      http.request({ ...options, headers: { "Content-Length": big.length } }, response => {
        response.resume();
        response.on("end", () => {
          if (checked !== 2) console.error(`FAIL: ${checked} of 2 callbacks ran`);
          agent.destroy();
          peer.close();
          process.exit(failed || checked !== 2 ? 1 : 0);
        });
      }),
    );
    second.on("drain", () => {
      expectStore("'drain' of a request whose write waited on a reused connection", "write");
      second.end();
    });
    second.once("socket", () => {
      asyncLocalStorage.run("write", () =>
        second.write(big, () => {
          expectStore("the callback of a request write that waited on a reused connection", "write");
        }),
      );
      peerSocket.resume();
    });
  });
});
