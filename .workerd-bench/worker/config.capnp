using Workerd = import "/workerd/workerd.capnp";

# Minimal echo/hello service: one worker, one socket, no bindings. The socket
# address here is a placeholder; the bench overrides it per iteration with
# `--socket-addr http=unix:/tmp/wd-<iter>.sock` so each workerd cold start
# listens on a fresh unix domain socket (no TCP port reuse / TIME_WAIT).
const config :Workerd.Config = (
  services = [
    (name = "main", worker = .echoWorker),
  ],
  sockets = [
    (name = "http", address = "unix:/tmp/workerd-default.sock", http = (), service = "main"),
  ],
);

const echoWorker :Workerd.Worker = (
  modules = [
    (name = "worker.js", esModule = embed "worker.js"),
  ],
  compatibilityDate = "2024-01-01",
);
