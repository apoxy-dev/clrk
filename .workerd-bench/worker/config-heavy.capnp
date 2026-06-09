using Workerd = import "/workerd/workerd.capnp";

# Non-trivial worker: same shape as the echo config but embeds the generated
# heavy bundle (gen_heavy.py). Socket address is overridden per-iteration by
# the bench to a fresh unix domain socket.
const config :Workerd.Config = (
  services = [
    (name = "main", worker = .heavyWorker),
  ],
  sockets = [
    (name = "http", address = "unix:/tmp/workerd-default.sock", http = (), service = "main"),
  ],
);

const heavyWorker :Workerd.Worker = (
  modules = [
    (name = "worker-heavy.js", esModule = embed "worker-heavy.js"),
  ],
  compatibilityDate = "2024-01-01",
);
