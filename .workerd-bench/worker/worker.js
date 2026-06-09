// Trivial echo/hello worker: the runtime floor for a workerd cold start. The
// handler does no work beyond constructing a Response so the measured
// listen->first-byte segment is dominated by isolate init + top-level eval +
// first-request handler compile, not by user code.
export default {
  async fetch(req) {
    return new Response("ok");
  },
};
