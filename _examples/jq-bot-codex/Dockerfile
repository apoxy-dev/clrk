# Image for the jq-bot-codex example. The OpenAI Codex CLI replaces
# Claude Code as the in-sandbox LLM driver; everything else mirrors
# _examples/jq-bot.
#
# Build + push to a registry your worker pool can pull from, then point
# manifests/taskagent.yaml at the result. For `clrk dev` you can push to
# the local registry `clrk dev` starts and reference it as
# `clrk-registry:5000/<repo>:<tag>` in the TaskAgent's image field.
FROM node:22-alpine

# @openai/codex ships a prebuilt Rust binary per platform; on Alpine the
# npm postinstall selects the musl build. If your base or arch lacks a
# musl artifact, switch to a glibc base (e.g. node:22-slim).
RUN apk add --no-cache bash jq ca-certificates \
 && npm install -g @openai/codex \
 && rm -rf /root/.npm

# Codex refuses to start without an API key in the env. This placeholder
# is what the agent process sees; the egress MITM rewrites the
# `Authorization` header on every outbound /v1/responses call via the
# attached CredentialInjectionPolicy, so the real key never reaches this
# process. See manifests/credentialinjectionpolicy.yaml.
ENV OPENAI_API_KEY=clrk-injected-by-proxy

# Codex writes config/scratch under $HOME and $CODEX_HOME. These ENVs
# are belt-and-suspenders -- the worker does not propagate image-baked
# ENV (see the README gotcha), so agent.sh also exports them, and
# taskagent.yaml lists them in spec.env.
ENV HOME=/tmp
ENV CODEX_HOME=/tmp/.codex

COPY agent.sh /usr/local/bin/agent.sh
RUN chmod +x /usr/local/bin/agent.sh

ENTRYPOINT ["/usr/local/bin/agent.sh"]
