# Image for the jq-bot example. ~250MB; node + claude CLI dominate.
#
# Build + push to a registry your worker pool can pull from, then
# point manifests/taskagent.yaml at the resulting reference. For
# `clrk dev` you can `docker push` to the local registry
# `clrk dev` starts (port printed at startup) and reference it as
# `clrk-registry:5000/<repo>:<tag>` in the TaskAgent's image field.
FROM node:22-alpine

RUN apk add --no-cache bash jq ca-certificates curl \
 && npm install -g @anthropic-ai/claude-code \
 && mkdir -p /root/.claude/projects \
 && rm -rf /root/.npm

# Claude Code refuses to start without an API key in the env. The
# value here is a placeholder -- the egress MITM rewrites the
# `x-api-key` header on every Anthropic request via the attached
# CredentialInjectionPolicy, so the real key never reaches this
# process. See manifests/credentialinjectionpolicy.yaml.
ENV ANTHROPIC_API_KEY=clrk-injected-by-proxy

COPY agent.sh /usr/local/bin/agent.sh
RUN chmod +x /usr/local/bin/agent.sh

ENTRYPOINT ["/usr/local/bin/agent.sh"]
