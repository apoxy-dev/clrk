#!/bin/sh
# Per-invocation entrypoint for jq-bot-codex -- the jq-bot agent driven
# by the OpenAI Codex CLI instead of Claude Code. Same contract, same
# output; the only difference is which LLM CLI runs inside the sandbox
# and which provider clrk injects credentials for.
#
# Wire shape from the caller (identical to jq-bot):
#   { "input": <arbitrary JSON>, "want": "<natural-language description>" }
#
# clrk's dispatcher writes a CloudEvents structured-mode envelope on
# stdin; with content-type application/json the body is inlined under
# `.data` -- see internal/worker/agents/dispatcher.go::buildCEEnvelope.
set -eu

# Codex reads its config (and, unless --ephemeral, writes session state)
# under $CODEX_HOME and scratches under $HOME. A fresh clrk rootfs has
# neither writable in the way Codex expects, so pin both at /tmp.
# CODEX_HOME must already exist or Codex aborts at startup
# ("CODEX_HOME points to ... does not exist").
export HOME=/tmp
export CODEX_HOME=/tmp/.codex
mkdir -p "$CODEX_HOME"

# Force Codex onto the plain HTTP Responses API. Codex 0.139 defaults to
# a WebSocket transport (wss://api.openai.com/v1/responses, the
# `prefer_websockets` provider default) that ignores OPENAI_BASE_URL and
# that clrk's HTTP egress MITM can't intercept: a WebSocket is one
# long-lived Upgrade connection, so there is no per-inference HTTP
# request to match the AIProviderRoute against or to inject the key on,
# and the request/response bodies the telemetry codecs parse never
# appear as HTTP messages.
#
# A custom provider with wire_api="responses" makes Codex POST
# /v1/responses over ordinary HTTPS instead -- one request per model
# call, which the EgressGateway terminates and where the
# CredentialInjectionPolicy swaps the placeholder key for the real one.
# The built-in `openai` provider id can't be overridden, hence a
# distinct id. (wire_api="chat" -- the /v1/chat/completions surface --
# was removed in 0.139, so /v1/responses is the only HTTP option.)
cat > "$CODEX_HOME/config.toml" <<'TOML'
model = "gpt-5-mini"
model_provider = "openai_http"
approval_policy = "never"
sandbox_mode = "danger-full-access"

[model_providers.openai_http]
name = "openai-http"
base_url = "https://api.openai.com/v1"
env_key = "OPENAI_API_KEY"
wire_api = "responses"
TOML

ce=$(cat)

# The dispatcher delivers a CloudEvents JSON envelope with the request body
# inlined under .data. Validate it before building the prompt: a malformed
# body would otherwise abort under `set -e` with no output, and a missing
# .input would let jq write the literal `null` into input.json -- the model
# then "succeeds" with a filter computed over null and returns confident
# garbage. Reject both up front with a structured {filter:"", error} envelope.
if ! printf '%s' "$ce" | jq -e . >/dev/null 2>&1; then
  jq -nc '{filter:"", error:"request body is not valid JSON"}'
  exit 0
fi
if ! printf '%s' "$ce" | jq -e '(.data.input // null) != null' >/dev/null 2>&1; then
  jq -nc '{filter:"", error:"request body has no .input document"}'
  exit 0
fi
want=$(printf '%s' "$ce" | jq -r '.data.want // empty')
if [ -z "$want" ]; then
  jq -nc '{filter:"", error:"request body has no .want description"}'
  exit 0
fi
printf '%s' "$ce" | jq '.data.input' > /tmp/input.json
printf '%s' "$want" > /tmp/want.txt

prompt=$(cat <<EOF
Write ONE jq filter that, applied to /tmp/input.json, produces what
/tmp/want.txt describes.

JSON:
$(cat /tmp/input.json)

What to extract:
$(cat /tmp/want.txt)

Reply with ONLY the jq filter on a single line. No code fences, no
backticks, no commentary, no surrounding quotes.
EOF
)

# codex exec runs the agent non-interactively. The prompt is read from
# stdin (the `-` argument). Flags:
#   --skip-git-repo-check  the sandbox cwd is not a git repository;
#   --ephemeral            do not persist session files;
#   --dangerously-bypass-approvals-and-sandbox
#                          Codex's own landlock/seccomp sandbox can't
#                          initialize inside gVisor -- clrk IS the
#                          sandbox, so Codex must not try to nest one;
#   -o /tmp/last.txt       write ONLY the final agent message (the
#                          filter), keeping it out of the framing noise
#                          codex exec prints to stdout/stderr.
printf '%s' "$prompt" | codex exec \
  --skip-git-repo-check --ephemeral \
  --dangerously-bypass-approvals-and-sandbox \
  -o /tmp/last.txt - >/tmp/codex.log 2>&1 || true

raw=$(cat /tmp/last.txt 2>/dev/null || true)

# Strip code fences of any language (```jq, ```json, bare ```) and surrounding
# backticks the model may have added, then take the first non-empty line -- the
# prompt asks for a single-line filter.
filter=$(printf '%s' "$raw" \
  | sed -e 's/^[[:space:]]*```[a-zA-Z]*[[:space:]]*$//' \
  | sed -e 's/^`*//' -e 's/`*$//' \
  | awk 'NF{print; exit}')

if [ -z "$filter" ]; then
  jq -nc --arg e "$(tail -c 400 /tmp/codex.log 2>/dev/null)" \
    '{filter:"", codex_error:$e}'
  exit 0
fi

# Run the filter; -c gives compact JSON. Filters that return scalars or
# multiple values produce multi-line output -- wrap in a JSON array so
# the final response stays a single object.
raw_out=$(jq -c "$filter" /tmp/input.json 2>/tmp/jq.err) || {
  jq -nc --arg f "$filter" --arg e "$(cat /tmp/jq.err)" \
    '{filter:$f, error:$e}'
  exit 0
}

# Collapse multi-line jq output into a JSON array via jq -s; single
# values pass through as that one value.
output=$(printf '%s' "$raw_out" | jq -sc 'if length==1 then .[0] else . end')

jq -nc --arg f "$filter" --argjson o "$output" '{filter:$f, output:$o}'
