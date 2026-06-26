#!/bin/sh
# Per-invocation entrypoint for jq-bot.
#
# Wire shape from the caller:
#   { "input": <arbitrary JSON>, "want": "<natural-language description>" }
#
# The clrk dispatcher writes a CloudEvents structured-mode envelope on
# stdin. With content-type application/json the body is inlined under
# `.data` -- see internal/worker/dispatcher.go::buildCEEnvelope.
#
# Claude Code (Bun-bundled) writes session artifacts under $HOME at
# startup; the clrk sandbox's /root isn't writable in the way the CLI
# expects, so we pin HOME=/tmp via spec.env. With --bare and
# --no-session-persistence claude becomes pleasantly stateless.
set -eu

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

raw=$(printf '%s' "$prompt" | claude --print --bare --no-session-persistence --model haiku 2>/tmp/claude.err) || true

# Strip code fences of any language (```jq, ```json, bare ```) and surrounding
# backticks the model may have added, then take the first non-empty line -- the
# prompt asks for a single-line filter.
filter=$(printf '%s' "$raw" \
  | sed -e 's/^[[:space:]]*```[a-zA-Z]*[[:space:]]*$//' \
  | sed -e 's/^`*//' -e 's/`*$//' \
  | awk 'NF{print; exit}')

if [ -z "$filter" ]; then
  jq -nc --arg e "$(tail -c 400 /tmp/claude.err 2>/dev/null)" \
    '{filter:"", claude_error:$e}'
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
