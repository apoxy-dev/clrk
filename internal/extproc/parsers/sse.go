package parsers

import (
	"bytes"
	"encoding/json"
)

// scanSSEData iterates `data:` payloads in an SSE byte stream, oldest
// first, and hands each payload to fn. Lines that aren't `data:` (event
// type, comment, retry, id, etc.) are skipped. Multi-line `data:`
// continuations within one event are joined with '\n' per the SSE spec.
//
// Capture is keep-last-N for streamed responses, so the first event
// the helper sees is often a partial fragment. fn is responsible for
// tolerating decode failures silently — the contract is "I'll show you
// every payload I find; you decide which ones decode."
func scanSSEData(b []byte, fn func(payload []byte)) {
	if len(b) == 0 {
		return
	}
	var (
		event []byte
		line  []byte
	)
	flush := func() {
		if len(event) > 0 {
			fn(event)
		}
		event = event[:0]
	}
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			line = b
			b = nil
		} else {
			line = b[:i]
			b = b[i+1:]
		}
		// Trim a trailing CR (CRLF line endings).
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		if len(line) == 0 {
			flush()
			continue
		}
		if line[0] == ':' {
			// Comment line.
			continue
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			// Non-data field (event:, id:, retry:). Ignore.
			continue
		}
		payload := line[len("data:"):]
		// One optional space after the colon per the spec.
		if len(payload) > 0 && payload[0] == ' ' {
			payload = payload[1:]
		}
		if len(event) > 0 {
			event = append(event, '\n')
		}
		event = append(event, payload...)
	}
	flush()
}

// lastJSONLine returns the last newline-delimited byte slice in b that
// json.Unmarshal accepts into an empty interface. Returns nil if none
// decode. Used for application/x-ndjson streams where capture is
// keep-last-N: the final line is the one we want, but it may be
// missing or partial under tight caps.
func lastJSONLine(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	// Walk backwards line-by-line; first line that decodes wins.
	end := len(b)
	for end > 0 {
		start := bytes.LastIndexByte(b[:end], '\n')
		var line []byte
		if start < 0 {
			line = b[:end]
			end = 0
		} else {
			line = b[start+1 : end]
			end = start
		}
		// Trim trailing CR.
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		if len(line) == 0 {
			continue
		}
		var probe any
		if err := json.Unmarshal(line, &probe); err == nil {
			return line
		}
	}
	return nil
}
