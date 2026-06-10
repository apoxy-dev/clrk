package llmcall

import (
	"bytes"

	"github.com/apoxy-dev/clrk/internal/extproc/jsonx"
)

// ScanSSEData iterates `data:` payloads in an SSE byte stream, oldest
// first, and hands each payload to fn. Lines that aren't `data:` (event
// type, comment, retry, id, etc.) are skipped. Multi-line `data:`
// continuations within one event are joined with '\n' per the SSE spec.
//
// Capture is keep-last-N for streamed responses, so the first event
// the helper sees is often a partial fragment. fn is responsible for
// tolerating decode failures silently — the contract is "I'll show you
// every payload I find; you decide which ones decode."
func ScanSSEData(b []byte, fn func(payload []byte)) {
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

// SSEEvent is one reassembled server-sent event.
type SSEEvent struct {
	// Name is the "event:" field; empty for bare data frames.
	Name string
	// Data is the "data:" payload, multi-line continuations joined
	// with '\n' per the SSE spec.
	Data []byte
}

// SSEScanner incrementally reassembles SSE events across arbitrary
// chunk boundaries — a chunk may end mid-line, mid-field, even
// mid-rune. Feed returns the events the new chunk completed; Flush
// drains the final unterminated event at end of stream. The zero value
// is ready to use.
//
// Unlike ScanSSEData (whole-buffer, tolerant, telemetry-side), the
// scanner is strict about frame boundaries so stream codecs never
// decode a half-received payload.
type SSEScanner struct {
	buf      []byte // pending bytes, at most one partial line
	name     string
	data     []byte
	dataSeen bool
}

// Feed appends chunk and returns the events completed by it. Returned
// Data slices are owned by the caller (copied out of the buffer).
func (s *SSEScanner) Feed(chunk []byte) []SSEEvent {
	s.buf = append(s.buf, chunk...)
	var out []SSEEvent
	for {
		i := bytes.IndexByte(s.buf, '\n')
		if i < 0 {
			return out
		}
		line := s.buf[:i]
		s.buf = s.buf[i+1:]
		if ev := s.line(line); ev != nil {
			out = append(out, *ev)
		}
	}
}

// Flush returns the final event when the stream ended without the
// terminating blank line (complete field lines only), plus any
// leftover partial line — non-empty leftover means the stream was
// truncated mid-frame.
func (s *SSEScanner) Flush() (*SSEEvent, []byte) {
	leftover := s.buf
	s.buf = nil
	if !s.dataSeen {
		return nil, leftover
	}
	ev := s.emit()
	return ev, leftover
}

// line consumes one complete line and returns a dispatched event when
// the line is the frame-terminating blank.
func (s *SSEScanner) line(line []byte) *SSEEvent {
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	if len(line) == 0 {
		if !s.dataSeen {
			// Per spec a frame with no data is not dispatched.
			s.name = ""
			return nil
		}
		return s.emit()
	}
	if line[0] == ':' {
		return nil
	}
	field, value, _ := bytes.Cut(line, []byte(":"))
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	switch string(field) {
	case "event":
		s.name = string(value)
	case "data":
		if s.dataSeen {
			s.data = append(s.data, '\n')
		}
		s.data = append(s.data, value...)
		s.dataSeen = true
	}
	// id:, retry:, unknown fields: ignored.
	return nil
}

func (s *SSEScanner) emit() *SSEEvent {
	ev := &SSEEvent{Name: s.name, Data: append([]byte(nil), s.data...)}
	s.name = ""
	s.data = s.data[:0]
	s.dataSeen = false
	return ev
}

// AppendSSE appends one SSE frame to dst: an optional "event: name"
// line, then "data:" lines (data newlines become continuations), and
// the terminating blank line.
func AppendSSE(dst []byte, name string, data []byte) []byte {
	if name != "" {
		dst = append(dst, "event: "...)
		dst = append(dst, name...)
		dst = append(dst, '\n')
	}
	for {
		line, rest, more := bytes.Cut(data, []byte("\n"))
		dst = append(dst, "data: "...)
		dst = append(dst, line...)
		dst = append(dst, '\n')
		if !more {
			break
		}
		data = rest
	}
	dst = append(dst, '\n')
	return dst
}

// LastJSONLine returns the last newline-delimited byte slice in b that
// json.Unmarshal accepts into an empty interface. Returns nil if none
// decode. Used for application/x-ndjson streams where capture is
// keep-last-N: the final line is the one we want, but it may be
// missing or partial under tight caps.
func LastJSONLine(b []byte) []byte {
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
		if err := jsonx.Unmarshal(line, &probe); err == nil {
			return line
		}
	}
	return nil
}
