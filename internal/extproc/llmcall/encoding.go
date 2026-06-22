package llmcall

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"io"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

// maxDecodedBody bounds DecodeBody's output. The captured (compressed)
// input is already bounded by the per-stream CaptureBody cap, so this
// guards only the post-inflation size: a small, highly compressible body
// must not expand into an outsized allocation inside the ext_proc. 16 MiB
// is far above any real provider response we parse usage from.
const maxDecodedBody = 16 << 20

// errEncodingUnsupported marks a content-encoding token DecodeBody can't
// inflate; the caller treats it like any other decode failure (usage
// stays absent).
var errEncodingUnsupported = errors.New("unsupported content-encoding")

// DecodeBody returns body with its HTTP content-encoding undone, so a
// telemetry parser sees plaintext. It is the seam that lets usage parsing
// work against real provider traffic: agents (the Anthropic SDK / Claude
// Code, OpenAI SDK, etc.) send `accept-encoding: gzip, deflate, br, zstd`
// and clrk's MITM forwards it untouched, so providers gzip even SSE
// bodies. A compressed body has no `data:` lines for ScanSSEData to find,
// which silently zeroes every token count.
//
// For an identity / empty encoding or an empty body it returns body
// unchanged with ok=true, so callers can invoke it unconditionally. ok is
// false only when a recognized encoding failed to inflate -- a corrupt
// or, far more often, a partially captured stream. The caller then keeps
// treating usage as absent, exactly as for any unparseable body, and must
// NOT use the returned slice.
//
// Pass only a COMPLETE captured body. Streamed-response capture is
// keep-last-N, so a truncated body is a header-less tail that no codec
// can inflate; gate the call on !truncated.
func DecodeBody(body []byte, contentEncoding string) ([]byte, bool) {
	encs := parseEncodings(contentEncoding)
	if len(body) == 0 || len(encs) == 0 {
		return body, true
	}
	cur := body
	// Senders apply the listed encodings left to right, so the rightmost
	// token is the outermost layer on the wire; undo them in reverse.
	for i := len(encs) - 1; i >= 0; i-- {
		dec, err := inflate(cur, encs[i])
		if err != nil {
			return body, false
		}
		cur = dec
	}
	return cur, true
}

// parseEncodings splits a content-encoding header into lowercased tokens,
// dropping identity (a no-op layer the spec permits anywhere in the list).
func parseEncodings(h string) []string {
	if h == "" {
		return nil
	}
	var out []string
	for _, tok := range strings.Split(h, ",") {
		tok = strings.TrimSpace(strings.ToLower(tok))
		if tok == "" || tok == "identity" {
			continue
		}
		out = append(out, tok)
	}
	return out
}

func inflate(body []byte, enc string) ([]byte, error) {
	switch enc {
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return readCapped(zr)
	case "br":
		return readCapped(brotli.NewReader(bytes.NewReader(body)))
	case "zstd":
		zr, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return readCapped(zr)
	case "deflate":
		// content-encoding: deflate is zlib-wrapped per RFC 9110, but
		// some servers emit a raw DEFLATE stream. Try zlib first, fall
		// back to raw flate when the zlib header doesn't decode.
		if out, err := inflateZlib(body); err == nil {
			return out, nil
		}
		fr := flate.NewReader(bytes.NewReader(body))
		defer fr.Close()
		return readCapped(fr)
	default:
		return nil, errEncodingUnsupported
	}
}

func inflateZlib(body []byte) ([]byte, error) {
	zr, err := zlib.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return readCapped(zr)
}

// readCapped reads r fully but refuses to buffer more than maxDecodedBody
// bytes, so a small compressed body can't inflate into an outsized
// allocation (a decompression bomb, or merely an unexpectedly large
// response).
func readCapped(r io.Reader) ([]byte, error) {
	out, err := io.ReadAll(io.LimitReader(r, maxDecodedBody+1))
	if err != nil {
		return nil, err
	}
	if len(out) > maxDecodedBody {
		return nil, errors.New("decoded body exceeds cap")
	}
	return out, nil
}
