// workerd-runner is a tiny, static helper that the exec-path benchmark
// (cmd/bench-runsc -mode=exec-workerd) execs into a warm gVisor sandbox. It
// launches `workerd serve` for a trivial echo worker, waits for the isolate to
// answer its first HTTP request, and reports a per-segment timeline.
//
// Two clock domains are kept strictly separate (see the bench's methodology
// notes):
//
//   - The very first thing it does is write a one-byte marker to stdout. The
//     host stamps the arrival of that byte on the HOST monotonic clock; the
//     delta from "just before runsc exec" is the exec-dispatch latency, with no
//     cross-clock subtraction.
//   - Everything after that (spawn, listen, first byte) is measured on the
//     in-sandbox (guest) monotonic clock and reported as relative durations.
//
// workerd's own stdio is redirected to /dev/null inside the sandbox so the
// only holder of the host stdout pipe is this process; it closes on exit and
// the host reader sees EOF cleanly.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type result struct {
	OK                  bool    `json:"ok"`
	EntryToSpawnUS      int64   `json:"entry_to_spawn_us"`      // guest: runner entry -> just before workerd fork+exec
	SpawnToListenUS     int64   `json:"spawn_to_listen_us"`     // guest: workerd spawn -> unix socket accepts a connection
	ListenToFirstByteUS int64   `json:"listen_to_firstbyte_us"` // guest: connected -> first response byte (isolate eval + handler)
	SpawnToFirstByteUS  int64   `json:"spawn_to_firstbyte_us"`  // guest: workerd cold start to first byte
	EntryToFirstByteUS  int64   `json:"entry_to_firstbyte_us"`  // guest: full in-sandbox exec -> first byte
	EntryToTeardownUS   int64   `json:"entry_to_teardown_us"`   // guest: entry -> just before workerd is killed (teardown is outside this)
	Status              string  `json:"status"`                 // first response line, e.g. "HTTP/1.1 200 OK"
	ReqUS               []int64 `json:"req_us,omitempty"`       // guest: per-request dial->first-byte (req[0]=cold first request, req[1:]=warm)
	Err                 string  `json:"err,omitempty"`
}

func main() {
	// Guest-clock origin: stamp before anything else.
	entry := time.Now()

	var (
		workerd  = flag.String("workerd", "/opt/workerd/workerd", "path to the workerd binary")
		config   = flag.String("config", "/opt/workerd/config.capnp", "path to the capnp config")
		sock     = flag.String("sock", "/tmp/workerd.sock", "unix socket path workerd should listen on (overrides the config socket)")
		libdir   = flag.String("libdir", "", "directory to prepend to LD_LIBRARY_PATH for workerd (glibc/libstdc++ fallback)")
		logf     = flag.String("workerd-log", "", "where workerd stdio is written (default: <sock>.log); its tail is folded into err on failure")
		timeout  = flag.Duration("timeout", 30*time.Second, "max wait for the first response byte")
		requests = flag.Int("requests", 1, "number of requests to issue against the workerd process (req[0]=cold, req[1:]=warm)")
	)
	flag.Parse()

	// Dispatch marker. The host stamps the arrival of this byte (host clock).
	os.Stdout.Write([]byte("R\n"))

	// Fresh socket node each iteration.
	_ = os.Remove(*sock)

	// Always capture workerd stdio to a file so its error output (the main
	// gVisor risk: missing syscall, CodeRange OOM, dynamic-link failure) can be
	// folded into the JSON err on failure instead of being lost to /dev/null.
	wlog := *logf
	if wlog == "" {
		wlog = *sock + ".log"
	}
	wOut := openSink(wlog)
	defer wOut.Close()

	// workerd resolves `embed` paths relative to the config file's directory.
	cfgDir := filepath.Dir(*config)
	cmd := exec.Command(*workerd, "serve", filepath.Base(*config),
		"--socket-addr", "http=unix:"+*sock)
	cmd.Dir = cfgDir
	cmd.Stdin = nil
	cmd.Stdout = wOut
	cmd.Stderr = wOut
	cmd.Env = os.Environ()
	if *libdir != "" {
		cmd.Env = append(cmd.Env, "LD_LIBRARY_PATH="+*libdir)
	}
	// Own process group so a single kill(-pid) reaps workerd and any children.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	spawn := time.Now()
	if err := cmd.Start(); err != nil {
		emit(result{Err: "workerd start: " + err.Error() + logSuffix(wlog), EntryToSpawnUS: us(spawn.Sub(entry))})
		os.Exit(1)
	}
	pid := cmd.Process.Pid
	deadline := time.Now().Add(*timeout)

	// Phase 1: connect-retry until workerd's listening socket accepts, then
	// close it -- it was only used to detect that workerd is listening, so the
	// requests below all measure a uniform dial+request (including req[0]).
	var listenAt time.Time
	for {
		if time.Now().After(deadline) {
			kill(pid)
			_, _ = cmd.Process.Wait()
			emit(result{Err: "timeout waiting for workerd to listen" + logSuffix(wlog), EntryToSpawnUS: us(spawn.Sub(entry))})
			os.Exit(1)
		}
		c, err := net.Dial("unix", *sock)
		if err == nil {
			listenAt = time.Now()
			_ = c.Close()
			break
		}
		// Tiny backoff: bounds the listen-detection error to ~200us without
		// busy-spinning a whole vCPU away from workerd on a small VM.
		time.Sleep(200 * time.Microsecond)
	}

	// Phase 2: issue N requests, a fresh unix connection each (Connection:
	// close, so EOF delimits the full response). req[0] is the cold isolate's
	// first request; req[1:] hit the now-warm workerd process. Each latency is
	// dial -> first response byte.
	n := *requests
	if n < 1 {
		n = 1
	}
	reqUS := make([]int64, 0, n)
	var firstByte time.Time
	var status, ferr string
	for k := 0; k < n; k++ {
		if time.Now().After(deadline) {
			ferr = "deadline during requests" + logSuffix(wlog)
			break
		}
		t0 := time.Now()
		c, err := net.Dial("unix", *sock)
		if err != nil {
			ferr = "dial req: " + err.Error() + logSuffix(wlog)
			break
		}
		_, _ = c.Write([]byte("GET / HTTP/1.1\r\nHost: bench\r\nConnection: close\r\n\r\n"))
		_ = c.SetReadDeadline(deadline)
		buf := make([]byte, 256)
		rn, rerr := c.Read(buf)
		tb := time.Now()
		// Close immediately after the first response byte -- do NOT drain to
		// EOF: workerd holds the HTTP/1.1 connection open, so a read-to-EOF
		// would block until the deadline. We only need dial -> first byte.
		_ = c.Close()
		if rerr != nil && rn == 0 {
			ferr = "read req: " + rerr.Error() + logSuffix(wlog)
			break
		}
		reqUS = append(reqUS, us(tb.Sub(t0)))
		if k == 0 {
			firstByte = tb
			status = firstLine(buf[:max0(rn)])
		}
	}
	teardownAt := time.Now()

	// Teardown is outside every measured interval.
	kill(pid)
	_, _ = cmd.Process.Wait()
	_ = os.Remove(*sock)

	res := result{
		OK:                len(reqUS) > 0 && ferr == "",
		EntryToSpawnUS:    us(spawn.Sub(entry)),
		SpawnToListenUS:   us(listenAt.Sub(spawn)),
		EntryToTeardownUS: us(teardownAt.Sub(entry)),
		Status:            status,
		ReqUS:             reqUS,
		Err:               ferr,
	}
	if !firstByte.IsZero() {
		res.ListenToFirstByteUS = us(firstByte.Sub(listenAt))
		res.SpawnToFirstByteUS = us(firstByte.Sub(spawn))
		res.EntryToFirstByteUS = us(firstByte.Sub(entry))
	}
	emit(res)
}

// logSuffix returns a short, single-line tail of workerd's captured stdio,
// formatted for inclusion in the JSON err field.
func logSuffix(path string) string {
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		return ""
	}
	const n = 600
	if len(b) > n {
		b = b[len(b)-n:]
	}
	s := strings.ReplaceAll(strings.TrimSpace(string(b)), "\n", " | ")
	return " | workerd: " + s
}

func openSink(path string) *os.File {
	if path != "" {
		if f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644); err == nil {
			return f
		}
	}
	f, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	return f
}

func kill(pid int) { _ = syscall.Kill(-pid, syscall.SIGKILL) }

func us(d time.Duration) int64 { return d.Microseconds() }

func max0(n int) int {
	if n < 0 {
		return 0
	}
	return n
}

func firstLine(b []byte) string {
	s := string(b)
	if i := strings.IndexByte(s, '\r'); i >= 0 {
		return s[:i]
	}
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// emit writes the result as a single JSON line to stdout (after the marker
// line) so the host can parse it from the captured exec stdout.
func emit(r result) {
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}
