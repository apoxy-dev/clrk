//go:build linux

// Exec-path benchmark: measure the timeline from the start of `runsc exec`
// until the first HTTP response byte from a Cloudflare workerd JS isolate
// launched inside an already-warm gVisor sandbox. This is the CLRK warm-pool
// dispatch path, but with a workerd-based agent instead of /bin/true.
//
// Selected with `-mode=exec-workerd`. It does two sweeps:
//
//   - warm-binary: one warm sandbox (init = sleep infinity) is created once,
//     then workerd is cold-started via `runsc exec` N times into it. After the
//     first few execs the workerd ELF is resident in the sentry page cache, so
//     this is the "reused pool member" number.
//   - cold-binary: a fresh sandbox per iteration, so the workerd binary faults
//     cold through the gofer every time. This is the "first dispatch into a
//     never-used pool member" number. The gap between the two is the value of
//     pre-warming workerd into the pool member.
//
// Each exec runs the static workerd-runner (cmd/workerd-runner), which spawns
// `workerd serve` for a trivial echo worker on a unix socket, connect-retries
// until it listens, and reads the first response byte. Two clock domains are
// kept separate: exec-dispatch latency is a pure host-clock interval (host
// stamps the arrival of the runner's first stdout byte); the workerd segments
// are measured on the in-sandbox monotonic clock and reported by the runner.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"

	"github.com/apoxy-dev/clrk/internal/sentrystack"
	"github.com/apoxy-dev/clrk/internal/worker/sandbox"
)

// Flags are registered at package init so the single flag.Parse() in main()
// picks them up. -mode selects this driver.
var (
	execMode      = flag.String("mode", "runsc-cold", "bench mode: runsc-cold (default, /bin/true cold boot) | exec-workerd")
	wkHostDir     = flag.String("workerd-host-dir", "/opt/workerd", "host directory bind-mounted read-only at /opt/workerd (workerd, runner, config, worker.js)")
	wkImage       = flag.String("workerd-image", "docker.io/library/debian:bookworm-slim", "OCI image for the warm sandbox rootfs (must be glibc, not musl)")
	wkWarmIters   = flag.Int("warm-iters", 100, "measured warm-binary exec iterations")
	wkWarmWarmup  = flag.Int("warm-warmup", 3, "warm-binary warmup execs (excluded)")
	wkColdIters   = flag.Int("cold-iters", 30, "measured cold-binary iterations (fresh sandbox each)")
	wkLibDir      = flag.String("workerd-libdir", "/opt/workerd/lib", "LD_LIBRARY_PATH dir inside the sandbox for workerd (glibc/libstdc++ fallback libs, if staged)")
	wkRunnerName  = flag.String("runner-name", "workerd-runner", "runner binary basename inside /opt/workerd")
	wkWorkerdName = flag.String("workerd-name", "workerd", "workerd binary basename inside /opt/workerd")
	wkConfigName  = flag.String("config-name", "config.capnp", "capnp config basename inside /opt/workerd")
	wkExecTimeout = flag.Duration("exec-timeout", 60*time.Second, "per-exec wall timeout")
	wkRequests    = flag.Int("requests", 1, "requests per workerd process: req[0]=cold isolate, req[1:]=warm (>1 prints warm-request latency tables)")
	wkJSON        = flag.String("workerd-json", "", "if set, append per-iteration JSON rows here")
)

const wkMountDest = "/opt/workerd"

// execResult is one measured exec → first-byte sample.
type execResult struct {
	Sweep               string  `json:"sweep"` // "warm" | "cold"
	Iter                int     `json:"iter"`
	Warmup              bool    `json:"warmup"`
	OK                  bool    `json:"ok"`
	DispatchUS          int64   `json:"dispatch_us"`            // host clock: runsc exec call -> runner running (incl. runner Go start)
	EntryToSpawnUS      int64   `json:"entry_to_spawn_us"`      // guest
	SpawnToListenUS     int64   `json:"spawn_to_listen_us"`     // guest
	ListenToFirstByteUS int64   `json:"listen_to_firstbyte_us"` // guest
	SpawnToFirstByteUS  int64   `json:"spawn_to_firstbyte_us"`  // guest
	EntryToFirstByteUS  int64   `json:"entry_to_firstbyte_us"`  // guest
	EndToEndUS          int64   `json:"end_to_end_us"`          // dispatch + entry_to_firstbyte (host+guest composed)
	ReqUS               []int64 `json:"req_us,omitempty"`       // per-request dial->first-byte (req[0]=cold, req[1:]=warm)
	Status              string  `json:"status"`
	Err                 string  `json:"err,omitempty"`
}

func runExecWorkerdMode() {
	if os.Geteuid() != 0 {
		die("exec-workerd: must run as root")
	}
	if err := os.MkdirAll(runscRootDir, 0o755); err != nil {
		die("mkdir runsc root: %v", err)
	}
	if err := os.MkdirAll(imageBaseDir, 0o755); err != nil {
		die("mkdir image dir: %v", err)
	}
	// Validate the staging dir up front so failures are obvious.
	for _, n := range []string{*wkWorkerdName, *wkRunnerName, *wkConfigName} {
		p := filepath.Join(*wkHostDir, n)
		if _, err := os.Stat(p); err != nil {
			die("staging file missing: %s (%v)", p, err)
		}
	}

	ctx := context.Background()
	store := sandbox.NewImageStore(imageBaseDir)

	t0 := time.Now()
	imgInfo, err := store.EnsureImage(ctx, *wkImage)
	if err != nil {
		die("ensuring image %q: %v", *wkImage, err)
	}
	pullCold := time.Since(t0)

	fmt.Printf("# ===== exec-workerd bench =====\n")
	fmt.Printf("# Image: %s (rootfs %s, pull %v)\n", *wkImage, imgInfo.RootFS, pullCold)
	fmt.Printf("# Platform: %s | Network: %s\n", runscPlatform, runscNetwork)
	fmt.Printf("# Stage: %s | runner=%s workerd=%s config=%s\n", *wkHostDir, *wkRunnerName, *wkWorkerdName, *wkConfigName)
	fmt.Printf("# Warm: warmup=%d measured=%d | Cold: measured=%d | Requests/proc: %d\n", *wkWarmWarmup, *wkWarmIters, *wkColdIters, *wkRequests)

	var jf *os.File
	if *wkJSON != "" {
		jf, err = os.OpenFile(*wkJSON, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			die("opening -workerd-json: %v", err)
		}
		defer jf.Close()
	}

	var warm, cold []execResult

	// ---- warm-binary sweep: one sandbox, many execs ----
	fmt.Printf("\n# --- warm-binary sweep (one warm sandbox, %d+%d execs) ---\n", *wkWarmWarmup, *wkWarmIters)
	warmID := fmt.Sprintf("wkwarm-%d", os.Getpid())
	cleanup, err := createWarmSandbox(ctx, store, warmID, imgInfo.RootFS)
	if err != nil {
		die("creating warm sandbox: %v", err)
	}
	total := *wkWarmWarmup + *wkWarmIters
	for i := 0; i < total; i++ {
		r := execOnce(ctx, warmID, "warm", i, i < *wkWarmWarmup)
		writeRow(jf, r)
		if !r.OK && r.Err != "" {
			fmt.Fprintf(os.Stderr, "[warm %d] FAIL: %s\n", i, r.Err)
		}
		if i < 5 || i%25 == 0 {
			fmt.Printf("[warm %3d%s] %s\n", i, warmTag(i < *wkWarmWarmup), r.oneLine())
		}
		if !r.Warmup && r.OK {
			warm = append(warm, r)
		}
	}
	cleanup()

	// ---- cold-binary sweep: fresh sandbox per iteration ----
	fmt.Printf("\n# --- cold-binary sweep (fresh sandbox each, %d execs) ---\n", *wkColdIters)
	for i := 0; i < *wkColdIters; i++ {
		id := fmt.Sprintf("wkcold-%d-%d", os.Getpid(), i)
		cl, cerr := createWarmSandbox(ctx, store, id, imgInfo.RootFS)
		if cerr != nil {
			fmt.Fprintf(os.Stderr, "[cold %d] sandbox create failed: %v\n", i, cerr)
			continue
		}
		r := execOnce(ctx, id, "cold", i, false)
		cl()
		writeRow(jf, r)
		if !r.OK && r.Err != "" {
			fmt.Fprintf(os.Stderr, "[cold %d] FAIL: %s\n", i, r.Err)
		}
		if i < 5 || i%10 == 0 {
			fmt.Printf("[cold %3d] %s\n", i, r.oneLine())
		}
		if r.OK {
			cold = append(cold, r)
		}
	}

	fmt.Println()
	printExecStats("WARM-BINARY (reused sandbox, steady state)", warm)
	printExecStats("COLD-BINARY (fresh sandbox, workerd faults cold)", cold)

	if *wkRequests > 1 {
		printRequestStats(warm)
	}
}

// printRequestStats pools per-request latencies across all warm samples and
// prints the cold-first-request and warm-request distributions. req[0] of each
// workerd process is its isolate's first request; req[1:] hit the now-warm
// process. Latency is dial -> first response byte over a fresh unix connection.
func printRequestStats(rs []execResult) {
	var coldFirst, warmReq []float64
	procs := 0
	for _, r := range rs {
		if len(r.ReqUS) == 0 {
			continue
		}
		procs++
		coldFirst = append(coldFirst, float64(r.ReqUS[0])/1000)
		for _, v := range r.ReqUS[1:] {
			warmReq = append(warmReq, float64(v)/1000)
		}
	}
	fmt.Printf("================= REQUEST LATENCY (fresh unix conn/req, dial->first byte) =================\n")
	fmt.Printf("%d workerd processes; req[0]=cold isolate first request, req[1:]=warm process\n", procs)
	fmt.Printf("%-34s %9s %9s %9s %9s %9s\n", "bucket", "min", "p50", "p90", "p99", "max")
	printReqRow("first request (cold isolate)   ", coldFirst)
	printReqRow("warm requests (same process)   ", warmReq)
	fmt.Println()
}

func printReqRow(label string, vals []float64) {
	if len(vals) == 0 {
		fmt.Printf("%-34s %s\n", label, "(no samples)")
		return
	}
	min, p50, p90, p99, mx := pctiles(vals)
	fmt.Printf("%-34s %7.3fms %7.3fms %7.3fms %7.3fms %7.3fms  n=%d\n", label, min, p50, p90, p99, mx, len(vals))
}

func warmTag(w bool) string {
	if w {
		return "*"
	}
	return " "
}

func (r execResult) oneLine() string {
	if !r.OK {
		return "FAILED: " + r.Err
	}
	return fmt.Sprintf("e2e=%6.1fms dispatch=%5.1fms spawn->listen=%6.1fms listen->1B=%5.1fms (%s)",
		float64(r.EndToEndUS)/1000, float64(r.DispatchUS)/1000,
		float64(r.SpawnToListenUS)/1000, float64(r.ListenToFirstByteUS)/1000, r.Status)
}

// createWarmSandbox creates+starts a sandbox whose init is `sleep infinity`,
// with /opt/workerd bind-mounted read-only. Returns a delete func.
func createWarmSandbox(ctx context.Context, store *sandbox.ImageStore, id, rootfs string) (func(), error) {
	gw, sbIP, err := allocateIPs()
	if err != nil {
		return nil, fmt.Errorf("alloc ips: %w", err)
	}
	spec := buildWorkerdSpec(id, rootfs)

	bundle := filepath.Join(runscRootDir, id+"-bundle")
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir bundle: %w", err)
	}
	if err := writeConfigJSON(bundle, spec); err != nil {
		os.RemoveAll(bundle)
		return nil, fmt.Errorf("write config.json: %w", err)
	}

	is := &sentrystack.InitStr{
		SandboxID:       id,
		Eth0V4:          sbIP.String(),
		Eth0V4PrefixLen: 32,
		GatewayV4:       gw.String(),
		Eth0MAC:         "02:00:00:00:00:01",
		IMDSHostAddr:    "127.0.0.1:1",
		EgressHostAddr:  "127.0.0.1:1",
	}
	initStr, err := is.Encode()
	if err != nil {
		os.RemoveAll(bundle)
		return nil, fmt.Errorf("encode initstr: %w", err)
	}

	if err := runRunsc(ctx, "create", nil, initStr, []string{"create", "--bundle=" + bundle, id}); err != nil {
		_ = runRunsc(ctx, "delete", nil, "", []string{"delete", "--force", id})
		os.RemoveAll(bundle)
		return nil, fmt.Errorf("runsc create: %w", err)
	}
	if err := runRunsc(ctx, "start", nil, initStr, []string{"start", id}); err != nil {
		_ = runRunsc(ctx, "delete", nil, "", []string{"delete", "--force", id})
		os.RemoveAll(bundle)
		return nil, fmt.Errorf("runsc start: %w", err)
	}
	cleanup := func() {
		_ = runRunsc(ctx, "delete", nil, "", []string{"delete", "--force", id})
		os.RemoveAll(bundle)
	}
	return cleanup, nil
}

// buildWorkerdSpec extends the base spec with the /opt/workerd bind mount, a
// long-lived init, and a higher fd limit. RLIMIT_AS is intentionally left
// unset so V8 can reserve its CodeRange/pointer-cage virtual region.
func buildWorkerdSpec(id, rootfs string) *specs.Spec {
	spec := buildSpec(id, rootfs, []string{"/bin/sleep", "infinity"})
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: wkMountDest,
		Type:        "bind",
		Source:      *wkHostDir,
		Options:     []string{"ro", "rbind"},
	})
	spec.Process.Rlimits = []specs.POSIXRlimit{
		{Type: "RLIMIT_NOFILE", Hard: 8192, Soft: 8192},
	}
	return spec
}

// execOnce runs one `runsc exec workerd-runner` into the sandbox and parses the
// runner's reported timeline.
func execOnce(ctx context.Context, id, sweep string, iter int, warmup bool) execResult {
	r := execResult{Sweep: sweep, Iter: iter, Warmup: warmup}
	sock := fmt.Sprintf("/tmp/wd-%s-%d.sock", sweep, iter)
	argv := []string{
		filepath.Join(wkMountDest, *wkRunnerName),
		"-workerd=" + filepath.Join(wkMountDest, *wkWorkerdName),
		"-config=" + filepath.Join(wkMountDest, *wkConfigName),
		"-sock=" + sock,
		"-libdir=" + *wkLibDir,
		fmt.Sprintf("-requests=%d", *wkRequests),
	}
	dispatch, stdout, stderrOut, err := runRunscExec(ctx, id, argv)
	if err != nil {
		r.Err = fmt.Sprintf("runsc exec: %v | stderr: %s", err, tail(stderrOut, 400))
		return r
	}
	rr, perr := parseRunnerJSON(stdout)
	if perr != nil {
		r.Err = fmt.Sprintf("parse runner output: %v | stdout: %q | stderr: %s", perr, tail(stdout, 400), tail(stderrOut, 400))
		return r
	}
	r.OK = rr.OK
	r.DispatchUS = dispatch.Microseconds()
	r.EntryToSpawnUS = rr.EntryToSpawnUS
	r.SpawnToListenUS = rr.SpawnToListenUS
	r.ListenToFirstByteUS = rr.ListenToFirstByteUS
	r.SpawnToFirstByteUS = rr.SpawnToFirstByteUS
	r.EntryToFirstByteUS = rr.EntryToFirstByteUS
	r.EndToEndUS = dispatch.Microseconds() + rr.EntryToFirstByteUS
	r.ReqUS = rr.ReqUS
	r.Status = rr.Status
	if rr.Err != "" {
		r.Err = rr.Err
	}
	return r
}

// runnerOut mirrors cmd/workerd-runner's result JSON.
type runnerOut struct {
	OK                  bool    `json:"ok"`
	EntryToSpawnUS      int64   `json:"entry_to_spawn_us"`
	SpawnToListenUS     int64   `json:"spawn_to_listen_us"`
	ListenToFirstByteUS int64   `json:"listen_to_firstbyte_us"`
	SpawnToFirstByteUS  int64   `json:"spawn_to_firstbyte_us"`
	EntryToFirstByteUS  int64   `json:"entry_to_firstbyte_us"`
	EntryToTeardownUS   int64   `json:"entry_to_teardown_us"`
	ReqUS               []int64 `json:"req_us"`
	Status              string  `json:"status"`
	Err                 string  `json:"err"`
}

func parseRunnerJSON(stdout string) (runnerOut, error) {
	var out runnerOut
	for _, ln := range strings.Split(stdout, "\n") {
		ln = strings.TrimSpace(ln)
		if !strings.HasPrefix(ln, "{") {
			continue
		}
		if err := json.Unmarshal([]byte(ln), &out); err == nil {
			return out, nil
		}
	}
	return out, fmt.Errorf("no JSON line found")
}

// runRunscExec runs `runsc exec` into id, capturing stdout via a raw pipe and
// stamping the arrival of the runner's first byte (the dispatch marker) on the
// host clock. The runner redirects workerd's stdio to /dev/null inside the
// sandbox, so nothing long-lived holds this pipe; it EOFs when the runner exits.
func runRunscExec(ctx context.Context, id string, argv []string) (dispatch time.Duration, stdout, stderrOut string, err error) {
	full := []string{
		"--root=" + runscRootDir,
		"--network=" + runscNetwork,
		"--ignore-cgroups",
		"--platform=" + runscPlatform,
		"--TESTONLY-unsafe-nonroot=true",
		"exec", "--cwd=/",
	}
	full = append(full, id)
	full = append(full, argv...)

	cctx, cancel := context.WithTimeout(ctx, *wkExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "/proc/self/exe", full...)
	cmd.Env = os.Environ()

	dn, derr := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if derr != nil {
		return 0, "", "", derr
	}
	defer dn.Close()
	cmd.Stdin = dn

	pr, pw, perr := os.Pipe()
	if perr != nil {
		return 0, "", "", perr
	}
	cmd.Stdout = pw
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf

	type readResult struct {
		first time.Time
		data  []byte
	}
	done := make(chan readResult, 1)
	go func() {
		var first time.Time
		var all []byte
		b := make([]byte, 4096)
		for {
			n, e := pr.Read(b)
			if n > 0 {
				if first.IsZero() {
					first = time.Now()
				}
				all = append(all, b[:n]...)
			}
			if e != nil {
				break
			}
		}
		done <- readResult{first, all}
	}()

	callStart := time.Now()
	if e := cmd.Start(); e != nil {
		pw.Close()
		pr.Close()
		return 0, "", errBuf.String(), e
	}
	pw.Close() // parent drops the write end so the reader EOFs on runner exit
	werr := cmd.Wait()
	rr := <-done
	pr.Close()

	if !rr.first.IsZero() {
		dispatch = rr.first.Sub(callStart)
	}
	return dispatch, string(rr.data), errBuf.String(), werr
}

func writeRow(jf *os.File, r execResult) {
	if jf == nil {
		return
	}
	b, _ := json.Marshal(r)
	fmt.Fprintln(jf, string(b))
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func printExecStats(title string, rs []execResult) {
	fmt.Printf("================= %s =================\n", title)
	if len(rs) == 0 {
		fmt.Println("(no successful samples)")
		fmt.Println()
		return
	}
	fmt.Printf("n=%d\n", len(rs))
	fields := []struct {
		name string
		pick func(execResult) float64
	}{
		{"exec_dispatch    ", func(r execResult) float64 { return float64(r.DispatchUS) / 1000 }},
		{"runner_entry->spawn", func(r execResult) float64 { return float64(r.EntryToSpawnUS) / 1000 }},
		{"workerd spawn->listen", func(r execResult) float64 { return float64(r.SpawnToListenUS) / 1000 }},
		{"listen->first_byte ", func(r execResult) float64 { return float64(r.ListenToFirstByteUS) / 1000 }},
		{"workerd spawn->1B  ", func(r execResult) float64 { return float64(r.SpawnToFirstByteUS) / 1000 }},
		{"END-TO-END exec->1B", func(r execResult) float64 { return float64(r.EndToEndUS) / 1000 }},
	}
	fmt.Printf("%-22s %9s %9s %9s %9s %9s\n", "segment", "min", "p50", "p90", "p99", "max")
	for _, f := range fields {
		vals := make([]float64, 0, len(rs))
		for _, r := range rs {
			vals = append(vals, f.pick(r))
		}
		min, p50, p90, p99, mx := pctiles(vals)
		fmt.Printf("%-22s %7.2fms %7.2fms %7.2fms %7.2fms %7.2fms\n", f.name, min, p50, p90, p99, mx)
	}
	fmt.Println()
}

func pctiles(xs []float64) (min, p50, p90, p99, max float64) {
	if len(xs) == 0 {
		return
	}
	sort.Float64s(xs)
	min = xs[0]
	max = xs[len(xs)-1]
	p50 = xs[pidx(len(xs), 0.50)]
	p90 = xs[pidx(len(xs), 0.90)]
	p99 = xs[pidx(len(xs), 0.99)]
	return
}

func pidx(n int, q float64) int {
	if n == 0 {
		return 0
	}
	i := int(q * float64(n))
	if i >= n {
		i = n - 1
	}
	return i
}
