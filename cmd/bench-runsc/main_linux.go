//go:build linux

// bench-runsc is a single-purpose spike for measuring runsc cold-start
// latency end-to-end. It mirrors the worker's runsc invocation exactly
// (same flags, OCI spec, sentrystack plugin) without going through
// Manager.Create, so per-phase wall times are exposed.
//
// Run as root in a cgroup v2 Linux host. /run/clrk/{runsc,images} must
// be writable. The binary doubles as runsc (tryDispatchRunsc) so the
// fork+exec self-recurses into gVisor's maincli.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"gvisor.dev/gvisor/runsc/cli/maincli"

	"github.com/apoxy-dev/clrk/internal/sentrystack"
	"github.com/apoxy-dev/clrk/internal/worker/sandbox"
)

// --- dispatcher (verbatim copy of cmd/worker/cli_linux.go subset) ----

var runscSubcommands = map[string]struct{}{
	"checkpoint": {}, "create": {}, "delete": {}, "events": {}, "exec": {},
	"kill": {}, "list": {}, "ps": {}, "pause": {}, "restore": {}, "resume": {},
	"run": {}, "spec": {}, "start": {}, "state": {}, "update": {}, "wait": {},
	"do": {}, "fscheckpoint": {}, "port-forward": {}, "tar": {}, "install": {},
	"mitigate": {}, "uninstall": {}, "nvproxy": {}, "trace": {}, "cpu-features": {},
	"debug": {}, "statefile": {}, "symbolize": {}, "usage": {}, "read-control": {},
	"write-control": {}, "metric-metadata": {}, "metric-export": {}, "metric-server": {},
	"boot": {}, "gofer": {}, "umount": {}, "help": {}, "flags": {},
}

func tryDispatchRunsc() {
	for _, a := range os.Args[1:] {
		if len(a) > 0 && a[0] == '-' {
			continue
		}
		if _, ok := runscSubcommands[a]; !ok {
			return
		}
		if sentrystack.Singleton() == nil {
			fmt.Fprintf(os.Stderr, "bench-runsc: sentrystack PluginStack not registered\n")
			os.Exit(1)
		}
		maincli.Main()
		return
	}
}

// ---------------------------- benchmark ------------------------------

const (
	runscRootDir = "/run/clrk/runsc"
	imageBaseDir = "/run/clrk/images"
	runscNetwork = "plugin"
)

var runscPlatform = "systrap"

func main() {
	tryDispatchRunsc()

	var (
		iters    = flag.Int("iters", 12, "iterations (excluding warmup)")
		warmup   = flag.Int("warmup", 2, "warmup iterations (excluded from stats)")
		image    = flag.String("image", "docker.io/library/alpine:3.20", "OCI image ref")
		cmdline  = flag.String("cmd", "/bin/true", "command (space-separated) to run inside sandbox")
		cgroup   = flag.String("cgroup", "", "absolute cgroup v2 path to delegate to runsc subprocess (empty = no CLONE_INTO_CGROUP)")
		jsonOut  = flag.String("json", "", "if set, append per-iteration JSON rows to this path")
		quiet    = flag.Bool("quiet", false, "suppress per-iteration log lines")
		noDebug  = flag.Bool("no-debug", false, "drop --debug --debug-log flags (no internal phase breakdown but lower I/O)")
		platform = flag.String("platform", "systrap", "runsc platform (systrap|kvm|ptrace)")
		probe    = flag.Bool("probe", false, "print effective GOMAXPROCS/NumCPU/Threads then exit 0; for the inittrace harness (E1/E2/E4)")
	)
	flag.Parse()
	runscPlatform = *platform

	// -probe is a no-boot fast path for the membarrier/inittrace harness:
	// Go's package init() (including hostmm's membarrier register) has
	// already run by the time main() executes, so GODEBUG=inittrace=1 has
	// emitted its dump to stderr. We just report the parent process's
	// effective parallelism (NumCPU is the box; GOMAXPROCS is what the env
	// requested; Threads is how many OS threads actually exist now — the
	// quantity the "walks every thread" story claims membarrier scales with)
	// and exit before any image pull or root requirement.
	if *probe {
		fmt.Printf("# GOMAXPROCS=%d NumCPU=%d Threads=%d\n", runtime.GOMAXPROCS(0), runtime.NumCPU(), procThreads())
		return
	}

	// Exec-path workerd bench is a separate driver (exec_workerd_linux.go).
	if *execMode == "exec-workerd" {
		runExecWorkerdMode()
		return
	}

	fmt.Printf("# Parent GOMAXPROCS=%d NumCPU=%d Threads=%d\n", runtime.GOMAXPROCS(0), runtime.NumCPU(), procThreads())

	if os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "bench-runsc: must run as root (cgroup + mount setup)")
		os.Exit(1)
	}

	if err := os.MkdirAll(runscRootDir, 0o755); err != nil {
		die("mkdir runsc root: %v", err)
	}
	if err := os.MkdirAll(imageBaseDir, 0o755); err != nil {
		die("mkdir image dir: %v", err)
	}

	cgroupFD, err := prepareCgroup(*cgroup)
	if err != nil {
		die("preparing cgroup: %v", err)
	}
	if cgroupFD != nil {
		defer cgroupFD.Close()
	}

	ctx := context.Background()
	store := sandbox.NewImageStore(imageBaseDir)

	// Cold image pull (separate measurement — not part of per-iter loop)
	t0 := time.Now()
	imgInfo, err := store.EnsureImage(ctx, *image)
	if err != nil {
		die("ensuring image: %v", err)
	}
	pullCold := time.Since(t0)

	t0 = time.Now()
	_, _ = store.EnsureImage(ctx, *image)
	pullWarm := time.Since(t0)
	fmt.Printf("# Image: %s\n", *image)
	fmt.Printf("# Rootfs: %s\n", imgInfo.RootFS)
	fmt.Printf("# Pull (cold/cached): %v / %v\n", pullCold, pullWarm)
	fmt.Printf("# Platform: %s | Network: %s | Cmd: %s\n", runscPlatform, runscNetwork, *cmdline)
	fmt.Printf("# Iters: warmup=%d measured=%d cgroup=%q\n", *warmup, *iters, *cgroup)

	var jf *os.File
	if *jsonOut != "" {
		jf, err = os.OpenFile(*jsonOut, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			die("opening jsonOut: %v", err)
		}
		defer jf.Close()
	}

	cmd := strings.Fields(*cmdline)
	if len(cmd) == 0 {
		cmd = []string{"/bin/true"}
	}

	var results []iterResult
	total := *warmup + *iters
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("bench-%d-%d", os.Getpid(), i)
		r, err := runIter(ctx, store, *image, cmd, id, cgroupFD, !*quiet, !*noDebug)
		if err != nil {
			fmt.Fprintf(os.Stderr, "iter %d (%s) failed: %v\n", i, id, err)
			continue
		}
		r.IsWarmup = i < *warmup
		r.IterIndex = i
		if jf != nil {
			b, _ := json.Marshal(r)
			fmt.Fprintln(jf, string(b))
		}
		if !*quiet {
			tag := "MEASURE"
			if r.IsWarmup {
				tag = "warmup "
			}
			fmt.Printf("[%s %2d] %s\n", tag, i, r.OneLine())
		}
		if !r.IsWarmup {
			results = append(results, r)
		}
	}

	if len(results) == 0 {
		die("no measured iterations completed")
	}
	printStats(results)
}

// ---------------------------- iteration ------------------------------

type iterResult struct {
	IterIndex int  `json:"iter"`
	IsWarmup  bool `json:"warmup"`

	// Outer wall times.
	AllocIPs       time.Duration `json:"alloc_ips"`
	SpecBuild      time.Duration `json:"spec_build"`
	BundleWrite    time.Duration `json:"bundle_write"`
	InitStr        time.Duration `json:"initstr"`
	RunscCreate    time.Duration `json:"runsc_create"`
	RunscStart     time.Duration `json:"runsc_start"`
	RunscWait      time.Duration `json:"runsc_wait"`
	RunscDelete    time.Duration `json:"runsc_delete"`
	CoreBoot       time.Duration `json:"core_boot"`       // runsc create + start
	WallToExit     time.Duration `json:"wall_to_exit"`    // everything before runsc delete
	OverallElapsed time.Duration `json:"overall_elapsed"` // including delete

	// Internal phases parsed from runsc --debug-log (μs precision).
	// Each is duration from the first log line of the corresponding
	// runsc subcommand (create / start) to the matched marker.
	BootMarkers map[string]time.Duration `json:"boot_markers,omitempty"`
}

func (r iterResult) OneLine() string {
	return fmt.Sprintf("core=%6.1fms create=%6.1fms start=%6.1fms wait=%6.1fms delete=%6.1fms",
		ms(r.CoreBoot), ms(r.RunscCreate), ms(r.RunscStart), ms(r.RunscWait), ms(r.RunscDelete))
}

func runIter(ctx context.Context, store *sandbox.ImageStore, imageRef string, processArgs []string,
	id string, cgroupFD *os.File, verbose, debugLogs bool) (iterResult, error) {

	overall := time.Now()
	var r iterResult

	// (re-)ensure image — should be a cache hit after first call
	imgInfo, err := store.EnsureImage(ctx, imageRef)
	if err != nil {
		return r, fmt.Errorf("ensure image: %w", err)
	}

	// 1) IP allocation (host-side counter — sub-microsecond but timed).
	t := time.Now()
	gw, sbIP, err := allocateIPs()
	if err != nil {
		return r, fmt.Errorf("alloc ips: %w", err)
	}
	r.AllocIPs = time.Since(t)

	// 2) Build OCI spec.
	t = time.Now()
	spec := buildSpec(id, imgInfo.RootFS, processArgs)
	r.SpecBuild = time.Since(t)

	// 3) Bundle dir + config.json.
	t = time.Now()
	bundle := filepath.Join(runscRootDir, id+"-bundle")
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		return r, fmt.Errorf("mkdir bundle: %w", err)
	}
	if err := writeConfigJSON(bundle, spec); err != nil {
		return r, fmt.Errorf("write config.json: %w", err)
	}
	r.BundleWrite = time.Since(t)

	// 4) InitStr.
	t = time.Now()
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
		return r, fmt.Errorf("encode initstr: %w", err)
	}
	r.InitStr = time.Since(t)

	debugLog := filepath.Join(runscRootDir, id+".debug.log")
	if os.Getenv("BENCH_KEEP_LOGS") == "" {
		defer os.Remove(debugLog)
	}
	defer os.RemoveAll(bundle)

	debugArgs := []string{}
	if debugLogs {
		debugArgs = []string{"--debug", "--debug-log=" + debugLog, "--panic-log=" + debugLog}
	}

	// 5) runsc create.
	t = time.Now()
	createArgs := append([]string{}, debugArgs...)
	createArgs = append(createArgs, "create", "--bundle="+bundle, id)
	if err := runRunsc(ctx, "create", cgroupFD, initStr, createArgs); err != nil {
		_ = runRunsc(ctx, "delete", nil, "", []string{"delete", "--force", id})
		return r, fmt.Errorf("runsc create: %w (log tail: %s)", err, logTail(debugLog, 4096))
	}
	r.RunscCreate = time.Since(t)

	// 6) runsc start.
	t = time.Now()
	startArgs := append([]string{}, debugArgs...)
	startArgs = append(startArgs, "start", id)
	if err := runRunsc(ctx, "start", nil, initStr, startArgs); err != nil {
		_ = runRunsc(ctx, "delete", nil, "", []string{"delete", "--force", id})
		return r, fmt.Errorf("runsc start: %w (log tail: %s)", err, logTail(debugLog, 4096))
	}
	r.RunscStart = time.Since(t)
	r.CoreBoot = r.RunscCreate + r.RunscStart

	// 7) runsc wait (init process exit).
	t = time.Now()
	_ = runRunsc(ctx, "wait", nil, "", []string{"wait", id})
	r.RunscWait = time.Since(t)
	r.WallToExit = time.Since(overall)

	// 8) runsc delete.
	t = time.Now()
	_ = runRunsc(ctx, "delete", nil, "", []string{"delete", "--force", id})
	r.RunscDelete = time.Since(t)
	r.OverallElapsed = time.Since(overall)

	// 9) Parse debug log for internal phase boundaries.
	r.BootMarkers = parseBootMarkers(debugLog)

	_ = verbose
	return r, nil
}

// runRunsc fork+exec /proc/self/exe with runsc argv. Mirrors the worker.
func runRunsc(ctx context.Context, label string, cgroupFD *os.File, initStr string, args []string) error {
	full := append([]string{
		"--root=" + runscRootDir,
		"--network=" + runscNetwork,
		"--ignore-cgroups",
		"--platform=" + runscPlatform,
		"--TESTONLY-unsafe-nonroot=true",
	}, args...)
	cmd := exec.CommandContext(ctx, "/proc/self/exe", full...)
	if cgroupFD != nil && label == "create" {
		cmd.SysProcAttr = &syscall.SysProcAttr{
			UseCgroupFD: true,
			CgroupFD:    int(cgroupFD.Fd()),
		}
	}
	cmd.Env = os.Environ()
	if initStr != "" {
		cmd.Env = append(cmd.Env, sentrystack.InitStrEnv+"="+initStr)
	}
	// Stdio MUST be raw *os.File pointing at /dev/null — NOT a Go pipe.
	// runsc's long-lived boot+gofer children inherit these FDs and keep
	// the write-end open for the life of the sandbox. If we hand exec
	// an *os.File backed by a Go pipe (e.g. bytes.Buffer => internal
	// goroutine reader), cmd.Wait blocks until ALL writers close —
	// which is `runsc delete` time, not `runsc create` time.
	dn, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer dn.Close()
	cmd.Stdin = dn
	cmd.Stdout = dn
	cmd.Stderr = dn
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w (see debug log)", label, err)
	}
	return nil
}

// ------------------------- OCI spec mirroring ------------------------

func buildSpec(id, rootfs string, args []string) *specs.Spec {
	caps := []string{"CAP_NET_BIND_SERVICE"}
	return &specs.Spec{
		Version: "1.0.0",
		Process: &specs.Process{
			User: specs.User{UID: 0, GID: 0},
			Args: args,
			Env:  []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			Cwd:  "/",
			Capabilities: &specs.LinuxCapabilities{
				Bounding: caps, Effective: caps, Permitted: caps, Ambient: caps,
			},
			NoNewPrivileges: true,
			Rlimits: []specs.POSIXRlimit{
				{Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024},
			},
		},
		Root:     &specs.Root{Path: rootfs, Readonly: true},
		Hostname: id,
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"noexec", "nosuid", "nodev"}},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755"}},
			{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"ro", "noexec", "nosuid", "nodev"}},
			{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup", Options: []string{"ro", "noexec", "nosuid", "nodev", "relatime"}},
			{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
			{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=1777", "size=100M"}},
		},
		Linux: &specs.Linux{
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.MountNamespace},
				{Type: specs.UTSNamespace},
				{Type: specs.IPCNamespace},
				{Type: specs.PIDNamespace},
				{Type: specs.CgroupNamespace},
				{Type: specs.NetworkNamespace, Path: "/proc/self/ns/net"},
			},
			MaskedPaths:   []string{"/proc/kcore", "/sys/firmware"},
			ReadonlyPaths: []string{"/proc/sys", "/proc/sysrq-trigger", "/proc/irq", "/proc/bus"},
			Resources:     &specs.LinuxResources{},
			CgroupsPath:   filepath.Join("/system", id),
		},
	}
}

func writeConfigJSON(bundleDir string, spec *specs.Spec) error {
	f, err := os.OpenFile(filepath.Join(bundleDir, "config.json"),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(spec)
}

// ---------------------------- IP allocator ---------------------------

var ipNext uint32

func allocateIPs() (gw, ctr netip.Addr, err error) {
	ipNext++
	off := ipNext * 4
	base := [4]byte{10, 200, 0, 0}
	hp := uint32(base[2])<<8 | uint32(base[3])
	hp += off
	base[2] = byte(hp >> 8)
	base[3] = byte(hp)
	gw = netip.AddrFrom4([4]byte{base[0], base[1], base[2], base[3] + 1})
	ctr = netip.AddrFrom4([4]byte{base[0], base[1], base[2], base[3] + 2})
	return gw, ctr, nil
}

// ----------------------------- cgroup --------------------------------

func prepareCgroup(path string) (*os.File, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", path, err)
	}
	// best-effort: enable controllers on the parent
	parent := filepath.Dir(path)
	_ = os.WriteFile(filepath.Join(parent, "cgroup.subtree_control"), []byte("+cpu +memory"), 0o644)
	systemDir := filepath.Join(path, "system")
	if err := os.MkdirAll(systemDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir system: %w", err)
	}
	// Per-iteration cgroup will be system/<id>; open the parent FD so
	// clone3 attaches each runsc-create subprocess via CLONE_INTO_CGROUP.
	f, err := os.OpenFile(systemDir, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open system cgroup dir: %w", err)
	}
	return f, nil
}

// ------------------------- log parsing -------------------------------

// runsc emits klog-style lines like:
//   I0522 18:13:00.123456  12345 file.go:42] message...
// We grep for several well-known marker phrases inside the per-sandbox
// debug log, then return time deltas from the first line of the file.
// Phase names are stable identifiers consumed by the analysis script.

// Markers grouped by phase. Strings must appear in the runsc debug log
// (verified against real output 2026-05-22). Phases compose into a
// timeline of the boot subprocess's wall-clock progression.
var bootMarkers = []struct {
	name string
	sub  string
}{
	// runsc create CLI (parent of boot+gofer):
	{"01_create.start", "Create container"},        // container.go:201 — runsc create entered
	{"02_gofer.started", "Gofer started, PID"},     // container.go:1552 — gofer fork+exec done
	{"03_sandbox.started", "Sandbox started, PID"}, // sandbox.go:1331 — sentry fork done

	// boot subprocess re-exec (after applyCaps):
	{"04_seccomp.install", "Installing seccomp filters"}, // sandbox/seccomp.go:67
	{"05_seccomp.done", "Seccomp filters installed"},     // sandbox/seccomp.go:87
	{"06_gofer.serving", "Serving \"/\" mapped"},         // runsc/cmd/gofer.go:339 — gofer serving root
	{"07_loader.cpus", "CPUs:"},                          // loader.go:519
	{"08_loader.platform", "Platform: systrap"},          // loader.go:891 (after platform init)
	{"09_loader.memory", "Setting total memory"},         // loader.go:649
	{"10_loader.packet", "Packet logging"},               // loader.go:712

	// runsc start CLI:
	{"20_start.connect", "Connecting to sandbox"},       // sandbox.go:796 — start CLI dials Sentry
	{"21_start.rpc", "Start root sandbox"},              // sandbox.go:440 — RPC sent
	{"22_start.rpc.recv", "containerManager.StartRoot"}, // controller.go:275 — Sentry receives StartRoot
	{"23_start.kernel", "Process should have started"},  // loader.go:1117 — k.Start has returned
}

// runscLogTSLayout: gVisor logs as "Sxxxx HH:MM:SS.uuuuuu" where Sxxxx
// is severity-letter + 4-digit MMDD. Time substring runs from offset 1
// to offset 21 inclusive (20 chars: "MMDD HH:MM:SS.uuuuuu").
const runscLogTSLayout = "0102 15:04:05.000000"

func parseBootMarkers(path string) map[string]time.Duration {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	lines := strings.Split(string(b), "\n")
	var firstTS time.Time
	out := make(map[string]time.Duration)
	for _, ln := range lines {
		if len(ln) < 22 {
			continue
		}
		if ln[0] != 'I' && ln[0] != 'W' && ln[0] != 'E' && ln[0] != 'D' && ln[0] != 'F' {
			continue
		}
		tsStr := ln[1:21] // 20 chars = "MMDD HH:MM:SS.uuuuuu"
		ts, err := time.Parse(runscLogTSLayout, tsStr)
		if err != nil {
			continue
		}
		if firstTS.IsZero() {
			firstTS = ts
		}
		body := ln[21:]
		for _, m := range bootMarkers {
			if _, set := out[m.name]; set {
				continue
			}
			if strings.Contains(body, m.sub) {
				out[m.name] = ts.Sub(firstTS)
			}
		}
	}
	return out
}

func logTail(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}

// ----------------------------- stats ---------------------------------

func printStats(rs []iterResult) {
	fmt.Println()
	fmt.Println("================= AGGREGATE (warmups excluded) =================")
	fields := []struct {
		name string
		pick func(iterResult) time.Duration
	}{
		{"alloc_ips    ", func(r iterResult) time.Duration { return r.AllocIPs }},
		{"spec_build   ", func(r iterResult) time.Duration { return r.SpecBuild }},
		{"bundle_write ", func(r iterResult) time.Duration { return r.BundleWrite }},
		{"initstr      ", func(r iterResult) time.Duration { return r.InitStr }},
		{"runsc_create ", func(r iterResult) time.Duration { return r.RunscCreate }},
		{"runsc_start  ", func(r iterResult) time.Duration { return r.RunscStart }},
		{"runsc_wait   ", func(r iterResult) time.Duration { return r.RunscWait }},
		{"runsc_delete ", func(r iterResult) time.Duration { return r.RunscDelete }},
		{"CORE_BOOT    ", func(r iterResult) time.Duration { return r.CoreBoot }},
		{"WALL_TO_EXIT ", func(r iterResult) time.Duration { return r.WallToExit }},
		{"OVERALL      ", func(r iterResult) time.Duration { return r.OverallElapsed }},
	}
	fmt.Printf("%-14s %10s %10s %10s %10s %10s\n", "phase", "min", "p50", "mean", "p95", "max")
	for _, f := range fields {
		vals := make([]float64, 0, len(rs))
		for _, r := range rs {
			vals = append(vals, ms(f.pick(r)))
		}
		min, p50, mean, p95, max := summarize(vals)
		fmt.Printf("%-14s %9.2fms %9.2fms %9.2fms %9.2fms %9.2fms\n",
			f.name, min, p50, mean, p95, max)
	}

	// Boot markers (mean μs from start of debug log per phase)
	fmt.Println()
	fmt.Println("============ DEBUG-LOG BOOT MARKERS (mean across iters) ============")
	agg := make(map[string][]float64)
	for _, r := range rs {
		for k, v := range r.BootMarkers {
			agg[k] = append(agg[k], ms(v))
		}
	}
	if len(agg) == 0 {
		fmt.Println("(no markers matched — check that --debug-log emitted lines)")
		return
	}
	keys := make([]string, 0, len(agg))
	for k := range agg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, p50, mean, _, max := summarize(agg[k])
		fmt.Printf("%-22s p50=%6.2fms mean=%6.2fms max=%6.2fms n=%d\n",
			k, p50, mean, max, len(agg[k]))
	}
}

func summarize(xs []float64) (min, p50, mean, p95, max float64) {
	if len(xs) == 0 {
		return
	}
	sort.Float64s(xs)
	min = xs[0]
	max = xs[len(xs)-1]
	p50 = xs[len(xs)/2]
	idx := int(math.Ceil(0.95*float64(len(xs)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(xs) {
		idx = len(xs) - 1
	}
	p95 = xs[idx]
	sum := 0.0
	for _, v := range xs {
		sum += v
	}
	mean = sum / float64(len(xs))
	return
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// procThreads reports the number of OS threads in this process by reading
// the "Threads:" field of /proc/self/status. Used to show that at the point
// membarrier registration runs, the thread count is small and roughly
// constant regardless of GOMAXPROCS (Go creates Ms lazily), which is the
// counter-evidence to the "register walks every thread" framing.
func procThreads() int {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, ln := range strings.Split(string(b), "\n") {
		if rest, ok := strings.CutPrefix(ln, "Threads:"); ok {
			n, err := strconv.Atoi(strings.TrimSpace(rest))
			if err != nil {
				return -1
			}
			return n
		}
	}
	return -1
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "bench-runsc: "+format+"\n", args...)
	os.Exit(1)
}

// keep strconv used so vet doesn't complain on platforms missing parts.
var _ = strconv.Itoa
