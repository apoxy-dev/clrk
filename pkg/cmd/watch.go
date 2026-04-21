package cmd

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// watcher drives rebuilds in --watch mode. It observes filesystem events on
// the source tree, debounces them, runs `make build-linux` via the golang
// builder container, then signals each driver that its binary is stale.
type watcher struct {
	root     string
	dataDir  string
	reload   map[string]func(context.Context) error
	buildMu  sync.Mutex
	debounce time.Duration
}

// newWatcher configures a watcher rooted at repoRoot. The `reload` map keys
// are source-tree paths (relative to repoRoot) that should trigger each
// driver's Reload; e.g. {"cmd/worker": worker.Reload}.
func newWatcher(repoRoot, dataDir string, reload map[string]func(context.Context) error) *watcher {
	return &watcher{
		root:     repoRoot,
		dataDir:  dataDir,
		reload:   reload,
		debounce: 750 * time.Millisecond,
	}
}

// run blocks until ctx is canceled. On every debounced batch of filesystem
// events it rebuilds binaries and invokes the affected reload funcs.
func (w *watcher) run(ctx context.Context) error {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("fsnotify: %w", err)
	}
	defer fw.Close()

	// Watch the top-level source directories. We walk each so subdirs get
	// picked up too — fsnotify on Linux does not recurse by default.
	for _, dir := range []string{"api", "cmd", "internal", "pkg"} {
		abs := filepath.Join(w.root, dir)
		if err := addTree(fw, abs); err != nil {
			slog.Warn("Failed to add watch tree", "dir", dir, "err", err)
		}
	}

	slog.Info("Watch mode active; rebuilding on source changes")

	var timer *time.Timer
	touched := make(map[string]bool)
	var mu sync.Mutex

	flush := func() {
		mu.Lock()
		changed := touched
		touched = make(map[string]bool)
		mu.Unlock()
		if len(changed) == 0 {
			return
		}
		if err := w.rebuild(ctx); err != nil {
			slog.Error("Rebuild failed", "err", err)
			return
		}
		w.triggerReloads(ctx, changed)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-fw.Events:
			if !ok {
				return nil
			}
			if !interesting(ev) {
				continue
			}
			// A new directory may have been created inside a watched
			// tree; start watching it.
			if ev.Op&fsnotify.Create != 0 {
				if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
					_ = addTree(fw, ev.Name)
				}
			}
			mu.Lock()
			rel, _ := filepath.Rel(w.root, ev.Name)
			touched[rel] = true
			mu.Unlock()
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(w.debounce, flush)
		case err, ok := <-fw.Errors:
			if !ok {
				return nil
			}
			slog.Warn("Fsnotify error", "err", err)
		}
	}
}

// rebuild invokes `make build-linux` under a mutex so overlapping events
// don't spawn competing builder containers.
func (w *watcher) rebuild(ctx context.Context) error {
	w.buildMu.Lock()
	defer w.buildMu.Unlock()

	slog.Info("Rebuilding linux binaries")
	start := time.Now()
	cmd := exec.CommandContext(ctx, "make", "build-linux")
	cmd.Dir = w.root
	cmd.Stdout = io.Discard
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("make build-linux: %w", err)
	}
	slog.Info("Rebuild done", "took", time.Since(start).Round(time.Millisecond))

	// Copy freshly built binaries into the bind-mount location the
	// containers observe.
	binDir := filepath.Join(w.dataDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return err
	}
	for _, name := range []string{"controller-manager", "worker"} {
		src := filepath.Join(w.root, "bin", "linux", name)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		dst := filepath.Join(binDir, name)
		if err := copyFile(src, dst, 0o755); err != nil {
			return fmt.Errorf("copying %s: %w", name, err)
		}
	}
	return nil
}

// triggerReloads calls each driver's Reload whose source prefix was touched.
func (w *watcher) triggerReloads(ctx context.Context, changed map[string]bool) {
	fired := make(map[string]bool)
	for rel := range changed {
		for prefix, fn := range w.reload {
			if fired[prefix] {
				continue
			}
			if strings.HasPrefix(rel, prefix) || prefix == "" {
				fired[prefix] = true
				if err := fn(ctx); err != nil {
					slog.Warn("Reload failed", "prefix", prefix, "err", err)
				} else {
					slog.Info("Signaled reload", "prefix", prefix)
				}
			}
		}
	}
}

// addTree walks root and registers every subdirectory with the watcher.
func addTree(fw *fsnotify.Watcher, root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return nil
		}
		name := filepath.Base(path)
		// Skip generated-noise directories.
		if name == ".git" || name == ".gocache" || name == ".gomodcache" || name == "bin" {
			return filepath.SkipDir
		}
		return fw.Add(path)
	})
}

// interesting filters out fsnotify events we don't care about (chmod-only
// updates, non-.go files, generated files).
func interesting(ev fsnotify.Event) bool {
	if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) == 0 {
		return false
	}
	name := filepath.Base(ev.Name)
	if strings.HasPrefix(name, ".") {
		return false
	}
	if strings.HasSuffix(name, "~") {
		return false
	}
	if strings.HasPrefix(name, "zz_generated.") {
		return false
	}
	ext := filepath.Ext(name)
	return ext == "" || ext == ".go" || ext == ".proto"
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	// Write to a temp file and rename so containers never observe a
	// partial file via the bind mount.
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}
