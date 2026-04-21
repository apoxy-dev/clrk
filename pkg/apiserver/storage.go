package apiserver

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"

	driversgeneric "github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/endpoint"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/util/flowcontrol/request"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	serverbuilder "github.com/apoxy-dev/apoxy/pkg/apiserver/server/builder"
)

// sqliteAutoVacuumIncremental is PRAGMA auto_vacuum mode 2
// (https://www.sqlite.org/pragma.html#pragma_auto_vacuum).
const sqliteAutoVacuumIncremental = 2

func encodeSQLiteConnArgs(args map[string]string) string {
	var buf strings.Builder
	for k, v := range args {
		if buf.Len() > 0 {
			buf.WriteString("&")
		}
		buf.WriteString(k)
		buf.WriteString("=")
		buf.WriteString(v)
	}
	return buf.String()
}

// enableAutoVacuum sets auto_vacuum=INCREMENTAL on the database if not
// already set, running a one-time VACUUM to apply the change.
func enableAutoVacuum(path string) error {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return fmt.Errorf("opening database for auto_vacuum: %w", err)
	}
	defer db.Close()

	var mode int
	if err := db.QueryRow("PRAGMA auto_vacuum").Scan(&mode); err != nil {
		return fmt.Errorf("querying auto_vacuum mode: %w", err)
	}
	if mode == sqliteAutoVacuumIncremental {
		return nil
	}
	slog.Info("Setting SQLite auto_vacuum to incremental", "current_mode", mode)
	if _, err := db.Exec("PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
		return fmt.Errorf("setting auto_vacuum: %w", err)
	}
	if _, err := db.Exec("VACUUM"); err != nil {
		return fmt.Errorf("vacuuming database: %w", err)
	}
	return nil
}

// startIncrementalVacuum runs PRAGMA incremental_vacuum on a timer to reclaim
// freelist pages. Stops when ctx is canceled.
func startIncrementalVacuum(ctx context.Context, path string, interval time.Duration) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		slog.Error("Failed to open database for incremental vacuum", "error", err)
		return
	}
	go func() {
		defer db.Close()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var freePages int
				if err := db.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&freePages); err != nil {
					if ctx.Err() != nil {
						return
					}
					slog.Error("Failed to query freelist_count", "error", err)
					continue
				}
				if freePages == 0 {
					continue
				}
				if _, err := db.ExecContext(ctx, "PRAGMA incremental_vacuum"); err != nil {
					if ctx.Err() != nil {
						return
					}
					slog.Error("Incremental vacuum failed", "error", err)
				}
			}
		}
	}()
}

// newKineStorage configures a kine-backed storage StoreFn suitable for passing
// to builder.WithResourceAndStorage. dbPath is the SQLite file path (or
// "file::memory:" for in-memory). connArgs are SQLite DSN parameters.
func newKineStorage(ctx context.Context, dbPath string, connArgs map[string]string, logFormat string) (serverbuilder.StoreFn, error) {
	if !strings.Contains(dbPath, ":memory:") {
		if err := enableAutoVacuum(dbPath); err != nil {
			return nil, fmt.Errorf("enabling auto_vacuum: %w", err)
		}
	}

	dsn := "sqlite://" + dbPath
	if args := encodeSQLiteConnArgs(connArgs); args != "" {
		dsn += "?" + args
	}

	tmpDir := os.Getenv("KINE_TMPDIR")
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}
	listenerDir, err := os.MkdirTemp(tmpDir, "clrk-kine-*")
	if err != nil {
		return nil, fmt.Errorf("creating kine listener dir: %w", err)
	}
	go func() {
		<-ctx.Done()
		_ = os.RemoveAll(listenerDir)
	}()

	metricsRegisterer := prometheus.WrapRegistererWith(
		prometheus.Labels{"kine_listener": filepath.Base(listenerDir)},
		metrics.Registry,
	)
	etcdConfig, err := endpoint.Listen(ctx, endpoint.Config{
		Endpoint: dsn,
		Listener: "unix://" + filepath.Join(listenerDir, "kine.sock"),
		ConnectionPoolConfig: driversgeneric.ConnectionPoolConfig{
			MaxOpen: goruntime.NumCPU(),
		},
		MetricsRegisterer:   metricsRegisterer,
		NotifyInterval:      5 * time.Second,
		EmulatedETCDVersion: "3.5.13",
		CompactInterval:     5 * time.Minute,
		CompactTimeout:      5 * time.Second,
		CompactMinRetain:    1000,
		CompactBatchSize:    1000,
		PollBatchSize:       500,
		LogFormat:           logFormat,
	})
	if err != nil {
		return nil, err
	}

	if !strings.Contains(dbPath, ":memory:") {
		startIncrementalVacuum(ctx, dbPath, 5*time.Minute)
	}

	return func(scheme *runtime.Scheme, s *genericregistry.Store, options *generic.StoreOptions) {
		options.RESTOptions = &kineRESTOptionsGetter{
			scheme:         scheme,
			etcdConfig:     etcdConfig,
			groupVersioner: s.StorageVersioner,
		}
	}, nil
}

type kineRESTOptionsGetter struct {
	scheme         *runtime.Scheme
	etcdConfig     endpoint.ETCDConfig
	groupVersioner runtime.GroupVersioner
}

// GetRESTOptions implements generic.RESTOptionsGetter.
func (g *kineRESTOptionsGetter) GetRESTOptions(resource schema.GroupResource, _ runtime.Object) (generic.RESTOptions, error) {
	s := json.NewSerializer(json.DefaultMetaFactory, g.scheme, g.scheme, false)
	codec := serializer.NewCodecFactory(g.scheme).
		CodecForVersions(s, s, g.groupVersioner, g.groupVersioner)
	return generic.RESTOptions{
		ResourcePrefix:            resource.String(),
		Decorator:                 genericregistry.StorageWithCacher(),
		EnableGarbageCollection:   true,
		DeleteCollectionWorkers:   1,
		CountMetricPollPeriod:     time.Minute,
		StorageObjectCountTracker: request.NewStorageObjectCountTracker(),
		StorageConfig: &storagebackend.ConfigForResource{
			GroupResource: resource,
			Config: storagebackend.Config{
				Prefix:              "/kine/",
				Codec:               codec,
				EventsHistoryWindow: storagebackend.DefaultEventsHistoryWindow,
				Transport: storagebackend.TransportConfig{
					ServerList:    g.etcdConfig.Endpoints,
					TrustedCAFile: g.etcdConfig.TLSConfig.CAFile,
					CertFile:      g.etcdConfig.TLSConfig.CertFile,
					KeyFile:       g.etcdConfig.TLSConfig.KeyFile,
				},
			},
		},
	}, nil
}
