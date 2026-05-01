// Package drivers orchestrates controller-manager and worker containers for
// `clrk dev`. The abstraction is intentionally thin: drivers shell out to the
// local `docker` CLI rather than depending on the Docker SDK, so the caller's
// environment controls authentication, buildkit, and context selection.
package drivers

import "context"

// Driver is the common interface implemented by the controller-manager and
// worker drivers.
type Driver interface {
	// Start launches the container and blocks until it reports running.
	// The returned string is the final container name.
	Start(ctx context.Context, opts ...Option) (string, error)
	// Stop removes the running container. It is safe to call when the
	// container is already gone.
	Stop(ctx context.Context) error
	// Reload signals the running container to re-exec. Used by watch mode
	// when the bind-mounted binary has been rebuilt on the host.
	Reload(ctx context.Context) error
	// GetAddr returns an address callers can use to reach the container
	// from the host (for example "localhost:8443").
	GetAddr(ctx context.Context) (string, error)
}

// Options contains fields shared by all drivers.
type Options struct {
	Image       string
	WatchBinary string
	Args        []string
	Env         map[string]string
	Volumes     map[string]string
	Labels      map[string]string
	Ports       map[int]int
	ExtraHosts  map[string]string
}

// Option configures driver behavior.
type Option func(*Options)

// DefaultOptions returns an empty Options struct with maps pre-allocated.
func DefaultOptions() *Options {
	return &Options{
		Env:        map[string]string{},
		Volumes:    map[string]string{},
		Labels:     map[string]string{},
		Ports:      map[int]int{},
		ExtraHosts: map[string]string{},
	}
}

// WithImage overrides the container image reference.
func WithImage(image string) Option {
	return func(o *Options) { o.Image = image }
}

// WithWatchBinary bind-mounts the given host path as the container entrypoint
// binary. When set, the caller is responsible for keeping the host path up to
// date; Reload then re-execs the container so the new binary is picked up.
func WithWatchBinary(path string) Option {
	return func(o *Options) { o.WatchBinary = path }
}

// WithArgs appends container command arguments.
func WithArgs(args ...string) Option {
	return func(o *Options) { o.Args = append(o.Args, args...) }
}

// WithEnv sets environment variables. Overwrites existing keys.
func WithEnv(kv map[string]string) Option {
	return func(o *Options) {
		for k, v := range kv {
			o.Env[k] = v
		}
	}
}

// WithVolume adds a host:container bind-mount.
func WithVolume(host, container string) Option {
	return func(o *Options) { o.Volumes[host] = container }
}

// WithLabel adds a container label.
func WithLabel(key, value string) Option {
	return func(o *Options) { o.Labels[key] = value }
}

// WithPort adds a host:container port publish mapping.
func WithPort(host, container int) Option {
	return func(o *Options) { o.Ports[host] = container }
}

// WithExtraHost adds an /etc/hosts entry inside the container.
// The address may be a literal IP or the docker-special token
// `host-gateway` (resolves to the host bridge address).
func WithExtraHost(name, address string) Option {
	return func(o *Options) { o.ExtraHosts[name] = address }
}

// Apply runs the option funcs in order and returns the resulting Options.
func Apply(opts ...Option) *Options {
	o := DefaultOptions()
	for _, fn := range opts {
		fn(o)
	}
	return o
}
