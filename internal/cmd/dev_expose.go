package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/cmd/devtui"
)

// exposePortRangeLo and exposePortRangeHi bound the host-port window the
// `clrk dev` auto-forwarder allocates from. 18080–18099 is picked so the
// first Service to be exposed lands on :18080 — the historical port used
// by _examples/jq-bot/README.md back when users ran `kubectl port-forward`
// by hand.
const (
	exposePortRangeLo = 18080
	exposePortRangeHi = 18099

	exposeRetryBackoff = 2 * time.Second
)

var exposeLoopbackAddresses = supportedLoopbackAddresses()

func supportedLoopbackAddresses() []string {
	addresses := []string{"127.0.0.1"}
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		return addresses
	}
	_ = listener.Close()
	return append(addresses, "::1")
}

// exposedForward is the per-Service state owned by the reconciler. The
// goroutine reading svcCh applies updates and exits on ctx cancel.
type exposedForward struct {
	state  ExposedForward
	cancel context.CancelFunc
	done   chan struct{}
	// svcCh delivers the latest Service spec to the goroutine. The
	// reconciler's update handler sends on it (non-blocking — coalesced
	// by the goroutine).
	svcCh chan *corev1.Service
}

// exposeReconciler watches Services labeled clrk.apoxy.dev/expose=true
// and runs a client-go SPDY port-forward for each, picking a host port
// from a documented range. The reconciler is the source of truth at
// runtime; it mirrors its mapping into dev.json + the TUI on every
// change so external introspection works without contacting the cluster.
type exposeReconciler struct {
	restConfig *rest.Config
	kc         kubernetes.Interface
	dataDir    string
	prog       *devtui.Program

	mu       sync.Mutex
	forwards map[string]*exposedForward // key = "<ns>/<name>"
}

// startExposeReconciler builds the reconciler from a host-side kubeconfig
// and runs it in the current goroutine. Constructed errors are logged
// (not propagated) — the reconciler is a best-effort dev convenience,
// not a piece of bringUp that should fail the session.
func startExposeReconciler(ctx context.Context, kubeconfig, dataDir string, prog *devtui.Program) {
	r, err := newExposeReconciler(kubeconfig, dataDir, prog)
	if err != nil {
		slog.Warn("Expose reconciler disabled", "err", err)
		return
	}
	if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("Expose reconciler exited", "err", err)
	}
}

// newExposeReconciler builds the reconciler from a host-side kubeconfig
// path. Returns an error only when the kubeconfig can't be parsed or
// the clientset can't be built — same conditions that block every other
// dev subcommand, so failure here is fatal to the dev session.
func newExposeReconciler(kubeconfig, dataDir string, prog *devtui.Program) (*exposeReconciler, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig %s: %w", kubeconfig, err)
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("clientset: %w", err)
	}
	return &exposeReconciler{
		restConfig: cfg,
		kc:         kc,
		dataDir:    dataDir,
		prog:       prog,
		forwards:   map[string]*exposedForward{},
	}, nil
}

// Run drives the Service informer + per-Service goroutines until ctx is
// canceled. It returns the informer error (if any) once cleanup
// completes, so callers can treat Run as a fail-fast goroutine.
func (r *exposeReconciler) Run(ctx context.Context) error {
	selector := labels.SelectorFromSet(labels.Set{clrkv1alpha1.LabelExpose: "true"}).String()
	factory := informers.NewSharedInformerFactoryWithOptions(r.kc, 0,
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector
		}),
	)
	inf := factory.Core().V1().Services().Informer()
	if _, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.upsert(ctx, asService(obj)) },
		UpdateFunc: func(_, obj any) { r.upsert(ctx, asService(obj)) },
		DeleteFunc: func(obj any) { r.remove(deletionKey(obj)) },
	}); err != nil {
		return fmt.Errorf("registering Service event handler: %w", err)
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		return errors.New("Service informer never synced")
	}
	slog.Info("Expose reconciler started", "range", fmt.Sprintf("%d-%d", exposePortRangeLo, exposePortRangeHi))
	<-ctx.Done()
	r.shutdown()
	return nil
}

func asService(obj any) *corev1.Service {
	if svc, ok := obj.(*corev1.Service); ok {
		return svc
	}
	return nil
}

func deletionKey(obj any) string {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	if svc, ok := obj.(*corev1.Service); ok {
		return svc.Namespace + "/" + svc.Name
	}
	return ""
}

// upsert creates a new per-Service goroutine if one doesn't exist, or
// forwards the latest spec to the existing one. Coalescing happens
// inside the goroutine.
func (r *exposeReconciler) upsert(ctx context.Context, svc *corev1.Service) {
	if svc == nil || svc.Spec.ClusterIP == corev1.ClusterIPNone {
		return
	}
	if len(svc.Spec.Ports) == 0 || len(svc.Spec.Selector) == 0 {
		// Headless or selector-less Services aren't supported — the
		// forward needs a Pod target. Log once and skip.
		slog.Warn("Skipping unsupported Service for expose",
			"service", svc.Namespace+"/"+svc.Name,
			"reason", "no ports or no selector")
		return
	}
	key := svc.Namespace + "/" + svc.Name

	r.mu.Lock()
	f, ok := r.forwards[key]
	if !ok {
		gctx, cancel := context.WithCancel(ctx)
		f = &exposedForward{
			cancel: cancel,
			done:   make(chan struct{}),
			svcCh:  make(chan *corev1.Service, 1),
		}
		r.forwards[key] = f
		r.mu.Unlock()
		go r.runForward(gctx, key, svc, f)
		// Send the initial spec; the goroutine consumes it on entry.
		select {
		case f.svcCh <- svc:
		default:
		}
		return
	}
	r.mu.Unlock()
	// Existing goroutine: deliver the latest spec (drop any unread one).
	select {
	case <-f.svcCh:
	default:
	}
	select {
	case f.svcCh <- svc:
	default:
	}
}

func (r *exposeReconciler) remove(key string) {
	if key == "" {
		return
	}
	r.mu.Lock()
	f, ok := r.forwards[key]
	if ok {
		delete(r.forwards, key)
	}
	r.mu.Unlock()
	if !ok {
		return
	}
	f.cancel()
	<-f.done
	r.notify()
}

func (r *exposeReconciler) shutdown() {
	r.mu.Lock()
	fs := make([]*exposedForward, 0, len(r.forwards))
	for k, f := range r.forwards {
		fs = append(fs, f)
		delete(r.forwards, k)
	}
	r.mu.Unlock()
	deadline := time.After(5 * time.Second)
	for _, f := range fs {
		f.cancel()
		select {
		case <-f.done:
		case <-deadline:
		}
	}
	r.notify()
}

// runForward is the per-Service goroutine. It loops: pick a Ready pod,
// allocate the host port, run the SPDY forward, retry on error until
// the goroutine's context is canceled. Host port stays sticky across
// retries — the allocator only reassigns when the port-hint label
// changes or the original allocation collides at startup.
func (r *exposeReconciler) runForward(ctx context.Context, key string, initial *corev1.Service, f *exposedForward) {
	defer close(f.done)
	svc := initial
	for {
		// Drain pending svc updates (the latest spec wins; we don't
		// queue stale ones).
		select {
		case s := <-f.svcCh:
			if s != nil {
				svc = s
			}
		default:
		}
		if ctx.Err() != nil {
			return
		}

		pod, podPort, svcPort, err := r.resolveTarget(ctx, svc)
		if err != nil {
			slog.Debug("Expose: target resolve failed; retrying",
				"service", key, "err", err)
			if !sleep(ctx, exposeRetryBackoff) {
				return
			}
			continue
		}

		hostPort, err := r.allocatePort(key, svc, f.state.HostPort)
		if err != nil {
			slog.Warn("Expose: host port allocation failed",
				"service", key, "err", err)
			if !sleep(ctx, exposeRetryBackoff) {
				return
			}
			continue
		}

		r.mu.Lock()
		f.state = ExposedForward{
			Namespace:   svc.Namespace,
			Name:        svc.Name,
			HostPort:    hostPort,
			ServicePort: svcPort,
		}
		r.mu.Unlock()
		r.notify()

		slog.Info("Forwarding host port to Service",
			"service", key,
			"host_port", hostPort,
			"pod", pod,
			"pod_port", podPort)
		err = r.doForward(ctx, svc.Namespace, pod, hostPort, podPort)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Debug("Expose: forward exited; reconnecting",
				"service", key, "err", err)
		}
		if !sleep(ctx, exposeRetryBackoff) {
			return
		}
	}
}

// resolveTarget picks a Ready pod backing the Service and returns the
// pod name, the pod's container port (target of the forward), and the
// Service's listening port (the rendered URL in `clrk dev status`). It
// uses the first TCP port — SPDY port-forward only carries TCP, so a
// UDP-first Service (e.g. kube-dns) would otherwise wedge the dial.
func (r *exposeReconciler) resolveTarget(ctx context.Context, svc *corev1.Service) (pod string, podPort int32, svcPort int32, err error) {
	p, ok := firstTCPPort(svc.Spec.Ports)
	if !ok {
		return "", 0, 0, errors.New("service has no TCP port")
	}
	svcPort = p.Port

	selector := labels.SelectorFromSet(svc.Spec.Selector).String()
	pods, err := r.kc.CoreV1().Pods(svc.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", 0, 0, fmt.Errorf("listing pods: %w", err)
	}
	for i := range pods.Items {
		pp := &pods.Items[i]
		if !isPodReady(pp) {
			continue
		}
		port, ok := containerPortForService(pp, p)
		if !ok {
			continue
		}
		return pp.Name, port, svcPort, nil
	}
	return "", 0, 0, errors.New("no Ready pod backs the Service")
}

// firstTCPPort returns the first port whose Protocol is empty (default
// TCP per the Service API) or explicitly TCP.
func firstTCPPort(ps []corev1.ServicePort) (corev1.ServicePort, bool) {
	for _, p := range ps {
		if p.Protocol == "" || p.Protocol == corev1.ProtocolTCP {
			return p, true
		}
	}
	return corev1.ServicePort{}, false
}

func isPodReady(p *corev1.Pod) bool {
	if p.DeletionTimestamp != nil {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// containerPortForService resolves the Service port's TargetPort against
// the pod's container ports. Returns the numeric container port. A
// numeric Service TargetPort short-circuits the lookup.
func containerPortForService(p *corev1.Pod, sp corev1.ServicePort) (int32, bool) {
	switch sp.TargetPort.Type {
	case 0: // intstr.Int
		if sp.TargetPort.IntVal > 0 {
			return sp.TargetPort.IntVal, true
		}
		// Implicit: TargetPort defaults to Port.
		return sp.Port, true
	case 1: // intstr.String — named container port
		want := sp.TargetPort.StrVal
		for _, c := range p.Spec.Containers {
			for _, cp := range c.Ports {
				if cp.Name == want {
					return cp.ContainerPort, true
				}
			}
		}
	}
	return 0, false
}

// allocatePort picks a host port for key. Preference order:
//  1. `clrk.apoxy.dev/expose-port` label, if free.
//  2. The sticky port the goroutine has been using, if still free.
//  3. The first free port in [exposePortRangeLo, exposePortRangeHi].
//
// "Free" is probed on each supported loopback address with net.Listen.
// The binds are released immediately. A brief TOCTOU is acceptable because
// portforward.NewOnAddresses returns a clear error on retry.
func (r *exposeReconciler) allocatePort(key string, svc *corev1.Service, sticky int) (int, error) {
	if hint := svc.Labels[clrkv1alpha1.LabelExposePort]; hint != "" {
		if p, err := strconv.Atoi(hint); err == nil && p > 0 && p < 65536 {
			if !r.portInUseByOther(key, p) && portFree(p) {
				return p, nil
			}
			slog.Warn("Expose: port-hint unavailable; falling back to range",
				"service", key, "hint", p)
		}
	}
	if sticky != 0 && !r.portInUseByOther(key, sticky) && portFree(sticky) {
		return sticky, nil
	}
	for p := exposePortRangeLo; p <= exposePortRangeHi; p++ {
		if r.portInUseByOther(key, p) {
			continue
		}
		if portFree(p) {
			return p, nil
		}
	}
	return 0, fmt.Errorf("no free port in %d-%d", exposePortRangeLo, exposePortRangeHi)
}

func (r *exposeReconciler) portInUseByOther(key string, port int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, f := range r.forwards {
		if k == key {
			continue
		}
		if f.state.HostPort == port {
			return true
		}
	}
	return false
}

func portFree(p int) bool {
	for _, address := range exposeLoopbackAddresses {
		l, err := net.Listen("tcp", net.JoinHostPort(address, strconv.Itoa(p)))
		if err != nil {
			return false
		}
		_ = l.Close()
	}
	return true
}

// doForward opens a SPDY port-forward from each supported loopback address
// at hostPort to <pod>:<podPort>. It blocks until the stream closes or ctx
// is canceled.
func (r *exposeReconciler) doForward(ctx context.Context, ns, pod string, hostPort int, podPort int32) error {
	transport, upgrader, err := spdy.RoundTripperFor(r.restConfig)
	if err != nil {
		return fmt.Errorf("spdy round tripper: %w", err)
	}
	host, err := url.Parse(r.restConfig.Host)
	if err != nil {
		return fmt.Errorf("parsing apiserver host: %w", err)
	}
	pfURL := host.ResolveReference(&url.URL{
		Path: fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", ns, pod),
	})
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", pfURL)

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stopCh)
	}()
	pf, err := portforward.NewOnAddresses(dialer,
		exposeLoopbackAddresses,
		[]string{fmt.Sprintf("%d:%d", hostPort, podPort)},
		stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return fmt.Errorf("portforward.New: %w", err)
	}
	return pf.ForwardPorts()
}

// notify mirrors the current forward set into dev.json and the TUI.
// Best-effort: both targets log on failure but never block the
// reconciler loop.
func (r *exposeReconciler) notify() {
	snap := r.snapshot()
	if err := rewriteForwards(r.dataDir, snap); err != nil {
		slog.Warn("Failed to mirror forwards into dev.json", "err", err)
	}
	if r.prog != nil {
		m := make(map[string]int, len(snap))
		for _, f := range snap {
			m[f.Namespace+"/"+f.Name] = f.HostPort
		}
		r.prog.SendExposedForwards(m)
	}
}

func (r *exposeReconciler) snapshot() []ExposedForward {
	r.mu.Lock()
	out := make([]ExposedForward, 0, len(r.forwards))
	for _, f := range r.forwards {
		if f.state.HostPort == 0 {
			continue
		}
		out = append(out, f.state)
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// sleep blocks for d unless ctx is canceled first. Returns false on
// cancellation so callers can break out of their retry loop.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
