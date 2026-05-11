//go:build linux

package worker

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"sync"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"

	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/netstack"
	"github.com/apoxy-dev/clrk/internal/sandbox/metadata"
)

// revStackGrace caps how long an idle RevisionStack stays alive after
// its last sandbox detaches before the manager tears it down. Tuned
// to typical warm-pool refill latency so back-to-back dispatches
// reuse the stack instead of churning the IMDS listener / DNS cache /
// forwarder goroutines.
const revStackGrace = 30 * time.Second

// revStackEntry is one (TaskAgent, revision) shared netstack on this
// worker: a gVisor stack hosting N NICs (one per sandbox), a single
// IMDS HTTP listener, a per-revision IdentityDialer, and the slot
// table the dialer + IMDS handler both read.
type revStackEntry struct {
	key WarmKey

	stack    *netstack.RevisionStack
	metadata *metadata.Server

	// dialer is the shared *egress.IdentityDialer this stack runs
	// for every intercepted connection. Per-dispatch state lives in
	// the slot table below; the dialer reads it on every dial via
	// SlotLookup keyed by the connection's source IP.
	dialer *egress.IdentityDialer

	mu      sync.Mutex
	slots   map[netip.Addr]*sandboxSlot
	refs    int
	closing *time.Timer
}

// sandboxSlot is the per-attachment record both consumers (the
// IdentityDialer and the metadata IMDS handler) read at request
// time. Mutated as a sandbox progresses through Create → Set* →
// Dispatch → Delete.
type sandboxSlot struct {
	nicID tcpip.NICID
	mu    sync.RWMutex
	dial  egress.DialSlot
	entry *metadata.Entry
}

// RevisionStackHandle is the per-sandbox handle the SandboxManager
// retains after Attach. It bundles the RevisionStack reference, the
// NIC ID, and the sandbox source IP so subsequent setters and the
// final Release don't need to re-resolve any of it.
type RevisionStackHandle struct {
	mgr       *RevisionStackManager
	entry     *revStackEntry
	nicID     tcpip.NICID
	sandboxIP netip.Addr
}

// RevisionStackManager owns the per-(TaskAgent, revision) shared
// netstacks on this worker, keyed by WarmKey. RevisionStacks are
// created lazily on the first sandbox Attach for a key and torn
// down some time after the last sandbox Detaches — see revStackGrace.
type RevisionStackManager struct {
	baseDialer      netstack.Dialer
	workerResolvers []netip.AddrPort

	mu      sync.Mutex
	entries map[WarmKey]*revStackEntry
}

// NewRevisionStackManager constructs a manager. baseDialer is the
// underlying TCP/UDP dialer (typically the egress.Router) every
// per-revision IdentityDialer wraps. workerResolvers are the
// worker's own nameservers, used to rewrite sandbox :53 dials.
func NewRevisionStackManager(baseDialer netstack.Dialer, workerResolvers []netip.AddrPort) *RevisionStackManager {
	return &RevisionStackManager{
		baseDialer:      baseDialer,
		workerResolvers: workerResolvers,
		entries:         make(map[WarmKey]*revStackEntry),
	}
}

// Attach gets (or lazily creates) the RevisionStack for identity and
// adds one NIC backed by tapFD. The returned handle is the
// SandboxManager's reference for later slot mutations + Detach.
//
// Identity carries the revision-stable fields (Kind, Namespace,
// Name, UID, Revision) stamped into proxyproto TLVs by the shared
// IdentityDialer. The InvocationID field is ignored here — per-
// dispatch InvocationID is delivered via SetInvocation on the slot.
func (m *RevisionStackManager) Attach(
	ctx context.Context,
	identity proxyproto.AgentIdentity,
	tapFD *os.File,
	gw, sandboxIP netip.Addr,
) (*RevisionStackHandle, error) {
	key := WarmKey{Namespace: identity.Namespace, Agent: identity.Name, Revision: identity.Revision}

	m.mu.Lock()
	entry, ok := m.entries[key]
	if !ok {
		built, err := m.buildEntry(ctx, key, identity)
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		entry = built
		m.entries[key] = entry
	}
	// Cancel any pending grace-period close — a new attach revives
	// the stack.
	if entry.closing != nil {
		entry.closing.Stop()
		entry.closing = nil
	}
	m.mu.Unlock()

	nicID, err := entry.stack.Attach(tapFD, gw, sandboxIP)
	if err != nil {
		m.releaseIfUnused(key)
		return nil, fmt.Errorf("attach NIC: %w", err)
	}

	entry.mu.Lock()
	entry.slots[sandboxIP] = &sandboxSlot{nicID: nicID}
	entry.refs++
	entry.mu.Unlock()

	return &RevisionStackHandle{
		mgr:       m,
		entry:     entry,
		nicID:     nicID,
		sandboxIP: sandboxIP,
	}, nil
}

// buildEntry assembles a fresh revStackEntry: gVisor stack, dialer,
// IMDS server. Called under m.mu so concurrent Attaches for the
// same key dedupe to a single stack.
func (m *RevisionStackManager) buildEntry(ctx context.Context, key WarmKey, identity proxyproto.AgentIdentity) (*revStackEntry, error) {
	stk, err := netstack.NewRevisionStack()
	if err != nil {
		return nil, fmt.Errorf("creating revision netstack: %w", err)
	}

	entry := &revStackEntry{
		key:   key,
		stack: stk,
		slots: make(map[netip.Addr]*sandboxSlot),
	}

	// Strip InvocationID from the dialer's identity copy — it's
	// per-dispatch and resolved via SlotLookup on every dial.
	revIdentity := identity
	revIdentity.InvocationID = ""

	entry.dialer = &egress.IdentityDialer{
		Base:         m.baseDialer,
		Identity:     revIdentity,
		DNSResolvers: m.workerResolvers,
		DNSCache:     stk.DNSCache(),
		SlotLookup:   entry.lookupDial,
	}

	stk.Start(context.Background(), entry.dialer)

	srv, err := metadata.New(stk.Stack(), entry.lookupEntry)
	if err != nil {
		stk.Close()
		return nil, fmt.Errorf("starting metadata server: %w", err)
	}
	entry.metadata = srv

	return entry, nil
}

// Detach removes the handle's NIC from its RevisionStack and clears
// the slot. When the stack's refcount hits zero it stays alive for
// revStackGrace so the next dispatch (warm-pool refill, follow-up
// request) doesn't have to rebuild the IMDS listener / DNS cache /
// forwarders. Idempotent — repeat calls are safe.
func (h *RevisionStackHandle) Detach() {
	if h == nil || h.entry == nil {
		return
	}
	entry := h.entry

	entry.mu.Lock()
	delete(entry.slots, h.sandboxIP)
	if entry.refs > 0 {
		entry.refs--
	}
	idle := entry.refs == 0
	entry.mu.Unlock()

	_ = entry.stack.Detach(h.nicID)

	if idle {
		h.mgr.scheduleClose(entry.key)
	}

	h.entry = nil
}

// SandboxIP returns the per-NIC source IP the dispatcher should
// write into the slot table on Set* calls.
func (h *RevisionStackHandle) SandboxIP() netip.Addr { return h.sandboxIP }

// SetEgressBackends writes the per-sandbox backend list into the
// slot. Concurrent dispatches against the same revision keep their
// own backend snapshots — re-resolved per dispatch at the dispatcher.
func (h *RevisionStackHandle) SetEgressBackends(backends []egress.BackendListener) {
	if h == nil || h.entry == nil {
		return
	}
	slot := h.entry.getSlot(h.sandboxIP)
	if slot == nil {
		return
	}
	slot.mu.Lock()
	slot.dial.Backends = backends
	slot.mu.Unlock()
}

// SetEgressPolicy writes the per-sandbox SandboxPolicy handle into
// the slot.
func (h *RevisionStackHandle) SetEgressPolicy(policy *egress.SandboxPolicy) {
	if h == nil || h.entry == nil {
		return
	}
	slot := h.entry.getSlot(h.sandboxIP)
	if slot == nil {
		return
	}
	slot.mu.Lock()
	slot.dial.Policy = policy
	slot.mu.Unlock()
}

// SetInvocationID stamps the per-dispatch InvocationID into the
// slot. Called once per dispatch — cold and warm path alike — so
// warm reruns get a fresh ID for proxyproto TLVs (fixes the latent
// staleness bug where the old per-sandbox dialer captured the
// first dispatch's InvocationID for the sandbox's lifetime).
func (h *RevisionStackHandle) SetInvocationID(invocationID string) {
	if h == nil || h.entry == nil {
		return
	}
	slot := h.entry.getSlot(h.sandboxIP)
	if slot == nil {
		return
	}
	slot.mu.Lock()
	slot.dial.InvocationID = invocationID
	slot.mu.Unlock()
}

// RegisterMetadataEntry attaches a per-dispatch *metadata.Entry to
// the slot so the IMDS handler answers /v1/event for this sandbox.
// The returned closer clears the slot's Entry when the dispatch
// teardown runs — leaving the slot itself intact (the sandbox may
// be reused on a warm-pool path).
func (h *RevisionStackHandle) RegisterMetadataEntry(entry *metadata.Entry) func() {
	if h == nil || h.entry == nil {
		return func() {}
	}
	slot := h.entry.getSlot(h.sandboxIP)
	if slot == nil {
		return func() {}
	}
	slot.mu.Lock()
	slot.entry = entry
	slot.mu.Unlock()
	return func() {
		slot.mu.Lock()
		if slot.entry == entry {
			slot.entry = nil
		}
		slot.mu.Unlock()
	}
}

// lookupDial implements egress.SlotLookupFunc — resolves the dial-
// time per-sandbox config from the connection's source IP.
func (e *revStackEntry) lookupDial(srcIP netip.Addr) (egress.DialSlot, bool) {
	e.mu.Lock()
	slot := e.slots[srcIP]
	e.mu.Unlock()
	if slot == nil {
		return egress.DialSlot{}, false
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	return slot.dial, true
}

// lookupEntry implements metadata.EntryLookup — resolves the IMDS
// *Entry for an inbound /v1/event or /v1/response request by
// connection source IP.
func (e *revStackEntry) lookupEntry(srcIP netip.Addr) *metadata.Entry {
	e.mu.Lock()
	slot := e.slots[srcIP]
	e.mu.Unlock()
	if slot == nil {
		return nil
	}
	slot.mu.RLock()
	defer slot.mu.RUnlock()
	return slot.entry
}

// getSlot returns the per-sandbox slot pointer or nil when no
// sandbox is attached. Callers hold the returned slot's own mutex
// for state mutations; the entry-level mu only guards the map.
func (e *revStackEntry) getSlot(srcIP netip.Addr) *sandboxSlot {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.slots[srcIP]
}

// scheduleClose arms the grace-period close timer. A subsequent
// Attach to the same key cancels it; if the timer fires while the
// stack is still idle, the manager tears the entry down.
func (m *RevisionStackManager) scheduleClose(key WarmKey) {
	m.mu.Lock()
	entry, ok := m.entries[key]
	if !ok {
		m.mu.Unlock()
		return
	}
	if entry.closing != nil {
		entry.closing.Stop()
	}
	entry.closing = time.AfterFunc(revStackGrace, func() {
		m.closeIfIdle(key)
	})
	m.mu.Unlock()
}

// closeIfIdle tears down the entry for key if its refcount is still
// zero. Triggered by the grace-period timer.
func (m *RevisionStackManager) closeIfIdle(key WarmKey) {
	m.mu.Lock()
	entry, ok := m.entries[key]
	if !ok {
		m.mu.Unlock()
		return
	}
	entry.mu.Lock()
	stillIdle := entry.refs == 0
	entry.mu.Unlock()
	if !stillIdle {
		m.mu.Unlock()
		return
	}
	delete(m.entries, key)
	m.mu.Unlock()

	if entry.metadata != nil {
		_ = entry.metadata.Close()
	}
	_ = entry.stack.Close()
}

// releaseIfUnused tears the entry down synchronously when a fresh
// build was wasted (Attach failed after buildEntry inserted it). No
// grace period — there's nothing to keep alive.
func (m *RevisionStackManager) releaseIfUnused(key WarmKey) {
	m.mu.Lock()
	entry, ok := m.entries[key]
	if !ok {
		m.mu.Unlock()
		return
	}
	entry.mu.Lock()
	idle := entry.refs == 0 && len(entry.slots) == 0
	entry.mu.Unlock()
	if !idle {
		m.mu.Unlock()
		return
	}
	delete(m.entries, key)
	m.mu.Unlock()

	if entry.metadata != nil {
		_ = entry.metadata.Close()
	}
	_ = entry.stack.Close()
}

// Shutdown closes every entry immediately. Intended for worker
// shutdown — the dispatcher has already drained, so no new Attaches
// race against it.
func (m *RevisionStackManager) Shutdown() {
	m.mu.Lock()
	entries := make([]*revStackEntry, 0, len(m.entries))
	for _, e := range m.entries {
		if e.closing != nil {
			e.closing.Stop()
			e.closing = nil
		}
		entries = append(entries, e)
	}
	m.entries = make(map[WarmKey]*revStackEntry)
	m.mu.Unlock()

	for _, e := range entries {
		if e.metadata != nil {
			_ = e.metadata.Close()
		}
		_ = e.stack.Close()
	}
}

// ensureContext keeps godoc happy that we use ctx (unused field
// today — passed in case future Attach paths need an early cancel).
var _ = func(_ context.Context) {}
