// Package invocation backs the Invocation API kind with a ClickHouse
// read model materialized from a JetStream event stream.
//
// Write path (system-only): producers publish a complete Invocation
// snapshot per lifecycle transition to the JetStream INVOCATIONS stream
// (see internal/invevent); the in-process Consumer materializes those
// events into the append-only invocation_events table. The apiserver
// itself does not write on the hot path — top-level Create/Update is
// 405 by default, opened only behind a header-gated test door (see
// testWriteRequested) that publishes through the same JetStream path.
//
// Read path: Get/List reconstruct current state as the highest-seq
// event per invocation (argMax(object, stream_seq)). The list
// resourceVersion is the Consumer's committed high-water sequence, and
// Watch tails JetStream from rv+1 via an ephemeral ordered consumer, so
// a List-then-Watch handoff never crosses the materialization gap.
//
// Three GVRs share one Storage impl: the top-level
// invocations.clrk.apoxy.dev/v1alpha1 (writable via the test door) and
// the per-parent subresources taskagents/invocations,
// daemonagents/invocations (LIST + WATCH only; writes always 405).
package invocation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	registryrest "k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/utils/ptr"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/apiserver/chsql"
)

// Doer is the shared ClickHouse-pool seam (internal/apiserver/chsql),
// aliased here so Storage's Deps/field references stay local. Unit tests
// inject a fake recording the issued ch.Query and populating Result
// columns from a canned dataset.
type Doer = chsql.Doer

// Deps are the process-wide dependencies shared by all three Invocation
// GVRs — identical across the top-level resource and the two
// subresources; only gr/singular/parentKind differ per GVR.
type Deps struct {
	// Pool is the ClickHouse read pool (the shared LazyPool in prod).
	Pool Doer
	// HighWater is the Consumer's committed stream sequence, surfaced as
	// the list resourceVersion. Shared pointer; never nil.
	HighWater *atomic.Uint64
	// Bus lazily carries the JetStream handle (for Watch) and the
	// Publisher (for the header-gated test-write door) once the embedded
	// NATS server is ready. Shared across the three GVRs. While
	// unresolved, Watch degrades to an empty watch and writes stay 405.
	Bus *Bus
	// AllowTestWrites gates the header-driven Create/Update door. False
	// in production; true in dev/CI.
	AllowTestWrites bool
}

// Storage is the rest.Storage backing every Invocation GVR.
type Storage struct {
	deps           Deps
	gr             schema.GroupResource
	singular       string
	parentKind     clrkv1alpha1.InvocationParentKind
	tableConvertor registryrest.TableConvertor
}

var (
	_ registryrest.Storage              = (*Storage)(nil)
	_ registryrest.Scoper               = (*Storage)(nil)
	_ registryrest.Lister               = (*Storage)(nil)
	_ registryrest.Getter               = (*Storage)(nil)
	_ registryrest.Creater              = (*Storage)(nil)
	_ registryrest.Updater              = (*Storage)(nil)
	_ registryrest.Watcher              = (*Storage)(nil)
	_ registryrest.TableConvertor       = (*Storage)(nil)
	_ registryrest.SingularNameProvider = (*Storage)(nil)
)

// New constructs a Storage. parentKind is empty for the top-level GVR
// and one of TaskAgent / DaemonAgent for the corresponding subresource.
func New(deps Deps, gr schema.GroupResource, singular string, parentKind clrkv1alpha1.InvocationParentKind) *Storage {
	return &Storage{
		deps:           deps,
		gr:             gr,
		singular:       singular,
		parentKind:     parentKind,
		tableConvertor: registryrest.NewDefaultTableConvertor(gr),
	}
}

// NewProvider wraps New as the StorageProvider the apoxy-cli builder
// expects. The *runtime.Scheme + RESTOptionsGetter args are unused (we
// don't store via the apiserver generic registry).
func NewProvider(deps Deps, gr schema.GroupResource, singular string, parentKind clrkv1alpha1.InvocationParentKind) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return New(deps, gr, singular, parentKind), nil
	}
}

func (s *Storage) New() runtime.Object     { return &clrkv1alpha1.Invocation{} }
func (s *Storage) NewList() runtime.Object { return &clrkv1alpha1.InvocationList{} }
func (s *Storage) Destroy()                {}
func (s *Storage) NamespaceScoped() bool   { return true }
func (s *Storage) GetSingularName() string { return s.singular }

func (s *Storage) ConvertToTable(ctx context.Context, obj runtime.Object, opts runtime.Object) (*metav1.Table, error) {
	return s.tableConvertor.ConvertToTable(ctx, obj, opts)
}

// scopeClauses returns the namespace and the namespace/parent WHERE
// predicates for the current request. The parent_kind / parent_name
// filters are added only for the per-parent subresources, where the
// parent name arrives as RequestInfo.Name.
func (s *Storage) scopeClauses(ctx context.Context) (string, []string) {
	ns, _ := request.NamespaceFrom(ctx)
	var clauses []string
	if ns != "" {
		clauses = append(clauses, "namespace = "+sqlString(ns))
	}
	if s.parentKind != "" {
		clauses = append(clauses, "parent_kind = "+sqlString(string(s.parentKind)))
		if info, ok := request.RequestInfoFrom(ctx); ok && info.Name != "" {
			clauses = append(clauses, "parent_name = "+sqlString(info.Name))
		}
	}
	return ns, clauses
}

// Get returns the current state of one invocation: the highest-seq
// event row for (namespace, invocation_id). resourceVersion is stamped
// from that row's stream_seq.
func (s *Storage) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	ns, _ := request.NamespaceFrom(ctx)
	clauses := []string{
		"namespace = " + sqlString(ns),
		"invocation_id = " + sqlString(name),
	}
	if s.parentKind != "" {
		clauses = append(clauses, "parent_kind = "+sqlString(string(s.parentKind)))
	}
	body := fmt.Sprintf(
		"SELECT object, stream_seq FROM %s.%s WHERE %s ORDER BY stream_seq DESC LIMIT 1",
		Database, Table, strings.Join(clauses, " AND "),
	)
	var (
		objects proto.ColStr
		seqs    proto.ColUInt64
	)
	if err := s.deps.Pool.Do(ctx, ch.Query{
		Body: body,
		Result: proto.Results{
			{Name: "object", Data: &objects},
			{Name: "stream_seq", Data: &seqs},
		},
	}); err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	if objects.Rows() == 0 {
		return nil, apierrors.NewNotFound(s.gr, name)
	}
	inv, err := decodeInvocation(objects.Row(0), seqs.Row(0))
	if err != nil {
		return nil, err
	}
	return inv, nil
}

// List reconstructs current state per invocation, newest first, with
// cursor paging. The snapshot resourceVersion is the Consumer's
// committed high-water seq (or the RV pinned by a continue token); every
// page filters stream_seq <= RV so a multi-page list is consistent.
func (s *Storage) List(ctx context.Context, opts *internalversion.ListOptions) (runtime.Object, error) {
	_, clauses := s.scopeClauses(ctx)

	var (
		rv     uint64
		cursor *listCursor
	)
	if opts != nil && opts.Continue != "" {
		c, err := decodeCursor(opts.Continue)
		if err != nil {
			return nil, apierrors.NewBadRequest(err.Error())
		}
		cursor = &c
		rv = c.RV
	} else {
		rv = s.deps.HighWater.Load()
	}

	limit := int64(defaultListLimit)
	if opts != nil && opts.Limit > 0 {
		limit = opts.Limit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	clauses = append(clauses, "stream_seq <= "+strconv.FormatUint(rv, 10))
	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}

	// Page boundary: created_at is NOT invariant across an invocation's
	// events — ingress and the worker stamp independent creationTimestamps
	// for the same id — so the cursor predicate must run on the per-group
	// aggregate, not on raw event rows (a raw-row WHERE filter could keep
	// some of an invocation's events and drop others, duplicating or
	// skipping it across pages). We order by min(created_at) (the earliest
	// / birth timestamp; deterministic, unlike any()) with invocation_id
	// as a stable tiebreaker, and page on that same (min_created_at, id)
	// tuple via HAVING.
	having := ""
	if cursor != nil {
		having = fmt.Sprintf(
			" HAVING (min(created_at), invocation_id) < (fromUnixTimestamp64Milli(toInt64(%d)), %s)",
			cursor.TS, sqlString(cursor.ID),
		)
	}
	body := fmt.Sprintf(
		"SELECT argMax(object, stream_seq) AS obj, max(stream_seq) AS seq, "+
			"toUnixTimestamp64Milli(min(created_at)) AS cat, invocation_id AS iid "+
			"FROM %s.%s%s GROUP BY invocation_id%s ORDER BY cat DESC, iid DESC LIMIT %d",
		Database, Table, where, having, limit+1,
	)

	var (
		objects proto.ColStr
		seqs    proto.ColUInt64
		cats    proto.ColInt64
		iids    proto.ColStr
	)
	if err := s.deps.Pool.Do(ctx, ch.Query{
		Body: body,
		Result: proto.Results{
			{Name: "obj", Data: &objects},
			{Name: "seq", Data: &seqs},
			{Name: "cat", Data: &cats},
			{Name: "iid", Data: &iids},
		},
	}); err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}

	list := &clrkv1alpha1.InvocationList{}
	list.ResourceVersion = strconv.FormatUint(rv, 10)

	rows := objects.Rows()
	more := int64(rows) > limit
	n := rows
	if more {
		n = int(limit)
	}
	list.Items = make([]clrkv1alpha1.Invocation, 0, n)
	for i := 0; i < n; i++ {
		inv, err := decodeInvocation(objects.Row(i), seqs.Row(i))
		if err != nil {
			return nil, err
		}
		list.Items = append(list.Items, *inv)
	}
	if more {
		last := n - 1
		list.Continue = encodeCursor(listCursor{RV: rv, TS: cats.Row(last), ID: iids.Row(last)})
	}
	return list, nil
}

// Create is 405 on the subresources and 405 by default on the top-level
// resource; it is permitted only behind the header-gated test door
// (AllowTestWrites + the request header). A permitted write publishes a
// complete snapshot through the same JetStream path producers use, so
// it is materialized by the Consumer and observed by Watch — not a
// second write path. ResourceVersion on the returned object is the
// assigned stream sequence; a bounded best-effort wait gives
// read-after-write for tests.
func (s *Storage) Create(ctx context.Context, obj runtime.Object, createValidation registryrest.ValidateObjectFunc, _ *metav1.CreateOptions) (runtime.Object, error) {
	if !s.writeAllowed(ctx) {
		return nil, apierrors.NewMethodNotSupported(s.gr, "create")
	}
	inv, ok := obj.(*clrkv1alpha1.Invocation)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("expected *Invocation, got %T", obj))
	}
	if inv.Spec.ParentRef.Name == "" || inv.Spec.ParentRef.Kind == "" {
		return nil, apierrors.NewBadRequest("spec.parentRef.kind and spec.parentRef.name are required")
	}
	ns, _ := request.NamespaceFrom(ctx)
	if ns == "" {
		return nil, apierrors.NewBadRequest("namespace is required")
	}
	inv.Namespace = ns
	if inv.Name == "" {
		inv.Name = uuid.NewString()
	}
	inv.UID = types.UID(inv.Name)
	inv.CreationTimestamp = metav1.NewTime(time.Now().UTC())
	if inv.Status.Phase == "" {
		inv.Status.Phase = clrkv1alpha1.InvocationPhasePending
	}
	inv.OwnerReferences = []metav1.OwnerReference{{
		APIVersion:         clrkv1alpha1.SchemeGroupVersion.String(),
		Kind:               string(inv.Spec.ParentRef.Kind),
		Name:               inv.Spec.ParentRef.Name,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(false),
	}}

	if createValidation != nil {
		if err := createValidation(ctx, inv); err != nil {
			return nil, err
		}
	}

	seq, err := s.deps.Bus.Publisher().Publish(ctx, inv, true)
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("publish invocation: %w", err))
	}
	inv.ResourceVersion = strconv.FormatUint(seq, 10)
	s.waitMaterialized(ctx, ns, inv.Name)
	return inv, nil
}

// Update is 405 on the subresources and 405 by default on the top-level
// resource; permitted only behind the test door. A permitted update
// publishes a new (non-birth) snapshot carrying the changed phase. It
// honours the standard optimistic-concurrency precondition against the
// current high-seq event.
func (s *Storage) Update(
	ctx context.Context,
	name string,
	objInfo registryrest.UpdatedObjectInfo,
	createValidation registryrest.ValidateObjectFunc,
	updateValidation registryrest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	_ *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	if !s.writeAllowed(ctx) {
		return nil, false, apierrors.NewMethodNotSupported(s.gr, "update")
	}

	cur, err := s.Get(ctx, name, &metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if !forceAllowCreate {
			return nil, false, err
		}
		fresh := &clrkv1alpha1.Invocation{}
		fresh.Name = name
		newObj, err := objInfo.UpdatedObject(ctx, fresh)
		if err != nil {
			return nil, false, err
		}
		out, err := s.Create(ctx, newObj, createValidation, &metav1.CreateOptions{})
		if err != nil {
			return nil, false, err
		}
		return out, true, nil
	}
	if err != nil {
		return nil, false, err
	}

	newObj, err := objInfo.UpdatedObject(ctx, cur)
	if err != nil {
		return nil, false, err
	}
	inv, ok := newObj.(*clrkv1alpha1.Invocation)
	if !ok {
		return nil, false, apierrors.NewBadRequest(fmt.Sprintf("expected *Invocation, got %T", newObj))
	}

	curRV := cur.(*clrkv1alpha1.Invocation).ResourceVersion
	if inv.ResourceVersion != "" && inv.ResourceVersion != curRV {
		return nil, false, apierrors.NewConflict(s.gr, name, fmt.Errorf(
			"the object has been modified; please apply your changes to the latest version and try again (have %s, want %s)",
			inv.ResourceVersion, curRV,
		))
	}
	if updateValidation != nil {
		if err := updateValidation(ctx, inv, cur); err != nil {
			return nil, false, err
		}
	}

	seq, err := s.deps.Bus.Publisher().Publish(ctx, inv, false)
	if err != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("publish invocation: %w", err))
	}
	inv.ResourceVersion = strconv.FormatUint(seq, 10)
	return inv, false, nil
}

// writeAllowed reports whether a Create/Update is permitted: never on a
// subresource, and on the top-level resource only when the process flag
// is on, the per-request test-write header was stamped onto ctx, and a
// publisher is wired.
func (s *Storage) writeAllowed(ctx context.Context) bool {
	return s.parentKind == "" &&
		s.deps.AllowTestWrites &&
		s.deps.Bus != nil &&
		s.deps.Bus.Publisher() != nil &&
		testWriteRequested(ctx)
}

// waitMaterialized polls the read model until the just-published
// invocation is visible or a short deadline elapses. Best-effort: gives
// tests read-after-write without making the write path depend on
// materialization. Errors and timeouts are ignored — the row will
// appear once the Consumer catches up.
func (s *Storage) waitMaterialized(ctx context.Context, ns, name string) {
	deadline := time.Now().Add(2 * time.Second)
	mctx := request.WithNamespace(ctx, ns)
	for time.Now().Before(deadline) {
		if _, err := s.Get(mctx, name, &metav1.GetOptions{}); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// decodeInvocation unmarshals a stored snapshot and stamps its
// resourceVersion from the row's stream sequence.
func decodeInvocation(raw string, seq uint64) (*clrkv1alpha1.Invocation, error) {
	var inv clrkv1alpha1.Invocation
	if err := json.Unmarshal([]byte(raw), &inv); err != nil {
		return nil, fmt.Errorf("decode invocation: %w", err)
	}
	inv.ResourceVersion = strconv.FormatUint(seq, 10)
	return &inv, nil
}

// EnsureTable runs the CREATE TABLE IF NOT EXISTS DDL. Caller invokes it
// once on startup against the Doer the Storage will use. ttlDays is the
// row retention in days.
func EnsureTable(ctx context.Context, pool Doer, ttlDays int) error {
	body := fmt.Sprintf(createTableTmpl, ttlDays)
	if err := pool.Do(ctx, ch.Query{Body: body}); err != nil {
		return fmt.Errorf("ensure %s table: %w", Table, err)
	}
	return nil
}
