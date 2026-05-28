// Package invocation backs the Invocation API kind with an
// embedded-ClickHouse storage. Three GVRs are served from one shared
// pool: top-level invocations.clrk.apoxy.dev/v1alpha1 (full CRUD) and
// the per-parent subresources taskagents/invocations,
// daemonagents/invocations (LIST + WATCH only). The whole Invocation
// object is stored verbatim as JSON in the `object` column; indexed
// columns (namespace, parent_kind, parent_name, id, created_at, phase)
// are MATERIALIZED projections derived from the JSON at write time,
// so Spec/Status additions on the Go type need zero schema migration.
package invocation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	registryrest "k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/utils/ptr"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// MaxListSize caps the number of rows any single List call materialises
// from the table, regardless of the client-supplied Limit. Protects
// the controller-manager from OOM when a billing/analytics client
// requests a cluster-wide list without paging through a table that has
// grown into the millions of rows.
const MaxListSize = 10000

// Doer is the subset of *chpool.Pool that Storage needs. Pulled out as
// an interface so unit tests can inject a fake (recording the issued
// ch.Query and populating Result columns from a canned dataset)
// without spinning up a real ClickHouse process.
type Doer interface {
	Do(ctx context.Context, q ch.Query) error
}

// Storage is the rest.Storage backing every Invocation GVR. One Storage
// per GVR; all share the same Doer (a chpool.Pool in production). The
// parentKind field is empty for the top-level GVR and pinned for the
// per-parent subresources; it gates which kind a LIST returns and
// rejects writes at the subresource URL.
type Storage struct {
	pool           Doer
	gr             schema.GroupResource
	singular       string
	parentKind     clrkv1alpha1.InvocationParentKind
	tableConvertor registryrest.TableConvertor
	seq            atomic.Uint64
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

// New constructs a Storage rooted at the given Doer. parentKind is
// empty for the top-level GVR and one of TaskAgent / DaemonAgent for
// the corresponding subresource. The returned Storage seeds its seq
// counter from time.Now().UnixNano() so seq is monotonic across
// controller-manager restarts.
func New(pool Doer, gr schema.GroupResource, singular string, parentKind clrkv1alpha1.InvocationParentKind) *Storage {
	s := &Storage{
		pool:           pool,
		gr:             gr,
		singular:       singular,
		parentKind:     parentKind,
		tableConvertor: registryrest.NewDefaultTableConvertor(gr),
	}
	s.seq.Store(uint64(time.Now().UnixNano()))
	return s
}

// NewProvider wraps New as the StorageProvider apoxy-cli's builder
// expects. The Doer, GroupResource, singular name, and parent kind
// are captured at registration; the *runtime.Scheme +
// RESTOptionsGetter arguments are unused (we don't store via the
// apiserver generic registry).
func NewProvider(pool Doer, gr schema.GroupResource, singular string, parentKind clrkv1alpha1.InvocationParentKind) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return New(pool, gr, singular, parentKind), nil
	}
}

func (s *Storage) New() runtime.Object     { return &clrkv1alpha1.Invocation{} }
func (s *Storage) NewList() runtime.Object { return &clrkv1alpha1.InvocationList{} }
func (s *Storage) Destroy()                {}
func (s *Storage) NamespaceScoped() bool   { return true }
func (s *Storage) GetSingularName() string { return s.singular }

// nextSeq returns the next monotonic stream sequence; used as both
// the ReplacingMergeTree dedup version and the apiserver
// resourceVersion. UnixNano on Storage construction provides a
// restart-safe lower bound; atomic Add keeps a single cm process
// monotonic across concurrent writers.
func (s *Storage) nextSeq() uint64 { return s.seq.Add(1) }

// Get returns the latest object version for (namespace, name).
// ReplacingMergeTree FINAL ensures we see the dedupe'd row.
func (s *Storage) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	ns, _ := request.NamespaceFrom(ctx)

	var objects proto.ColStr
	body := fmt.Sprintf(
		"SELECT object FROM %s.%s FINAL WHERE namespace = %s AND id = %s LIMIT 1",
		Database, Table, sqlString(ns), sqlString(name),
	)
	if err := s.pool.Do(ctx, ch.Query{
		Body:   body,
		Result: proto.Results{{Name: "object", Data: &objects}},
	}); err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	if objects.Rows() == 0 {
		return nil, apierrors.NewNotFound(s.gr, name)
	}
	var inv clrkv1alpha1.Invocation
	if err := json.Unmarshal([]byte(objects.Row(0)), &inv); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &inv, nil
}

// List returns the matching set, scoped by namespace (always) and by
// (parent_kind, parent_name) when serving a per-parent subresource.
// Cluster-wide listing (namespace == "") is supported for the
// top-level GVR so billing/analytics can stream all parents.
func (s *Storage) List(ctx context.Context, opts *internalversion.ListOptions) (runtime.Object, error) {
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

	body := fmt.Sprintf("SELECT object FROM %s.%s FINAL", Database, Table)
	if len(clauses) > 0 {
		body += " WHERE "
		for i, c := range clauses {
			if i > 0 {
				body += " AND "
			}
			body += c
		}
	}
	body += " ORDER BY created_at DESC, id ASC"
	limit := int64(MaxListSize)
	if opts != nil && opts.Limit > 0 && opts.Limit < limit {
		limit = opts.Limit
	}
	body += " LIMIT " + strconv.FormatInt(limit, 10)

	var objects proto.ColStr
	if err := s.pool.Do(ctx, ch.Query{
		Body:   body,
		Result: proto.Results{{Name: "object", Data: &objects}},
	}); err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}

	list := &clrkv1alpha1.InvocationList{
		Items: make([]clrkv1alpha1.Invocation, 0, objects.Rows()),
	}
	// ResourceVersion of a list is the high-watermark seq we've
	// observed so a client can resume a watch from here. The seq
	// counter advances on every write through this Storage instance.
	list.ResourceVersion = strconv.FormatUint(s.seq.Load(), 10)
	for i := 0; i < objects.Rows(); i++ {
		var inv clrkv1alpha1.Invocation
		if err := json.Unmarshal([]byte(objects.Row(i)), &inv); err != nil {
			return nil, fmt.Errorf("decode row %d: %w", i, err)
		}
		list.Items = append(list.Items, inv)
	}
	return list, nil
}

// Create inserts a brand-new Invocation. The metadata.name and
// metadata.uid are mirrored to a fresh UUID when name is empty;
// otherwise metadata.uid is set equal to metadata.name so ce-id,
// resource name, and UID all agree. A controller=true,
// blockOwnerDeletion=false owner reference is synthesised from
// spec.parentRef so Kubernetes GC nukes invocations when the parent is
// deleted. POST against a per-parent subresource (parentKind != "") is
// rejected with 405-equivalent — the subresource is read-only.
func (s *Storage) Create(ctx context.Context, obj runtime.Object, createValidation registryrest.ValidateObjectFunc, _ *metav1.CreateOptions) (runtime.Object, error) {
	if s.parentKind != "" {
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
	now := metav1.NewTime(time.Now().UTC())
	inv.CreationTimestamp = now

	inv.OwnerReferences = []metav1.OwnerReference{{
		APIVersion:         clrkv1alpha1.SchemeGroupVersion.String(),
		Kind:               string(inv.Spec.ParentRef.Kind),
		Name:               inv.Spec.ParentRef.Name,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(false),
		// UID intentionally empty: looking up the parent UID requires
		// a k8s client we don't plumb here. Cascading GC is degraded
		// until a follow-up adds the lookup; the parent_kind /
		// parent_name projection still drives all per-parent queries.
	}}

	if createValidation != nil {
		if err := createValidation(ctx, inv); err != nil {
			return nil, err
		}
	}

	if err := s.insert(ctx, inv); err != nil {
		return nil, err
	}
	return inv, nil
}

// Update writes a new version of an existing Invocation. The supplied
// UpdatedObjectInfo runs against the current row (after FINAL). When
// the client provides metadata.resourceVersion on the incoming object,
// it must match the current row's resourceVersion — this is the
// standard k8s optimistic-concurrency precondition, and without it
// concurrent Updates silently lose writes (later seq wins via
// ReplacingMergeTree dedup at merge time). A fresh stream_seq is
// allocated for the new row so the post-write read sees it. PUT against
// a not-yet-existing name with forceAllowCreate=true routes through
// Create so the metadata-synthesis path runs.
func (s *Storage) Update(
	ctx context.Context,
	name string,
	objInfo registryrest.UpdatedObjectInfo,
	createValidation registryrest.ValidateObjectFunc,
	updateValidation registryrest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	if s.parentKind != "" {
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

	if err := s.insert(ctx, inv); err != nil {
		return nil, false, err
	}
	return inv, false, nil
}

// Watch is stubbed to an immediately-closed watch until a JetStream
// (or ClickHouse-poll) tail lands. kubectl get --watch exits cleanly
// against this; informers should not depend on it yet.
func (s *Storage) Watch(context.Context, *internalversion.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (s *Storage) ConvertToTable(ctx context.Context, obj runtime.Object, opts runtime.Object) (*metav1.Table, error) {
	return s.tableConvertor.ConvertToTable(ctx, obj, opts)
}

// insert serialises inv to JSON, MUTATES inv.ResourceVersion to a fresh
// monotonic seq, and issues a single-row INSERT. Idempotency on
// stream_seq is enforced by ReplacingMergeTree at merge time. Callers
// (Create/Update) rely on the RV mutation to return the post-write
// view to the client.
func (s *Storage) insert(ctx context.Context, inv *clrkv1alpha1.Invocation) error {
	seq := s.nextSeq()
	inv.ResourceVersion = strconv.FormatUint(seq, 10)

	encoded, err := json.Marshal(inv)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	objects := new(proto.ColStr)
	seqs := new(proto.ColUInt64)
	objects.Append(string(encoded))
	seqs.Append(seq)

	body := fmt.Sprintf("INSERT INTO %s.%s (object, stream_seq) VALUES", Database, Table)
	if err := s.pool.Do(ctx, ch.Query{
		Body: body,
		Input: proto.Input{
			{Name: "object", Data: objects},
			{Name: "stream_seq", Data: seqs},
		},
	}); err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return nil
}

// EnsureTable runs the CREATE TABLE IF NOT EXISTS DDL. Caller should
// invoke once on startup against any Doer the Storage will use.
// ttlDays is the row retention in days.
func EnsureTable(ctx context.Context, pool Doer, ttlDays int) error {
	body := fmt.Sprintf(createTableTmpl, ttlDays)
	if err := pool.Do(ctx, ch.Query{Body: body}); err != nil {
		return fmt.Errorf("ensure invocations table: %w", err)
	}
	return nil
}
