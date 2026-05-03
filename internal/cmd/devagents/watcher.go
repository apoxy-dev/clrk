package devagents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// resourceConfig binds one CRD to its plural resource string. We can't
// use clrkv1alpha1.GetGroupVersionResource here because we don't have
// typed objects in hand — the watcher uses the dynamic client end-to-end.
type resourceConfig struct {
	kind Kind
	gvr  schema.GroupVersionResource
}

func resourceConfigs() []resourceConfig {
	gv := clrkv1alpha1.SchemeGroupVersion
	return []resourceConfig{
		{KindTaskAgent, gv.WithResource("taskagents")},
		{KindDaemonAgent, gv.WithResource("daemonagents")},
	}
}

// runWatchers starts one List+Watch loop per agent CRD and feeds
// events into onChange / onDelete. Returns when ctx cancels. Errors
// from the apiserver back off and retry — the dev cluster can hiccup
// during rebuilds and we don't want to tear down the whole TUI.
func runWatchers(
	ctx context.Context,
	kubeconfig string,
	onChange func(Kind, *unstructured.Unstructured),
	onDelete func(Kind, string, string),
) error {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("loading kubeconfig %s: %w", kubeconfig, err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	for _, rc := range resourceConfigs() {
		go watchOne(ctx, dyn, rc, onChange, onDelete)
	}
	<-ctx.Done()
	return nil
}

// watchOne performs the classic informer pattern by hand: List, then
// Watch from the list's resourceVersion. On any error or watch close
// it backs off and resyncs from a fresh List. We avoid client-go's
// SharedInformer to keep startup cheap and keep the dev binary off
// the heavy informer dep tree it doesn't otherwise need.
func watchOne(
	ctx context.Context,
	dyn dynamic.Interface,
	rc resourceConfig,
	onChange func(Kind, *unstructured.Unstructured),
	onDelete func(Kind, string, string),
) {
	backoff := time.Second
	for {
		if err := listAndWatch(ctx, dyn, rc, onChange, onDelete); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Debug("Agent watcher reconnecting", "kind", rc.kind, "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 10*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

func listAndWatch(
	ctx context.Context,
	dyn dynamic.Interface,
	rc resourceConfig,
	onChange func(Kind, *unstructured.Unstructured),
	onDelete func(Kind, string, string),
) error {
	list, err := dyn.Resource(rc.gvr).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		// Aggregated APIService may still be settling; treat the
		// transient "not found" as a benign retry signal.
		if apierrors.IsNotFound(err) || apierrors.IsServiceUnavailable(err) {
			return err
		}
		return fmt.Errorf("list %s: %w", rc.kind, err)
	}
	for i := range list.Items {
		item := list.Items[i]
		onChange(rc.kind, &item)
	}
	rv := list.GetResourceVersion()

	w, err := dyn.Resource(rc.gvr).Namespace("").Watch(ctx, metav1.ListOptions{
		ResourceVersion:     rv,
		AllowWatchBookmarks: true,
	})
	if err != nil {
		return fmt.Errorf("watch %s: %w", rc.kind, err)
	}
	defer w.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-w.ResultChan():
			if !ok {
				return errors.New("watch channel closed")
			}
			switch ev.Type {
			case watch.Added, watch.Modified:
				if u, ok := ev.Object.(*unstructured.Unstructured); ok {
					onChange(rc.kind, u)
				}
			case watch.Deleted:
				if u, ok := ev.Object.(*unstructured.Unstructured); ok {
					onDelete(rc.kind, u.GetNamespace(), u.GetName())
				}
			case watch.Error:
				// Re-list. apiserver sent us an error frame — most
				// commonly "too old resource version".
				return fmt.Errorf("watch %s error frame: %v", rc.kind, ev.Object)
			case watch.Bookmark:
				// nothing to do; resourceVersion bumps live in ev.Object's
				// metadata and we don't keep it across the loop.
			}
		}
	}
}
