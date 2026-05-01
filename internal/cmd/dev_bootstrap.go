package cmd

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

// bootstrapDefaultWorkerPool ensures a `default` WorkerPool exists in the
// dev cluster so DaemonAgent/TaskAgent revisions referencing
// workerPoolRef: default reconcile cleanly without examples having to ship
// a 14-line workerpool.yaml. clrk dev's worker container defaults
// CLRK_POOL_NAME=default, so the worker self-binds to this pool.
//
// Idempotent: server-side applied with field manager `clrk-dev`. Operators
// who want different replicas / podTemplate apply their own WorkerPool
// (any name) and reference it explicitly from their agents.
func bootstrapDefaultWorkerPool(ctx context.Context, kubeconfig string) error {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("loading kubeconfig: %w", err)
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}

	wp := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "clrk.apoxy.dev/v1alpha1",
		"kind":       "WorkerPool",
		"metadata": map[string]interface{}{
			"name":      "default",
			"namespace": "default",
		},
		"spec": map[string]interface{}{
			"replicas": int64(1),
			"podTemplate": map[string]interface{}{
				"spec": map[string]interface{}{
					"containers": []interface{}{
						map[string]interface{}{
							"name":  "worker",
							"image": "clrk/worker:latest",
						},
					},
				},
			},
		},
	}}

	gvr := schema.GroupVersionResource{
		Group:    "clrk.apoxy.dev",
		Version:  "v1alpha1",
		Resource: "workerpools",
	}
	data, err := wp.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshalling WorkerPool: %w", err)
	}
	_, err = dynClient.Resource(gvr).Namespace("default").Patch(
		ctx, "default", types.ApplyPatchType, data,
		metav1.PatchOptions{FieldManager: "clrk-dev", Force: ptr(true)},
	)
	if err != nil {
		return fmt.Errorf("applying default WorkerPool: %w", err)
	}
	return nil
}

func ptr[T any](v T) *T { return &v }
