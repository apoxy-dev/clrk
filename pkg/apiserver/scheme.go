package apiserver

import (
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// Scheme is the controller-runtime scheme used by the embedded apiserver and
// the reconcilers attached to it. It is populated at init() time so that the
// apiserver and ctrl.Manager both see the same type registry.
var Scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clrkv1alpha1.Install(Scheme))
	utilruntime.Must(coordinationv1.AddToScheme(Scheme))
}
