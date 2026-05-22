// Package otelemit is the shared OTLP/HTTP TracerProvider+LoggerProvider
// bootstrap used by both the controller-manager (extproc capture sink)
// and the worker (sandbox/egress events). Resource composition stays
// at the call site so each component sets its own service.name.
//
// Emitter.Tracer() and Emitter.Logger() are safe for concurrent use.
// Close blocks until both providers flush or the supplied context
// fires, whichever comes first.
package otelemit
