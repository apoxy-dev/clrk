package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/ports"
)

// exitCodeError carries a process exit code out of a cobra RunE without
// os.Exit, so cmd/clrk/main.go can exit with it (124/77/...) instead of
// the default 1. An empty message prints nothing -- run-task has already
// written the agent's response and status -- it only sets the code.
type exitCodeError struct {
	code int
	msg  string
}

func (e *exitCodeError) Error() string { return e.msg }
func (e *exitCodeError) ExitCode() int { return e.code }

// newAgentsRunTaskCmd is `clrk agent run-task <name>` -- the write-side
// counterpart to `invocations`. It POSTs to the taskagents/<name>/invoke
// connect subresource (which reverse-proxies to the per-TA ingress
// Envoy), prints the buffered response, then follows the Invocation it
// created to a terminal phase and exits with a code that mirrors it
// (Succeeded 0 / Failed 1 / Timeout 124 / Rejected 77).
//
// Correlation is by a CLI-minted invocation id: the CLI sends it as
// X-Clrk-Invocation-ID, ingress honors it when canonical (APO-684), so
// the run the CLI started carries the id the CLI already knows and can
// Get from the read model.
func newAgentsRunTaskCmd() *cobra.Command {
	var (
		namespace   string
		local       bool
		kubeconfig  string
		input       string
		request     string
		headers     []string
		ceType      string
		ceSource    string
		ceSubject   string
		contentType string
		timeout     time.Duration
	)
	cmd := &cobra.Command{
		Use:     "run-task NAME",
		Short:   "Invoke a TaskAgent and exit with its run's status",
		Aliases: []string{"run"},
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if input != "" && request != "" {
				return fmt.Errorf("--input and --request are mutually exclusive")
			}
			kc, err := resolveKubeconfig(kubeconfig, local)
			if err != nil {
				return err
			}
			cfg, err := clientcmd.BuildConfigFromFlags("", kc)
			if err != nil {
				return fmt.Errorf("loading kubeconfig %s: %w", kc, err)
			}
			ns := namespace
			if ns == "" {
				if ns, err = contextNamespace(kc); err != nil {
					return err
				}
			}
			return runTask(cmd.Context(), cmd.OutOrStdout(), cfg, ns, args[0], runTaskOpts{
				input:       input,
				request:     request,
				headers:     headers,
				ceType:      ceType,
				ceSource:    ceSource,
				ceSubject:   ceSubject,
				contentType: contentType,
				timeout:     timeout,
			})
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "Request body: literal, @file, or - for stdin. Becomes the agent's stdin.")
	cmd.Flags().StringVar(&request, "request", "", "Read a full CloudEvents HTTP request (headers + body) from a file or - for stdin; forwarded verbatim. Mutually exclusive with --input.")
	cmd.Flags().StringArrayVarP(&headers, "header", "H", nil, "Extra request header k=v (repeatable); e.g. ce-* or X-Clrk-* headers.")
	cmd.Flags().StringVar(&ceType, "ce-type", "", "CloudEvents type (sets the ce-type header).")
	cmd.Flags().StringVar(&ceSource, "ce-source", "", "CloudEvents source (sets the ce-source header).")
	cmd.Flags().StringVar(&ceSubject, "ce-subject", "", "CloudEvents subject (sets the ce-subject header).")
	cmd.Flags().StringVar(&contentType, "content-type", "application/json", "Content-Type for the --input body.")
	cmd.Flags().DurationVar(&timeout, "timeout", 60*time.Second, "Deadline for the run to reach a terminal phase.")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Target namespace (default: kubeconfig context).")
	cmd.Flags().BoolVar(&local, "local", false, "Target the kubeconfig of the running 'clrk dev' session (~/.clrk/kubeconfig.host).")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Explicit kubeconfig path (takes precedence over --local and $KUBECONFIG).")
	return cmd
}

type runTaskOpts struct {
	input       string
	request     string
	headers     []string
	ceType      string
	ceSource    string
	ceSubject   string
	contentType string
	timeout     time.Duration
}

func runTask(ctx context.Context, out io.Writer, cfg *rest.Config, ns, name string, opts runTaskOpts) error {
	rc, err := clrkRESTClient(cfg)
	if err != nil {
		return err
	}

	body, header, err := assembleRequest(opts)
	if err != nil {
		return err
	}

	// Mint the invocation id and pin it so the run we start carries an id
	// we already know. X-Clrk-TaskAgent and X-Clrk-Trigger are stamped
	// authoritatively by the invoke Connecter, so the caller cannot
	// retarget or relabel; the id is the one header we must set.
	invID := uuid.NewString()
	header.Set(ports.HeaderInvocationID, invID)

	// taskagents/<name>/invoke -- the write-side connect subresource. The
	// request builder assembles the group/version/namespace/resource path,
	// so the only literal is the subresource name.
	req := rc.Post().
		Namespace(ns).
		Resource(taskAgentGVR.Resource).
		Name(name).
		SubResource("invoke").
		Body(body)
	for k, vals := range header {
		req.SetHeader(k, vals...)
	}
	// Raw() returns the proxied body alongside any non-2xx error, so the
	// agent's output is printed whether the run succeeded or failed; the
	// exit code is decided by the Invocation phase below, not this status.
	respBody, invokeErr := req.Do(ctx).Raw()
	if len(respBody) > 0 {
		_, _ = out.Write(respBody)
		if respBody[len(respBody)-1] != '\n' {
			_, _ = io.WriteString(out, "\n")
		}
	}

	// Follow our invocation to a terminal phase. If the invoke transport
	// itself failed (e.g. no ready ingress endpoint), an Invocation may
	// never materialize -- cap the wait short in that case so we surface
	// the transport error promptly rather than after the full timeout.
	followTimeout := opts.timeout
	if invokeErr != nil && followTimeout > 6*time.Second {
		followTimeout = 6 * time.Second
	}
	phase, terminal, followErr := followInvocation(ctx, rc, ns, invID, followTimeout)
	if !terminal {
		switch {
		case invokeErr != nil:
			// The invoke transport itself failed and no invocation
			// materialized -- the transport error is the real cause.
			return fmt.Errorf("invoke request failed: %w", invokeErr)
		case followErr != nil:
			return fmt.Errorf("reading invocation %s status: %w", invID, followErr)
		default:
			return &exitCodeError{
				code: 124,
				msg:  fmt.Sprintf("clrk: timed out after %s waiting for invocation %s to reach a terminal phase", opts.timeout, invID),
			}
		}
	}

	code := exitCodeForPhase(phase)
	if code == 0 {
		return nil
	}
	return &exitCodeError{code: code, msg: fmt.Sprintf("clrk: invocation %s %s", invID, phase)}
}

// assembleRequest builds the request body and headers from the flags.
// Either --request (a full HTTP request, verbatim) or --input plus the
// ce-*/-H/--content-type flags supplies the envelope; the minimal valid
// invocation needs only the body (the worker self-resolves the rest).
func assembleRequest(opts runTaskOpts) ([]byte, http.Header, error) {
	header := http.Header{}
	if opts.request != "" {
		return readFullRequest(opts.request, header)
	}

	var body []byte
	if opts.input != "" {
		b, err := readInputSpec(opts.input)
		if err != nil {
			return nil, nil, err
		}
		body = b
	}
	// Only stamp Content-Type when there is a body to describe (the flag
	// documents itself as "Content-Type for the --input body"); a bodyless
	// trigger carries none.
	if len(body) > 0 && opts.contentType != "" {
		header.Set("Content-Type", opts.contentType)
	}
	if opts.ceType != "" {
		header.Set("ce-type", opts.ceType)
	}
	if opts.ceSource != "" {
		header.Set("ce-source", opts.ceSource)
	}
	if opts.ceSubject != "" {
		header.Set("ce-subject", opts.ceSubject)
	}
	for _, h := range opts.headers {
		k, v, ok := strings.Cut(h, "=")
		if !ok {
			return nil, nil, fmt.Errorf("invalid --header %q (want k=v)", h)
		}
		header.Add(strings.TrimSpace(k), strings.TrimSpace(v))
	}
	return body, header, nil
}

// readInputSpec resolves an --input value: "-" reads stdin, "@path"
// reads a file, anything else is the literal body.
func readInputSpec(spec string) ([]byte, error) {
	switch {
	case spec == "-":
		return io.ReadAll(os.Stdin)
	case strings.HasPrefix(spec, "@"):
		return os.ReadFile(spec[1:])
	default:
		return []byte(spec), nil
	}
}

// readFullRequest reads a complete HTTP request (request line, headers,
// body) from a file or stdin and returns its body and forwardable
// headers. Binary-mode CloudEvents (ce-* headers + body) and
// structured-mode (Content-Type: application/cloudevents+json) both ride
// through unchanged; hop-by-hop and length/host headers the REST client
// re-derives are dropped.
func readFullRequest(spec string, header http.Header) ([]byte, http.Header, error) {
	var src io.Reader
	if spec == "-" {
		src = os.Stdin
	} else {
		f, err := os.Open(spec)
		if err != nil {
			return nil, nil, err
		}
		defer f.Close()
		src = f
	}
	req, err := http.ReadRequest(bufio.NewReader(src))
	if err != nil {
		return nil, nil, fmt.Errorf("parsing --request as an HTTP request: %w", err)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("reading --request body: %w", err)
	}
	for k, vals := range req.Header {
		switch http.CanonicalHeaderKey(k) {
		case "Host", "Content-Length", "Connection", "Transfer-Encoding":
			continue
		}
		for _, v := range vals {
			header.Add(k, v)
		}
	}
	return body, header, nil
}

// followInvocation polls the top-level (namespace-scoped) invocations
// read model for our id until it reaches a terminal phase or the timeout
// elapses. A 404 means the Consumer has not materialized the run yet and
// transient 5xx mean the apiserver/ClickHouse is still coming up (or
// bouncing) -- both are retried until the deadline. A permanent
// authorization failure (403/401) is surfaced immediately rather than
// masked as a timeout. Returns (phase, true, nil) on a terminal phase,
// ("", false, nil) on deadline/cancel, or ("", false, err) on a
// permanent error.
func followInvocation(ctx context.Context, rc rest.Interface, ns, id string, timeout time.Duration) (string, bool, error) {
	deadline := time.Now().Add(timeout)
	backoff := 100 * time.Millisecond
	for {
		// invocations/<id> -- the top-level namespace-scoped read model.
		raw, err := rc.Get().
			Namespace(ns).
			Resource("invocations").
			Name(id).
			DoRaw(ctx)
		switch {
		case err == nil:
			var inv unstructured.Unstructured
			if uerr := inv.UnmarshalJSON(raw); uerr == nil {
				phase, _, _ := unstructured.NestedString(inv.Object, "status", "phase")
				if isTerminalPhase(phase) {
					return phase, true, nil
				}
			}
		case apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err):
			// Permanent and deterministic -- retrying only delays the
			// inevitable and hides the real cause behind a timeout.
			return "", false, err
		}
		if time.Now().After(deadline) {
			return "", false, nil
		}
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func isTerminalPhase(phase string) bool {
	return exitCodeForPhase(phase) >= 0
}

// exitCodeForPhase maps a terminal Invocation phase to a process exit
// code: Succeeded 0, Failed 1, Timeout 124 (matching timeout(1)),
// Rejected 77. A non-terminal or unknown phase returns -1.
func exitCodeForPhase(phase string) int {
	switch clrkv1alpha1.InvocationPhase(phase) {
	case clrkv1alpha1.InvocationPhaseSucceeded:
		return 0
	case clrkv1alpha1.InvocationPhaseFailed:
		return 1
	case clrkv1alpha1.InvocationPhaseTimeout:
		return 124
	case clrkv1alpha1.InvocationPhaseRejected:
		return 77
	default:
		return -1
	}
}
