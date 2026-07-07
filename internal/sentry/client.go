// Package sentry is clrk's phone-home client to api.apoxy.dev. The flow is
// one-way and opt-in behind the CLRKConfig signup + the controller-manager
// flags:
//
//   - register: exchange the operator email for a deployment id + bearer token
//     (the token is persisted to a k8s Secret by the clrkconfig controller,
//     never held here).
//   - advise:   pull security advisories that cosmos PRODUCES and materialize
//     each as an events.k8s.io/v1 Event in the Notification Center.
//
// There is no outbound reporting: cosmos never ingests cluster data. Nothing
// about the cluster (destinations, request bodies, prompts, headers,
// credentials) ever leaves; the only thing sent is the signup email at
// registration.
package sentry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// DefaultBaseURL is api.apoxy.dev unless overridden by --apoxy-api-base-url.
	DefaultBaseURL = "https://api.apoxy.dev"

	// DefaultTimeout caps each phone-home request. api.apoxy.dev is off-cluster;
	// 10s is generous but bounds goroutine pile-ups when it is slow, not dead.
	DefaultTimeout = 10 * time.Second

	// DefaultAdvisoryPoll is the advisory cadence when the register response
	// does not specify one (pre-register or an older server). The server
	// normally drives this; cosmos hands back a 1-minute cadence.
	DefaultAdvisoryPoll = 1 * time.Minute
)

// Client is a thin HTTP client for the api.apoxy.dev phone-home API served by
// cosmos's ClrkService under the /v1/clrk/ prefix.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient builds a phone-home client. Empty baseURL/timeout fall back to the
// package defaults.
func NewClient(baseURL string, timeout time.Duration) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: timeout},
	}
}

// RegisterRequest is posted to /v1/clrk/register on signup.
type RegisterRequest struct {
	Email       string `json:"email"`
	ClrkVersion string `json:"clrk_version,omitempty"`
	ClusterUID  string `json:"cluster_uid,omitempty"`
}

// RegisterResponse is the register reply. Token is written to a Secret by the
// caller and never retained by this client.
type RegisterResponse struct {
	DeploymentID             string `json:"deployment_id"`
	Token                    string `json:"token"`
	AdvisoryPollIntervalSecs int    `json:"advisory_poll_interval_seconds,omitempty"`
}

// AdvisoryPollInterval resolves the server-provided cadence, falling back to the
// default when unset or nonsensical.
func (r *RegisterResponse) AdvisoryPollInterval() time.Duration {
	if r == nil || r.AdvisoryPollIntervalSecs <= 0 {
		return DefaultAdvisoryPoll
	}
	return time.Duration(r.AdvisoryPollIntervalSecs) * time.Second
}

// Register exchanges the operator email for a deployment id + bearer token.
func (c *Client) Register(ctx context.Context, req RegisterRequest) (*RegisterResponse, error) {
	var out RegisterResponse
	if err := c.do(ctx, http.MethodPost, "/v1/clrk/register", "", req, &out); err != nil {
		return nil, err
	}
	if out.DeploymentID == "" || out.Token == "" {
		return nil, fmt.Errorf("register: response missing deployment_id or token")
	}
	return &out, nil
}

// Advisory is one security advisory produced by cosmos.
type Advisory struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Severity string `json:"severity"`
	Summary  string `json:"summary"`
	URL      string `json:"url,omitempty"`
	IssuedAt string `json:"issued_at,omitempty"`
}

type advisoriesResponse struct {
	Advisories []Advisory `json:"advisories"`
}

// FetchAdvisories pulls the active advisory set for the deployment. token
// authenticates the call. An empty feed decodes to a nil slice.
func (c *Client) FetchAdvisories(ctx context.Context, token string) ([]Advisory, error) {
	var out advisoriesResponse
	if err := c.do(ctx, http.MethodGet, "/v1/clrk/advisories", token, nil, &out); err != nil {
		return nil, err
	}
	return out.Advisories, nil
}

// do performs one JSON request. reqBody nil => no body; out nil => response
// discarded. A >=300 status is an error.
func (c *Client) do(ctx context.Context, method, path, token string, reqBody, out any) error {
	var body io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if out != nil {
		req.Header.Set("Accept", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		// An empty 2xx body (a bare empty 200 or a 204) is a valid "nothing to
		// report" answer -- e.g. cosmos returning no advisories. Decode surfaces
		// that as io.EOF; treat it as a zero-value response rather than a hard
		// error that would stall the poll loop.
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
