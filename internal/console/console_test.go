package console

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestNewHandlerReturnsEmptyCoreDiscovery(t *testing.T) {
	upstreamURL := &url.URL{Scheme: "https", Host: "127.0.0.1:1"}
	server := httptest.NewServer(NewHandler(upstreamURL))
	defer server.Close()

	response, err := http.Get(server.URL + "/api")
	if err != nil {
		t.Fatalf("get core discovery: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("core discovery status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var discovery struct {
		APIVersion string            `json:"apiVersion"`
		Kind       string            `json:"kind"`
		Items      []json.RawMessage `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&discovery); err != nil {
		t.Fatalf("decode core discovery: %v", err)
	}
	if discovery.APIVersion != "apidiscovery.k8s.io/v2" || discovery.Kind != "APIGroupDiscoveryList" {
		t.Fatalf("unexpected core discovery type %s %s", discovery.APIVersion, discovery.Kind)
	}
	if len(discovery.Items) != 0 {
		t.Fatalf("core discovery items = %d, want 0", len(discovery.Items))
	}
}

func TestNewHandlerProxiesWebSocketUpgrade(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/console/watch" || r.URL.RawQuery != "watch=true" {
			http.Error(w, "unexpected watch target", http.StatusBadRequest)
			return
		}
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "websocket upgrade is required", http.StatusBadRequest)
			return
		}

		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "response writer cannot upgrade", http.StatusInternalServerError)
			return
		}
		conn, rw, err := hijacker.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()

		_, _ = fmt.Fprint(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		if err := rw.Flush(); err != nil {
			return
		}
		payload := make([]byte, 4)
		if _, err := io.ReadFull(rw, payload); err != nil || string(payload) != "ping" {
			return
		}
		_, _ = rw.WriteString("pong")
		_ = rw.Flush()
	}))
	defer upstream.Close()

	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	proxy := httptest.NewServer(NewHandler(upstreamURL))
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	conn, err := net.DialTimeout("tcp", proxyURL.Host, 2*time.Second)
	if err != nil {
		t.Fatalf("connect to proxy: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set connection deadline: %v", err)
	}

	request := "GET /console/watch?watch=true HTTP/1.1\r\n" +
		"Host: " + proxyURL.Host + "\r\n" +
		"Connection: Upgrade\r\n" +
		"Upgrade: websocket\r\n\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatalf("send upgrade request: %v", err)
	}

	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read upgrade status: %v", err)
	}
	if status != "HTTP/1.1 101 Switching Protocols\r\n" {
		t.Fatalf("unexpected upgrade status %q", status)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read upgrade headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}

	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatalf("send upgraded payload: %v", err)
	}
	payload := make([]byte, 4)
	if _, err := io.ReadFull(reader, payload); err != nil {
		t.Fatalf("read upgraded payload: %v", err)
	}
	if string(payload) != "pong" {
		t.Fatalf("unexpected upgraded payload %q", payload)
	}
}
