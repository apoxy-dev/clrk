// echo-bot is a trivial DaemonAgent example that emits a heartbeat log
// line and then makes an outbound HTTPS call in a loop. When deployed
// with an EgressGateway ref, the HTTPS call flows through clrk's MITM
// path so request/response bodies land in the ext_proc capture sink.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	target := os.Getenv("ECHO_TARGET")
	if target == "" {
		target = "https://httpbin.org/anything"
	}
	interval := 5 * time.Second
	if s := os.Getenv("ECHO_INTERVAL"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			interval = d
		}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tick := time.NewTicker(interval)
	defer tick.Stop()

	slog.Info("echo-bot running", "target", target, "interval", interval)

	for {
		if err := poll(ctx, client, target); err != nil {
			slog.Error("poll failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func poll(ctx context.Context, client *http.Client, target string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "clrk-echo-bot/0")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	slog.Info("echo-bot reply", "status", resp.StatusCode, "bytes", len(body))
	return nil
}
