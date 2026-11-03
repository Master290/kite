package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Master290/kite/internal/config"
)

const (
	maxRelayBackoff         = 30 * time.Second
	maxRelayBackoffAttempts = 5
)

type relayWorker struct {
	mount      *Mount
	cfg        config.RelayConfig
	ctx        context.Context
	cancel     context.CancelFunc
	client     *http.Client
	retryDelay time.Duration
}

func (m *Mount) startRelay(cfg config.RelayConfig) {
	m.relayMu.Lock()
	defer m.relayMu.Unlock()

	if m.relayCancel != nil {
		m.relayCancel()
	}

	ctx, cancel := context.WithCancel(m.ctx)
	m.relayCancel = cancel

	retryDelay := cfg.RetryDelay.Duration()
	if retryDelay <= 0 {
		retryDelay = 3 * time.Second
	}

	idleTimeout := m.relayIdleTimeout()
	connectTimeout := 10 * time.Second
	if idleTimeout > 0 && idleTimeout < connectTimeout {
		connectTimeout = idleTimeout
	}

	rw := &relayWorker{
		mount:      m,
		cfg:        cfg,
		ctx:        ctx,
		cancel:     cancel,
		retryDelay: retryDelay,
		client: &http.Client{
			// No overall timeout on the client because streaming responses are long-lived.
			// Dial, TLS handshake, and response header timeouts bound connection setup,
			// while the idle-read watchdog closes resp.Body when the stream goes silent.
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   connectTimeout,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout:   connectTimeout,
				ResponseHeaderTimeout: connectTimeout,
				IdleConnTimeout:       30 * time.Second,
			},
		},
	}

	go rw.run()
}

func (m *Mount) stopRelay() {
	m.relayMu.Lock()
	defer m.relayMu.Unlock()
	if m.relayCancel != nil {
		m.relayCancel()
		m.relayCancel = nil
	}
}

func jitterRelayDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	if d > maxRelayBackoff {
		d = maxRelayBackoff
	}
	// Apply ±20% jitter ([0.8*d, 1.2*d]), capped at maxRelayBackoff (30s).
	factor := 0.8 + rand.Float64()*0.4
	jittered := time.Duration(float64(d) * factor)
	if jittered > maxRelayBackoff {
		jittered = maxRelayBackoff
	}
	if jittered <= 0 {
		return d
	}
	return jittered
}

func (rw *relayWorker) run() {
	baseDelay := rw.retryDelay
	if baseDelay > maxRelayBackoff {
		baseDelay = maxRelayBackoff
	}
	delay := baseDelay
	failures := 0
	for {
		select {
		case <-rw.ctx.Done():
			return
		default:
		}

		streamed, reason := rw.streamOnce()
		if rw.ctx.Err() != nil {
			return
		}
		rw.mount.observer.RelayReconnect(rw.mount.Config().Path, reason)

		if streamed {
			delay = baseDelay
			failures = 0
		} else {
			failures++
			if failures >= maxRelayBackoffAttempts {
				delay = baseDelay
				failures = 0
			}
		}

		wait := jitterRelayDelay(delay)
		if rw.waitOrProbe(wait, baseDelay) {
			delay = baseDelay
			failures = 0
			continue
		}
		if rw.ctx.Err() != nil {
			return
		}

		if delay < maxRelayBackoff {
			delay *= 2
			if delay > maxRelayBackoff {
				delay = maxRelayBackoff
			}
		}
	}
}

// waitOrProbe waits for wait duration, periodically probing the upstream when
// backoff is elevated so recovery is fast once the upstream returns. Returns
// true if a probe succeeded early.
func (rw *relayWorker) waitOrProbe(wait, baseDelay time.Duration) bool {
	if wait <= baseDelay*3/2 {
		select {
		case <-rw.ctx.Done():
		case <-time.After(wait):
		}
		return false
	}

	probeInterval := baseDelay
	if probeInterval > 2*time.Second {
		probeInterval = 2 * time.Second
	}
	if probeInterval < 20*time.Millisecond {
		probeInterval = 20 * time.Millisecond
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rw.ctx.Done():
			return false
		case <-timer.C:
			return false
		case <-ticker.C:
			if rw.probeUpstream(probeInterval) {
				return true
			}
		}
	}
}

func (rw *relayWorker) probeUpstream(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(rw.ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rw.cfg.URL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "Kite/1.0 (Relay)")
	pass := rw.resolvePassword()
	if rw.cfg.Username != "" && pass != "" {
		req.SetBasicAuth(rw.cfg.Username, pass)
	}
	resp, err := rw.client.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode < 500
}

func (rw *relayWorker) watchIdleRead(body io.Closer, idleTimeout time.Duration, idleTimedOut *atomic.Bool, done <-chan struct{}) {
	if idleTimeout <= 0 {
		idleTimeout = 10 * time.Second
	}
	interval := idleTimeout / 4
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	} else if interval > 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-rw.ctx.Done():
			_ = body.Close()
			return
		case <-ticker.C:
			rw.mount.mu.RLock()
			last := rw.mount.lastSource
			rw.mount.mu.RUnlock()
			if !last.IsZero() && time.Since(last) >= idleTimeout {
				idleTimedOut.Store(true)
				_ = body.Close()
				return
			}
		}
	}
}

func (rw *relayWorker) resolvePassword() string {
	if rw.cfg.Password != "" {
		return rw.cfg.Password
	}
	if rw.cfg.PasswordEnv != "" {
		return os.Getenv(rw.cfg.PasswordEnv)
	}
	if rw.cfg.PasswordFile != "" {
		b, err := os.ReadFile(rw.cfg.PasswordFile)
		if err == nil {
			return strings.TrimSpace(string(b))
		}
	}
	return ""
}

func (rw *relayWorker) streamOnce() (bool, string) {
	req, err := http.NewRequestWithContext(rw.ctx, http.MethodGet, rw.cfg.URL, nil)
	if err != nil {
		return false, "invalid_request"
	}

	req.Header.Set("Icy-MetaData", "1")
	req.Header.Set("User-Agent", "Kite/1.0 (Relay)")

	pass := rw.resolvePassword()
	if rw.cfg.Username != "" && pass != "" {
		req.SetBasicAuth(rw.cfg.Username, pass)
	}

	resp, err := rw.client.Do(req)
	if err != nil {
		if rw.ctx.Err() != nil {
			return false, "shutdown"
		}
		if IsTimeoutError(err) {
			return false, "connect_timeout"
		}
		return false, "connect_error"
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, "bad_status"
	}

	if err := rw.mount.AcquireRelaySource(); err != nil {
		// Another source (e.g. direct DJ) is already active
		return false, "source_busy"
	}
	defer rw.mount.ReleaseSource()

	mountPath := rw.mount.Config().Path
	rw.mount.observer.RelayConnected(mountPath)
	slog.Info("relay connected", "mount", mountPath, "url", rw.cfg.URL)

	rw.mount.SetSourceCloser(resp.Body)

	var idleTimedOut atomic.Bool
	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go rw.watchIdleRead(resp.Body, rw.mount.relayIdleTimeout(), &idleTimedOut, watchdogDone)

	// Check if upstream provides ICY metadata interval
	var reader io.Reader = resp.Body
	if metaintStr := resp.Header.Get("icy-metaint"); metaintStr != "" {
		if metaint, err := strconv.Atoi(metaintStr); err == nil && metaint > 0 {
			reader = newICYReader(resp.Body, metaint, rw.mount.SetMetadata)
		}
	}

	// Also check initial headers for icy-name if primary metadata title is unset
	if icyName := resp.Header.Get("icy-name"); icyName != "" {
		rw.mount.setDefaultPrimaryTitle(icyName)
	}

	var streamed atomic.Bool
	pumpErr := Pump(rw.mount.Profile(), reader, func(p []byte) error {
		if err := rw.mount.Write(p); err != nil {
			return err
		}
		streamed.Store(true)
		return nil
	})

	reason := "eof"
	switch {
	case idleTimedOut.Load():
		reason = "idle_timeout"
	case rw.ctx.Err() != nil:
		reason = "shutdown"
	case errors.Is(pumpErr, io.EOF) || errors.Is(pumpErr, io.ErrUnexpectedEOF):
		reason = "eof"
	case IsTimeoutError(pumpErr):
		reason = "read_timeout"
	case pumpErr != nil:
		reason = "stream_error"
	}

	rw.mount.observer.RelayDisconnected(mountPath, reason)
	slog.Info("relay disconnected", "mount", mountPath, "url", rw.cfg.URL, "reason", reason)
	return streamed.Load(), reason
}
