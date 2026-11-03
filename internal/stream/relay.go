package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Master290/kite/internal/config"
)

const defaultMaxRelayBackoff = 10 * time.Second

var errRelayClosed = errors.New("relay body closed")

type relayBody struct {
	rc           io.ReadCloser
	closed       atomic.Bool
	idleTimedOut atomic.Bool
}

func (b *relayBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if err != nil && b.closed.Load() {
		return n, fmt.Errorf("%w: %w", errRelayClosed, err)
	}
	return n, err
}

func (b *relayBody) Close() error {
	b.closed.Store(true)
	return b.rc.Close()
}

func (b *relayBody) closeIdleTimeout() error {
	b.idleTimedOut.Store(true)
	b.closed.Store(true)
	return b.rc.Close()
}

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

func jitterRelayDelay(d, maxBackoff time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxRelayBackoff
	}
	if d > maxBackoff {
		d = maxBackoff
	}
	// Apply ±20% jitter ([0.8*d, 1.2*d]), capped at maxBackoff.
	factor := 0.8 + rand.Float64()*0.4
	jittered := time.Duration(float64(d) * factor)
	if jittered > maxBackoff {
		jittered = maxBackoff
	}
	if jittered <= 0 {
		return d
	}
	return jittered
}

func redactRelayURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "<invalid-url>"
	}
	return u.Redacted()
}

func minStableRelaySession(maxBackoff time.Duration) time.Duration {
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxRelayBackoff
	}
	stable := 3 * time.Second
	if half := maxBackoff / 2; half < stable {
		stable = half
	}
	return stable
}

func (rw *relayWorker) run() {
	maxBackoff := rw.mount.relayMaxBackoff()
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxRelayBackoff
	}
	baseDelay := rw.retryDelay
	if baseDelay > maxBackoff {
		baseDelay = maxBackoff
	}
	delay := baseDelay
	for {
		select {
		case <-rw.ctx.Done():
			return
		default:
		}

		maxBackoff = rw.mount.relayMaxBackoff()
		if maxBackoff <= 0 {
			maxBackoff = defaultMaxRelayBackoff
		}
		sustained, reason := rw.streamOnce(minStableRelaySession(maxBackoff))
		if rw.ctx.Err() != nil {
			return
		}
		rw.mount.observer.RelayReconnect(rw.mount.Config().Path, reason)

		if sustained {
			delay = baseDelay
		}

		wait := jitterRelayDelay(delay, maxBackoff)
		if delay < maxBackoff {
			delay *= 2
			if delay > maxBackoff {
				delay = maxBackoff
			}
		}

		select {
		case <-rw.ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (rw *relayWorker) watchIdleRead(body *relayBody, idleTimeout time.Duration, done <-chan struct{}) {
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
				_ = body.closeIdleTimeout()
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

func (rw *relayWorker) streamOnce(minStableDuration time.Duration) (bool, string) {
	mountPath := rw.mount.Config().Path
	redactedURL := redactRelayURL(rw.cfg.URL)
	log := rw.mount.logger()

	req, err := http.NewRequestWithContext(rw.ctx, http.MethodGet, rw.cfg.URL, nil)
	if err != nil {
		log.Debug("relay connect attempt failed", "mount", mountPath, "url", redactedURL, "reason", "invalid_request", "error", err)
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
		reason := "connect_error"
		if IsTimeoutError(err) {
			reason = "connect_timeout"
		}
		log.Debug("relay connect attempt failed", "mount", mountPath, "url", redactedURL, "reason", reason, "error", err)
		return false, reason
	}
	body := &relayBody{rc: resp.Body}
	defer body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Debug("relay connect attempt failed", "mount", mountPath, "url", redactedURL, "reason", "bad_status", "status", resp.StatusCode)
		return false, "bad_status"
	}

	if err := rw.mount.acquireRelaySourceWithCloser(body); err != nil {
		// Another source (e.g. direct DJ) is already active
		log.Debug("relay connect attempt skipped", "mount", mountPath, "url", redactedURL, "reason", "source_busy")
		return false, "source_busy"
	}
	defer rw.mount.ReleaseSource()

	rw.mount.observer.RelayConnected(mountPath)
	log.Info("relay connected", "mount", mountPath, "url", redactedURL)

	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go rw.watchIdleRead(body, rw.mount.relayIdleTimeout(), watchdogDone)

	// Check if upstream provides ICY metadata interval
	var reader io.Reader = body
	if metaintStr := resp.Header.Get("icy-metaint"); metaintStr != "" {
		if metaint, err := strconv.Atoi(metaintStr); err == nil && metaint > 0 {
			reader = newICYReader(body, metaint, rw.mount.SetMetadata)
		}
	}

	// Also check initial headers for icy-name if primary metadata title is unset
	if icyName := resp.Header.Get("icy-name"); icyName != "" {
		rw.mount.setDefaultPrimaryTitle(icyName)
	}

	var (
		firstAudioAt time.Time
		lastAudioAt  time.Time
	)
	pumpErr := Pump(rw.mount.Profile(), reader, func(p []byte) error {
		if err := rw.mount.Write(p); err != nil {
			return err
		}
		now := time.Now()
		if firstAudioAt.IsZero() {
			firstAudioAt = now
		}
		lastAudioAt = now
		return nil
	})

	reason := "stream_error"
	switch {
	case body.idleTimedOut.Load():
		reason = "idle_timeout"
	case rw.ctx.Err() != nil:
		reason = "shutdown"
	case errors.Is(pumpErr, io.EOF) || errors.Is(pumpErr, io.ErrUnexpectedEOF):
		reason = "eof"
	case IsTimeoutError(pumpErr):
		reason = "read_timeout"
	case errors.Is(pumpErr, errRelayClosed) || errors.Is(pumpErr, net.ErrClosed):
		reason = "preempted"
	}

	rw.mount.observer.RelayDisconnected(mountPath, reason)
	log.Info("relay disconnected", "mount", mountPath, "url", redactedURL, "reason", reason)

	sustained := !firstAudioAt.IsZero() &&
		reason != "idle_timeout" &&
		reason != "preempted" &&
		lastAudioAt.Sub(firstAudioAt) >= minStableDuration
	return sustained, reason
}
