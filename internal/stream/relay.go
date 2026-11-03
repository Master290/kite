package stream

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Master290/kite/internal/config"
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

func (rw *relayWorker) run() {
	delay := rw.retryDelay
	maxDelay := rw.retryDelay * 8
	if maxDelay < 30*time.Second {
		maxDelay = 30 * time.Second
	}
	for {
		select {
		case <-rw.ctx.Done():
			return
		default:
		}

		if rw.streamOnce() {
			delay = rw.retryDelay
		}

		select {
		case <-rw.ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}

func (rw *relayWorker) watchIdleRead(body io.Closer, idleTimeout time.Duration, done <-chan struct{}) {
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

func (rw *relayWorker) streamOnce() bool {
	req, err := http.NewRequestWithContext(rw.ctx, http.MethodGet, rw.cfg.URL, nil)
	if err != nil {
		return false
	}

	req.Header.Set("Icy-MetaData", "1")
	req.Header.Set("User-Agent", "Kite/1.0 (Relay)")

	pass := rw.resolvePassword()
	if rw.cfg.Username != "" && pass != "" {
		req.SetBasicAuth(rw.cfg.Username, pass)
	}

	resp, err := rw.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	if err := rw.mount.AcquireRelaySource(); err != nil {
		// Another source (e.g. direct DJ) is already active
		return false
	}
	defer rw.mount.ReleaseSource()

	rw.mount.SetSourceCloser(resp.Body)

	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go rw.watchIdleRead(resp.Body, rw.mount.relayIdleTimeout(), watchdogDone)

	// Check if upstream provides ICY metadata interval
	var reader io.Reader = resp.Body
	if metaintStr := resp.Header.Get("icy-metaint"); metaintStr != "" {
		if metaint, err := strconv.Atoi(metaintStr); err == nil && metaint > 0 {
			reader = newICYReader(resp.Body, metaint, rw.mount.SetMetadata)
		}
	}

	// Also check initial headers for icy-name if metadata title is unset
	if icyName := resp.Header.Get("icy-name"); icyName != "" {
		current := rw.mount.Metadata()
		if current.Title == "" {
			rw.mount.SetMetadata(Metadata{Title: icyName})
		}
	}

	var streamed atomic.Bool
	_ = Pump(rw.mount.Profile(), reader, func(p []byte) error {
		if err := rw.mount.Write(p); err != nil {
			return err
		}
		streamed.Store(true)
		return nil
	})
	return streamed.Load()
}
