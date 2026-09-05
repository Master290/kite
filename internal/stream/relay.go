package stream

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
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

	rw := &relayWorker{
		mount:      m,
		cfg:        cfg,
		ctx:        ctx,
		cancel:     cancel,
		retryDelay: retryDelay,
		client: &http.Client{
			// No overall timeout on the client because streaming responses are long-lived.
			// Handshake and header timeouts are handled by the transport if needed.
			Transport: &http.Transport{
				ResponseHeaderTimeout: 10 * time.Second,
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
	for {
		select {
		case <-rw.ctx.Done():
			return
		default:
		}

		rw.streamOnce()

		select {
		case <-rw.ctx.Done():
			return
		case <-time.After(rw.retryDelay):
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

func (rw *relayWorker) streamOnce() {
	req, err := http.NewRequestWithContext(rw.ctx, http.MethodGet, rw.cfg.URL, nil)
	if err != nil {
		return
	}

	req.Header.Set("Icy-MetaData", "1")
	req.Header.Set("User-Agent", "Kite/1.0 (Relay)")

	pass := rw.resolvePassword()
	if rw.cfg.Username != "" && pass != "" {
		req.SetBasicAuth(rw.cfg.Username, pass)
	}

	resp, err := rw.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	if err := rw.mount.AcquireRelaySource(); err != nil {
		// Another source (e.g. direct DJ) is already active
		return
	}
	defer rw.mount.ReleaseSource()

	rw.mount.SetSourceCloser(resp.Body)

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

	_ = Pump(rw.mount.Profile(), reader, rw.mount.Write)
}
