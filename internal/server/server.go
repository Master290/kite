package server

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Master290/kite/internal/config"
	"github.com/Master290/kite/internal/stream"
	"github.com/Master290/kite/internal/web"
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"net/http/pprof"
)

var ErrRestartRequired = errors.New("restart required for this configuration change")

type Server struct {
	log       *slog.Logger
	store     *ConfigStore
	hub       *stream.Hub
	metrics   *Metrics
	tls       *certificateProvider
	startedAt time.Time
	authMu    sync.Mutex
	auth      *authLimiter

	httpServer      *http.Server
	httpsServer     *http.Server
	adminServer     *http.Server
	challengeServer *http.Server
	http3Server     *http3.Server
	listeners       []net.Listener
	packetConns     []net.PacketConn
	wg              sync.WaitGroup
	ready           atomic.Bool
	shutdownOnce    sync.Once
	shutdownErr     error
}

func New(cfg *config.Config, configPath string, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	metrics := NewMetrics()
	store := NewConfigStore(configPath, cfg)
	metrics.SetRevision(store.Revision())
	if err := validateFallbackFiles(cfg); err != nil {
		return nil, err
	}
	var tlsProvider *certificateProvider
	var err error
	if cfg.Server.HTTPSAddress != "" {
		tlsProvider, err = newCertificateProvider(cfg.TLS, metrics)
		if err != nil {
			return nil, err
		}
	}
	hub, err := stream.NewHubWithLogger(cfg, metrics, logger)
	if err != nil {
		return nil, err
	}
	s := &Server{log: logger, store: store, hub: hub, metrics: metrics, tls: tlsProvider, startedAt: time.Now(), auth: newAuthLimiter()}
	return s, nil
}

func (s *Server) Config() *config.Config { return s.store.Current() }

func (s *Server) Start(ctx context.Context) (startErr error) {
	cfg := s.Config()
	defer func() {
		if startErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration())
			defer cancel()
			_ = s.Shutdown(cleanupCtx)
		}
	}()
	publicMux := http.NewServeMux()
	s.registerPublic(publicMux)
	if cfg.Server.HTTPAddress != "" {
		ln, err := net.Listen("tcp", cfg.Server.HTTPAddress)
		if err != nil {
			return err
		}
		s.listeners = append(s.listeners, ln)
		ln = legacyListener{ln}
		s.httpServer = &http.Server{Handler: s.altSvc(publicMux), ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration(), IdleTimeout: cfg.Server.IdleTimeout.Duration(), MaxHeaderBytes: cfg.Server.MaxHeaderBytes}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.ready.Store(false)
				s.log.Error("http server stopped", "error", err)
			}
		}()
	}
	if cfg.Server.HTTPSAddress != "" {
		ln, err := net.Listen("tcp", cfg.Server.HTTPSAddress)
		if err != nil {
			return err
		}
		s.listeners = append(s.listeners, ln)
		https := &http.Server{Handler: s.altSvc(publicMux), ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration(), IdleTimeout: cfg.Server.IdleTimeout.Duration(), MaxHeaderBytes: cfg.Server.MaxHeaderBytes}
		https.TLSConfig = s.tls.TLSConfig()
		https.HTTP2 = s.http2Config()
		s.httpsServer = https
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := https.ServeTLS(ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.ready.Store(false)
				s.log.Error("https server stopped", "error", err)
			}
		}()
	}
	if cfg.TLS.Mode == "acme" && cfg.TLS.HTTPChallengeAddress != "" && s.tls.manager != nil {
		handler := s.tls.manager.HTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://"+r.Host+r.URL.RequestURI(), http.StatusPermanentRedirect)
		}))
		s.challengeServer = &http.Server{Addr: cfg.TLS.HTTPChallengeAddress, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
		challengeListener, err := net.Listen("tcp", cfg.TLS.HTTPChallengeAddress)
		if err != nil {
			return err
		}
		s.listeners = append(s.listeners, challengeListener)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.challengeServer.Serve(challengeListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error("acme challenge server stopped", "error", err)
			}
		}()
	}
	if cfg.Server.HTTP3Address != "" {
		udpAddress, err := net.ResolveUDPAddr("udp", cfg.Server.HTTP3Address)
		if err != nil {
			return err
		}
		packetConn, err := net.ListenUDP("udp", udpAddress)
		if err != nil {
			return err
		}
		s.packetConns = append(s.packetConns, packetConn)
		h3Mux := http.NewServeMux()
		s.registerPublic(h3Mux)
		s.http3Server = &http3.Server{
			Handler:        h3Mux,
			TLSConfig:      s.tls.TLSConfig(),
			QUICConfig:     s.quicConfig(),
			IdleTimeout:    cfg.Server.IdleTimeout.Duration(),
			MaxHeaderBytes: cfg.Server.MaxHeaderBytes,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.http3Server.Serve(packetConn); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				s.ready.Store(false)
				s.log.Error("http3 server stopped", "error", err)
			}
		}()
	}
	adminMux := http.NewServeMux()
	s.registerAdmin(adminMux)
	adminLn, err := net.Listen("tcp", cfg.Admin.Address)
	if err != nil {
		return err
	}
	s.listeners = append(s.listeners, adminLn)
	s.adminServer = &http.Server{Handler: adminMux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 64 << 10}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.adminServer.Serve(adminLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.ready.Store(false)
			s.log.Error("admin server stopped", "error", err)
		}
	}()
	s.ready.Store(true)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration())
		defer cancel()
		_ = s.Shutdown(shutdownCtx)
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.ready.Store(false)
		s.hub.Close()
		var errs []error
		for _, httpServer := range []*http.Server{s.httpServer, s.httpsServer, s.adminServer, s.challengeServer} {
			if httpServer == nil {
				continue
			}
			if err := httpServer.Shutdown(ctx); err != nil {
				errs = append(errs, err)
				_ = httpServer.Close()
			}
		}
		if s.http3Server != nil {
			if err := s.http3Server.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		for _, packetConn := range s.packetConns {
			if err := packetConn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		s.wg.Wait()
		s.shutdownErr = errors.Join(errs...)
	})
	return s.shutdownErr
}

func (s *Server) registerPublic(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleListener)
	mux.HandleFunc("/_kite/v1/events", s.handleSSE)
	mux.HandleFunc("/_kite/v1/ws", s.handleWebSocket)
	mux.HandleFunc("/_kite/v1/playlist.m3u", s.handlePlaylist)
	mux.HandleFunc("/status-json.xsl", s.handleStatus)
	mux.HandleFunc("/admin/metadata", s.handleMetadata)
	s.registerIcecastAdmin(mux)
}

func (s *Server) registerAdmin(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.HandlerFor(s.metrics.Registry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("/api/v1/config", s.adminAuth(s.handleConfig))
	mux.HandleFunc("/api/v1/config/validate", s.adminAuth(s.handleValidate))
	mux.HandleFunc("/api/v1/reload", s.adminAuth(s.handleReload))
	mux.HandleFunc("/api/v1/mounts", s.adminAuth(s.handleMounts))
	mux.HandleFunc("/api/v1/source", s.adminAuth(s.handleSourceAction))
	s.registerIcecastAdmin(mux)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	if os.Getenv("KITE_PPROF") == "1" {
		registerPprof(mux, s.adminAuth)
	}
}

// registerPprof attaches Go runtime profiling handlers behind the admin token.
func registerPprof(mux *http.ServeMux, auth func(http.HandlerFunc) http.HandlerFunc) {
	authed := func(h http.HandlerFunc) http.HandlerFunc {
		return auth(func(w http.ResponseWriter, r *http.Request) { h(w, r) })
	}
	mux.HandleFunc("/debug/pprof/", authed(pprof.Index))
	mux.HandleFunc("/debug/pprof/cmdline", authed(pprof.Cmdline))
	mux.HandleFunc("/debug/pprof/profile", authed(pprof.Profile))
	mux.HandleFunc("/debug/pprof/symbol", authed(pprof.Symbol))
	mux.HandleFunc("/debug/pprof/trace", authed(pprof.Trace))
}

func (s *Server) handleListener(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/_") || strings.HasPrefix(r.URL.Path, "/admin/") || r.URL.Path == "/status-json.xsl" {
		http.NotFound(w, r)
		return
	}
	if s.routeHLS(w, r, r.URL.Path) {
		return
	}
	if s.routeMountPlaylist(w, r, r.URL.Path) {
		return
	}
	m, ok := s.hub.Get(r.URL.Path)
	if !ok {
		if (r.URL.Path == "/" || r.URL.Path == "/index.html" || r.URL.Path == "/status.xsl") && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			s.handleStatusPage(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodPut || r.Method == http.MethodPost || r.Method == "SOURCE" {
		s.handleSource(w, r, m)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	meta := m.StreamMetadata()
	setCORS(w, m.Config().CORSOrigins, r.Header.Get("Origin"))
	w.Header().Set("Content-Type", m.Config().ContentType)
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("icy-name", meta.Name)
	w.Header().Set("icy-description", meta.Description)
	w.Header().Set("icy-genre", meta.Genre)
	w.Header().Set("icy-url", meta.URL)
	w.Header().Set("icy-br", strconv.Itoa(meta.Bitrate))
	w.Header().Set("icy-pub", boolToZeroOne(meta.Public))
	w.Header().Set("Access-Control-Expose-Headers", "icy-name, icy-description, icy-genre, icy-url, icy-br, icy-pub")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Header.Get("Icy-MetaData") == "1" {
		s.streamICY(w, r, m)
		return
	}
	sub := m.SubscribeContext(s.listenerContext(r), listenerTransport(r))
	writer := newIntervalWriter(w, s.Config().Defaults.WriteInterval.Duration(), s.listenerWriteTimeout(m))
	defer writer.Flush()
	if err := writer.Flush(); err != nil {
		if isDeadlineError(err) {
			sub.Close("write_timeout")
		} else {
			sub.Close("client_closed")
		}
		return
	}
	writer.last = time.Time{}
	_ = stream.Copy(r.Context(), writer, sub)
}

func (s *Server) listenerContext(r *http.Request) context.Context {
	return stream.WithListenerClientInfo(r.Context(), s.requestClientIP(r), r.Header.Get("User-Agent"))
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

func (s *Server) handleSource(w http.ResponseWriter, r *http.Request, m *stream.Mount) {
	ok, locked, retryAfter := s.authenticateSource(r, m.Config().Source)
	if locked {
		writeAuthLockout(w, retryAfter)
		return
	}
	if !ok {
		w.Header().Set("WWW-Authenticate", `Basic realm="kite"`)
		http.Error(w, "source authentication failed", http.StatusUnauthorized)
		return
	}
	if err := m.AcquireSource(); err != nil {
		s.log.Warn("source connection rejected: mount busy", "mount", m.Config().Path, "remote_ip", s.requestClientIP(r), "method", r.Method)
		http.Error(w, "source already connected", http.StatusForbidden)
		return
	}
	defer m.ReleaseSource()
	m.SetSourceStreamMetadata(s.parseSourceStreamMetadata(r))
	s.log.Info("source connected", "mount", m.Config().Path, "remote_ip", s.requestClientIP(r), "method", r.Method, "proto", r.Proto, "content_length", r.ContentLength, "transfer_encoding", r.TransferEncoding, "expect", r.Header.Get("Expect"))
	if r.ProtoMajor == 1 && r.ContentLength == 0 && len(r.TransferEncoding) == 0 {
		s.handleRawSource(w, r, m)
		return
	}
	rc := http.NewResponseController(w)
	var forceClosed atomic.Bool
	m.SetSourceCloser(closerFunc(func() error {
		forceClosed.Store(true)
		if err := rc.SetReadDeadline(time.Now()); err != nil {
			go func() { _ = r.Body.Close() }()
		}
		return nil
	}))
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, m.Config().ContentType) && m.Config().Profile != "opaque" {
		s.log.Warn("source content type differs", "expected", m.Config().ContentType, "actual", ct)
	}
	err := stream.Pump(m.Config().Profile, r.Body, m.Write)
	if forceClosed.Load() || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		s.log.Info("source disconnected", "mount", m.Config().Path, "reason", err)
		w.WriteHeader(http.StatusOK)
		return
	}
	if !errors.Is(err, context.Canceled) {
		s.log.Warn("source stream ended", "mount", m.Config().Path, "error", err)
		http.Error(w, "invalid source stream", http.StatusBadRequest)
	}
}

func (s *Server) handleRawSource(w http.ResponseWriter, r *http.Request, m *stream.Mount) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "raw source transport unavailable", http.StatusHTTPVersionNotSupported)
		return
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		s.log.Warn("source hijack failed", "mount", m.Config().Path, "error", err)
		return
	}
	defer conn.Close()
	m.SetSourceCloser(conn)
	if _, err := rw.WriteString("HTTP/1.0 200 OK\r\nConnection: close\r\n\r\n"); err != nil {
		return
	}
	if err := rw.Flush(); err != nil {
		return
	}
	timeout := m.Config().SourceTimeout.Duration()
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout * 2))
	writeFn := func(b []byte) error {
		_ = conn.SetReadDeadline(time.Now().Add(timeout * 2))
		return m.Write(b)
	}
	err = stream.Pump(m.Config().Profile, rw.Reader, writeFn)
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) && !os.IsTimeout(err) {
		s.log.Warn("raw source stream ended", "mount", m.Config().Path, "error", err)
	} else {
		s.log.Info("source disconnected", "mount", m.Config().Path, "reason", err)
	}
}

func (s *Server) streamICY(w http.ResponseWriter, r *http.Request, m *stream.Mount) {
	metaInt := m.Config().ICYMetaInterval
	w.Header().Set("icy-metaint", strconv.Itoa(metaInt))
	sub := m.SubscribeContext(s.listenerContext(r), "http-icy")
	defer sub.Close("client_closed")
	writer := newIntervalWriter(w, s.Config().Defaults.WriteInterval.Duration(), s.listenerWriteTimeout(m))
	defer writer.Flush()
	if err := writer.Flush(); err != nil {
		if isDeadlineError(err) {
			sub.Close("write_timeout")
		}
		return
	}
	writer.last = time.Time{}
	inBurst := sub.HasBuffered()
	sent := 0
	var lastSentTitle string
	var hasSentTitle bool
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		var (
			chunk stream.Chunk
			err   error
		)
		if !inBurst && !sub.HasBuffered() && writer.Buffered() > 0 {
			rem := writer.RemainingInterval()
			if rem <= 0 {
				if err := writer.Flush(); err != nil {
					if isDeadlineError(err) {
						sub.Close("write_timeout")
					}
					return
				}
				chunk, err = sub.Next(r.Context())
			} else {
				if timer == nil {
					timer = time.NewTimer(rem)
				} else {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(rem)
				}
				var timedOut bool
				chunk, timedOut, err = sub.NextOrTimeout(r.Context(), timer.C)
				if err == nil && timedOut {
					if err := writer.Flush(); err != nil {
						if isDeadlineError(err) {
							sub.Close("write_timeout")
						}
						return
					}
					chunk, err = sub.Next(r.Context())
				}
			}
		} else {
			chunk, err = sub.Next(r.Context())
		}
		if err != nil {
			return
		}
		data := chunk.Data
		for len(data) > 0 {
			need := metaInt - sent
			if len(data) < need {
				n, err := writer.Write(data)
				sub.RecordWrite(n)
				if err != nil {
					if isDeadlineError(err) {
						sub.Close("write_timeout")
					}
					return
				}
				sent += len(data)
				data = nil
				continue
			}
			n, err := writer.Write(data[:need])
			sub.RecordWrite(n)
			if err != nil {
				if isDeadlineError(err) {
					sub.Close("write_timeout")
				}
				return
			}
			data = data[need:]
			sent = 0
			curTitle := m.Metadata().StreamTitle()
			var block []byte
			if hasSentTitle && curTitle == lastSentTitle {
				block = []byte{0}
			} else {
				block = stream.FormatICYMetadataBlock(curTitle)
				lastSentTitle = curTitle
				hasSentTitle = true
			}
			if _, err := writer.Write(block); err != nil {
				if isDeadlineError(err) {
					sub.Close("write_timeout")
				}
				return
			}
		}
		if inBurst && !sub.HasBuffered() {
			inBurst = false
			if err := writer.Flush(); err != nil {
				if isDeadlineError(err) {
					sub.Close("write_timeout")
				}
				return
			}
		}
	}
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	m, ok := s.hub.Get(r.URL.Query().Get("mount"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	setCORS(w, m.Config().CORSOrigins, r.Header.Get("Origin"))
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	if _, ok := w.(http.Flusher); !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	rc := http.NewResponseController(w)
	timeout := s.listenerWriteTimeout(m)
	lastID, _ := strconv.ParseUint(r.Header.Get("Last-Event-ID"), 10, 64)
	ch, cancel := m.SubscribeEvents(lastID)
	defer cancel()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	_ = rc.SetWriteDeadline(time.Now().Add(timeout))
	if err := rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return
	}
	_ = rc.SetWriteDeadline(time.Time{})
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			_ = rc.SetWriteDeadline(time.Now().Add(timeout))
			if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.ID, ev.Type, b); err != nil {
				return
			}
			_ = rc.SetWriteDeadline(time.Now().Add(timeout))
			if err := rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
				return
			}
			_ = rc.SetWriteDeadline(time.Time{})
		case <-ticker.C:
			_ = rc.SetWriteDeadline(time.Now().Add(timeout))
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			_ = rc.SetWriteDeadline(time.Now().Add(timeout))
			if err := rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
				return
			}
			_ = rc.SetWriteDeadline(time.Time{})
		}
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	m, ok := s.hub.Get(r.URL.Query().Get("mount"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	if !originAllowed(r.Header.Get("Origin"), m.Config().CORSOrigins) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	setCORS(w, m.Config().CORSOrigins, r.Header.Get("Origin"))
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer c.CloseNow()
	ctx, wsCancel := context.WithCancel(r.Context())
	defer wsCancel()
	sub := m.SubscribeContext(s.listenerContext(r), "websocket")
	defer sub.Close("client_closed")
	events, cancel := m.SubscribeEvents(0)
	defer cancel()
	writeWS := func(typ websocket.MessageType, data []byte) error {
		writeCtx, cancelWrite := context.WithTimeout(ctx, s.listenerWriteTimeout(m))
		defer cancelWrite()
		err := c.Write(writeCtx, typ, data)
		if err != nil {
			if isDeadlineError(err) {
				sub.Close("write_timeout")
			}
			_ = c.CloseNow()
		}
		return err
	}
	hello, _ := json.Marshal(map[string]any{"type": "hello", "mount": m.Config().Path, "profile": m.Config().Profile, "content_type": m.Config().ContentType})
	if err := writeWS(websocket.MessageText, hello); err != nil {
		return
	}
	type audioResult struct {
		chunk stream.Chunk
		err   error
	}
	audio := make(chan audioResult, 1)
	go func() {
		for {
			chunk, err := sub.Next(ctx)
			select {
			case audio <- audioResult{chunk: chunk, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			if err := writeWS(websocket.MessageText, b); err != nil {
				return
			}
		case result := <-audio:
			if result.err != nil {
				if errors.Is(result.err, stream.ErrSlowListener) {
					timer := time.AfterFunc(s.listenerWriteTimeout(m), func() { _ = c.CloseNow() })
					_ = c.Close(websocket.StatusTryAgainLater, "listener fell behind")
					timer.Stop()
				}
				return
			}
			if err := writeWS(websocket.MessageBinary, result.chunk.Data); err != nil {
				return
			}
			sub.RecordWrite(len(result.chunk.Data))
		}
	}
}

func (s *Server) handlePlaylist(w http.ResponseWriter, r *http.Request) {
	mount := r.URL.Query().Get("mount")
	if mount == "" {
		mount = "/radio"
	}
	if _, ok := s.hub.Get(mount); !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "audio/x-mpegurl")
	base := s.requestBaseURL(r)
	fmt.Fprintf(w, "#EXTM3U\n#EXTINF:-1,Kite\n%s%s\n", base, mount)
}
func (s *Server) handleStatusPage(w http.ResponseWriter, r *http.Request) {
	if !s.Config().Server.StatusPage() {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(web.StatusHTML)
}

func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request) {
	mount := r.URL.Query().Get("mount")
	if mount == "" {
		mount = r.URL.Path
	}
	m, ok := s.hub.Get(mount)
	if !ok {
		http.NotFound(w, r)
		return
	}
	user, _, hasBasic := r.BasicAuth()
	var authOK, locked bool
	var retryAfter time.Duration
	if hasBasic && user == s.icecastAdminUser() && user != m.Config().Source.Username && s.icecastAdminPassword() != "" {
		authOK, locked, retryAfter = s.authenticateBasic(r, "icecast_admin", s.icecastAdminUser(), s.icecastAdminPassword())
	} else {
		authOK, locked, retryAfter = s.authenticateSource(r, m.Config().Source)
	}
	if locked {
		writeAuthLockout(w, retryAfter)
		return
	}
	if !authOK {
		w.Header().Set("WWW-Authenticate", `Basic realm="kite"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	urlVal := r.FormValue("url")
	if song := r.FormValue("song"); song != "" {
		m.SetMetadata(stream.Metadata{Title: song, URL: urlVal})
	} else {
		m.SetMetadata(stream.Metadata{
			Artist: r.FormValue("artist"),
			Song:   r.FormValue("title"),
			URL:    urlVal,
		})
	}
	writeXML(w, http.StatusOK, xmlIceResponse{
		Message: "Metadata update successful",
		Return:  1,
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		w.Header().Set("ETag", s.store.ETag())
		writeJSON(w, 200, redactedConfig(s.Config()))
		return
	}
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if match := r.Header.Get("If-Match"); match != "" && match != s.store.ETag() {
		http.Error(w, "etag mismatch", http.StatusPreconditionFailed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	next, err := s.store.Parse(body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s.restoreRedactedSecrets(next)
	rev, err := s.store.Commit(next, validateFallbackFiles, s.applyConfig)
	if errors.Is(err, ErrRestartRequired) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.metrics.SetRevision(rev)
	w.Header().Set("ETag", s.store.ETag())
	writeJSON(w, 200, map[string]any{"revision": rev})
}
func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	next, err := s.store.Parse(body)
	if err != nil {
		writeJSON(w, 400, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"valid": true, "paths": next.Paths()})
}
func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	next, err := config.Load(s.store.path)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	rev, err := s.store.Commit(next, validateFallbackFiles, s.applyConfig)
	if errors.Is(err, ErrRestartRequired) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.metrics.SetRevision(rev)
	writeJSON(w, 200, map[string]any{"revision": rev})
}
func (s *Server) handleMounts(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.hub.Status())
}
func (s *Server) handleSourceAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	m, ok := s.hub.Get(r.URL.Query().Get("mount"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	if !m.DisconnectSource() {
		http.Error(w, "source is not connected", http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
func (s *Server) applyConfig(next *config.Config) { _ = s.hub.Apply(next) }

func validateFallbackFiles(cfg *config.Config) error {
	for _, mount := range cfg.Mounts {
		for _, fallback := range mount.Fallback {
			if fallback.File != "" {
				if err := stream.ValidateFile(mount.Profile, fallback.File); err != nil {
					return fmt.Errorf("mount %s fallback file %s: %w", mount.Path, fallback.File, err)
				}
			}
			if fallback.Folder != "" {
				if err := stream.ValidateFolder(mount.Profile, fallback.Folder); err != nil {
					return fmt.Errorf("mount %s fallback folder %s: %w", mount.Path, fallback.Folder, err)
				}
			}
			if fallback.Playlist != "" {
				if err := stream.ValidatePlaylist(mount.Profile, fallback.Playlist); err != nil {
					return fmt.Errorf("mount %s fallback playlist %s: %w", mount.Path, fallback.Playlist, err)
				}
			}
		}
	}
	return nil
}

func (s *Server) restoreRedactedSecrets(next *config.Config) {
	current := s.Config()
	if next.Admin.IcecastAdminPassword == "<redacted>" {
		next.Admin.IcecastAdminPassword = current.Admin.IcecastAdminPassword
	}
	if next.Server.IcecastAdminPassword == "<redacted>" {
		next.Server.IcecastAdminPassword = current.Server.IcecastAdminPassword
	}
	for i := range next.Mounts {
		if next.Mounts[i].Source.PasswordBcrypt == "<redacted>" {
			if old, ok := current.Mount(next.Mounts[i].Path); ok {
				next.Mounts[i].Source.PasswordBcrypt = old.Source.PasswordBcrypt
			}
		}
		if next.Mounts[i].Relay != nil && next.Mounts[i].Relay.Password == "<redacted>" {
			if old, ok := current.Mount(next.Mounts[i].Path); ok && old.Relay != nil {
				next.Mounts[i].Relay.Password = old.Relay.Password
			}
		}
	}
}

func redactedConfig(current *config.Config) *config.Config {
	copyConfig := *current
	if copyConfig.Admin.IcecastAdminPassword != "" {
		copyConfig.Admin.IcecastAdminPassword = "<redacted>"
	}
	if copyConfig.Server.IcecastAdminPassword != "" {
		copyConfig.Server.IcecastAdminPassword = "<redacted>"
	}
	copyConfig.Mounts = append([]config.Mount(nil), current.Mounts...)
	for i := range copyConfig.Mounts {
		if copyConfig.Mounts[i].Source.PasswordBcrypt != "" {
			copyConfig.Mounts[i].Source.PasswordBcrypt = "<redacted>"
		}
		if copyConfig.Mounts[i].Relay != nil {
			relayCopy := *copyConfig.Mounts[i].Relay
			if relayCopy.Password != "" {
				relayCopy.Password = "<redacted>"
			}
			copyConfig.Mounts[i].Relay = &relayCopy
		}
	}
	return &copyConfig
}

func (s *Server) altSvc(next http.Handler) http.Handler {
	port := s.Config().Server.PublicHTTPSPort
	if port == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor < 3 {
			w.Header().Set("Alt-Svc", fmt.Sprintf(`h3=":%d"; ma=86400`, port))
		}
		next.ServeHTTP(w, r)
	})
}

func parseBearerToken(header string) (string, bool) {
	const prefix = "bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	return header[len(prefix):], true
}

func (s *Server) adminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := s.adminToken()
		if token == "" {
			http.Error(w, "admin token is not configured", http.StatusServiceUnavailable)
			return
		}
		ip := s.requestClientIP(r)
		limiter := s.getAuthLimiter()
		if isLocked, wait, failures := limiter.checkLocked(ip, "admin_api"); isLocked {
			s.log.Warn("authentication rejected: IP locked out", "scope", "admin_api", "ip", ip, "token", "[REDACTED]", "failures", failures, "retry_after", wait.Round(time.Millisecond).String())
			writeAuthLockout(w, wait)
			return
		}
		rawAuth := r.Header.Get("Authorization")
		if strings.TrimSpace(rawAuth) == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		got, hasBearer := parseBearerToken(rawAuth)
		if !hasBearer || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			failures, lockDelay := limiter.recordFailure(ip, "admin_api")
			s.log.Warn("authentication failed", "scope", "admin_api", "ip", ip, "token", "[REDACTED]", "failures", failures, "lockout", lockDelay.Round(time.Millisecond).String(), "retry_after", lockDelay.Round(time.Millisecond).String())
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		limiter.recordSuccess(ip, "admin_api")
		next(w, r)
	}
}
func (s *Server) adminToken() string {
	c := s.Config().Admin
	if c.TokenEnv != "" {
		return os.Getenv(c.TokenEnv)
	}
	if c.TokenFile != "" {
		b, _ := os.ReadFile(c.TokenFile)
		return strings.TrimSpace(string(b))
	}
	return ""
}
func (s *Server) authenticateSource(r *http.Request, cred config.SourceCredential) (ok bool, locked bool, retryAfter time.Duration) {
	expected := cred.PasswordBcrypt
	if cred.PasswordEnv != "" {
		if b, err := config.ResolveSecret(cred.PasswordEnv, ""); err == nil {
			expected = string(b)
		} else {
			expected = ""
		}
	}
	if cred.PasswordFile != "" {
		if b, err := config.ResolveSecret("", cred.PasswordFile); err == nil {
			expected = string(b)
		} else {
			expected = ""
		}
	}
	return s.authenticateBasic(r, "source", cred.Username, expected)
}

func (s *Server) checkSourceAuth(r *http.Request, cred config.SourceCredential) bool {
	ok, _, _ := s.authenticateSource(r, cred)
	return ok
}

func setCORS(w http.ResponseWriter, origins []string, origin string) {
	for _, o := range origins {
		if o == "*" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			return
		}
		if origin != "" && o == origin {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
			return
		}
	}
}

func originAllowed(origin string, origins []string) bool {
	if origin == "" {
		return true
	}
	for _, o := range origins {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func listenerTransport(r *http.Request) string {
	switch r.ProtoMajor {
	case 2:
		return "http2"
	case 3:
		return "http3"
	default:
		return "http"
	}
}

func (s *Server) listenerWriteTimeout(m *stream.Mount) time.Duration {
	if m != nil {
		if d := m.Config().ListenerWriteTimeout.Duration(); d > 0 {
			return d
		}
	}
	if cfg := s.Config(); cfg != nil {
		if d := cfg.Defaults.ListenerWriteTimeout.Duration(); d > 0 {
			return d
		}
		if d := cfg.Server.ListenerWriteTimeout.Duration(); d > 0 {
			return d
		}
	}
	return 10 * time.Second
}

// http2Config returns connection-level HTTP/2 settings using the global listener_write_timeout
// configured at startup (per-mount overrides and hot reload do not apply to connection-level timeouts).
func (s *Server) http2Config() *http.HTTP2Config {
	writeTimeout := s.listenerWriteTimeout(nil)
	return &http.HTTP2Config{
		WriteByteTimeout: writeTimeout,
		SendPingTimeout:  30 * time.Second,
		PingTimeout:      15 * time.Second,
	}
}

// quicConfig returns connection-level QUIC settings using the global listener_write_timeout
// configured at startup (per-mount overrides and hot reload do not apply to connection-level timeouts).
func (s *Server) quicConfig() *quic.Config {
	writeTimeout := s.listenerWriteTimeout(nil)
	idleTimeout := writeTimeout * 3
	if cfg := s.Config(); cfg != nil && cfg.Server.IdleTimeout.Duration() > 0 {
		idleTimeout = cfg.Server.IdleTimeout.Duration()
	}
	return &quic.Config{
		Allow0RTT:       true,
		MaxIdleTimeout:  idleTimeout,
		KeepAlivePeriod: writeTimeout,
	}
}

func isDeadlineError(err error) bool {
	return stream.IsTimeoutError(err)
}

const intervalWriteBatchSize = 4096

type intervalWriter struct {
	writer   io.Writer
	flusher  http.Flusher
	rc       *http.ResponseController
	interval time.Duration
	timeout  time.Duration
	last     time.Time
	buf      []byte
	err      error
}

func newIntervalWriter(w http.ResponseWriter, interval, timeout time.Duration) *intervalWriter {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	flusher, _ := w.(http.Flusher)
	return &intervalWriter{
		writer:   w,
		flusher:  flusher,
		rc:       http.NewResponseController(w),
		interval: interval,
		timeout:  timeout,
		buf:      make([]byte, 0, intervalWriteBatchSize*2),
	}
}
func (w *intervalWriter) Buffered() int { return len(w.buf) }
func (w *intervalWriter) RemainingInterval() time.Duration {
	if w.interval <= 0 || len(w.buf) == 0 {
		return 0
	}
	if w.last.IsZero() {
		return w.interval
	}
	rem := w.interval - time.Since(w.last)
	if rem < 0 {
		return 0
	}
	return rem
}
func (w *intervalWriter) setWriteDeadline() time.Time {
	if w.rc != nil && w.timeout > 0 {
		deadline := time.Now().Add(w.timeout)
		_ = w.rc.SetWriteDeadline(deadline)
		return deadline
	}
	return time.Time{}
}
func (w *intervalWriter) clearWriteDeadline() {
	if w.rc != nil && w.timeout > 0 {
		_ = w.rc.SetWriteDeadline(time.Time{})
	}
}
func normalizeWriteErr(err error, deadline time.Time) error {
	if err != nil && !isDeadlineError(err) && !deadline.IsZero() && !time.Now().Before(deadline) {
		return fmt.Errorf("%w: %w", err, os.ErrDeadlineExceeded)
	}
	return err
}
func (w *intervalWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if w.interval <= 0 {
		deadline := w.setWriteDeadline()
		n, err := w.writer.Write(p)
		if err != nil {
			err = normalizeWriteErr(err, deadline)
			w.err = err
			return n, err
		}
		if err := w.Flush(); err != nil {
			return n, err
		}
		return n, nil
	}
	if len(w.buf) == 0 && w.last.IsZero() {
		w.last = time.Now()
	}
	w.buf = append(w.buf, p...)
	if len(w.buf) >= intervalWriteBatchSize || (!w.last.IsZero() && time.Since(w.last) >= w.interval) {
		if err := w.Flush(); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}
func (w *intervalWriter) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.buf) > 0 {
		deadline := w.setWriteDeadline()
		_, err := w.writer.Write(w.buf)
		w.buf = w.buf[:0]
		if err != nil {
			err = normalizeWriteErr(err, deadline)
			w.err = err
			return err
		}
	}
	if w.flusher == nil {
		w.clearWriteDeadline()
		w.last = time.Now()
		return nil
	}
	deadline := w.setWriteDeadline()
	if w.rc != nil {
		err := w.rc.Flush()
		w.last = time.Now()
		if err != nil && !errors.Is(err, http.ErrNotSupported) {
			err = normalizeWriteErr(err, deadline)
			w.err = err
			return err
		}
		w.clearWriteDeadline()
		return nil
	}
	w.flusher.Flush()
	w.clearWriteDeadline()
	w.last = time.Now()
	return nil
}

func BasicAuthHeader(user, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
}
