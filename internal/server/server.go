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
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/http2"
)

var ErrRestartRequired = errors.New("restart required for this configuration change")

type Server struct {
	log     *slog.Logger
	store   *ConfigStore
	hub     *stream.Hub
	metrics *Metrics
	tls     *certificateProvider

	httpServer      *http.Server
	httpsServer     *http.Server
	adminServer     *http.Server
	challengeServer *http.Server
	http3Server     *http3.Server
	listeners       []net.Listener
	wg              sync.WaitGroup
	closed          atomic.Bool
}

func New(cfg *config.Config, configPath string, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	metrics := NewMetrics()
	store := NewConfigStore(configPath, cfg)
	metrics.SetRevision(store.Revision())
	var tlsProvider *certificateProvider
	var err error
	if cfg.Server.HTTPSAddress != "" {
		tlsProvider, err = newCertificateProvider(cfg.TLS, metrics)
		if err != nil {
			return nil, err
		}
	}
	hub, err := stream.NewHub(cfg, metrics)
	if err != nil {
		return nil, err
	}
	s := &Server{log: logger, store: store, hub: hub, metrics: metrics, tls: tlsProvider}
	return s, nil
}

func (s *Server) Config() *config.Config { return s.store.Current() }

func (s *Server) Start(ctx context.Context) error {
	cfg := s.Config()
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
		if err := http2.ConfigureServer(https, &http2.Server{}); err != nil {
			return err
		}
		s.httpsServer = https
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := https.ServeTLS(ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error("https server stopped", "error", err)
			}
		}()
	}
	if cfg.TLS.Mode == "acme" && cfg.TLS.HTTPChallengeAddress != "" && s.tls.manager != nil {
		handler := s.tls.manager.HTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://"+r.Host+r.URL.RequestURI(), http.StatusPermanentRedirect)
		}))
		s.challengeServer = &http.Server{Addr: cfg.TLS.HTTPChallengeAddress, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.challengeServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error("acme challenge server stopped", "error", err)
			}
		}()
	}
	if cfg.Server.HTTP3Address != "" {
		h3Mux := http.NewServeMux()
		s.registerPublic(h3Mux)
		s.http3Server = &http3.Server{Addr: cfg.Server.HTTP3Address, Handler: h3Mux, TLSConfig: s.tls.TLSConfig()}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.http3Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
			s.log.Error("admin server stopped", "error", err)
		}
	}()
	go func() { <-ctx.Done(); _ = s.Shutdown(context.Background()) }()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.hub.Close()
	var errs []error
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if s.httpsServer != nil {
		if err := s.httpsServer.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if s.adminServer != nil {
		if err := s.adminServer.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if s.challengeServer != nil {
		if err := s.challengeServer.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if s.http3Server != nil {
		if err := s.http3Server.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.wg.Wait()
	return errors.Join(errs...)
}

func (s *Server) registerPublic(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleListener)
	mux.HandleFunc("/_kite/v1/events", s.handleSSE)
	mux.HandleFunc("/_kite/v1/ws", s.handleWebSocket)
	mux.HandleFunc("/_kite/v1/playlist.m3u", s.handlePlaylist)
	mux.HandleFunc("/status-json.xsl", s.handleStatus)
	mux.HandleFunc("/admin/metadata", s.handleMetadata)
}

func (s *Server) registerAdmin(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.HandlerFor(s.metrics.Registry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("/api/v1/config", s.adminAuth(s.handleConfig))
	mux.HandleFunc("/api/v1/config/validate", s.adminAuth(s.handleValidate))
	mux.HandleFunc("/api/v1/reload", s.adminAuth(s.handleReload))
	mux.HandleFunc("/api/v1/mounts", s.adminAuth(s.handleMounts))
	mux.HandleFunc("/api/v1/source", s.adminAuth(s.handleSourceAction))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
}

func (s *Server) handleListener(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/_") || strings.HasPrefix(r.URL.Path, "/admin/") || r.URL.Path == "/status-json.xsl" {
		http.NotFound(w, r)
		return
	}
	m, ok := s.hub.Get(r.URL.Path)
	if !ok {
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
	setCORS(w, m.Config().CORSOrigins, r.Header.Get("Origin"))
	w.Header().Set("Content-Type", m.Config().ContentType)
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("icy-name", m.Config().Metadata.Name)
	w.Header().Set("icy-description", m.Config().Metadata.Description)
	w.Header().Set("icy-genre", m.Config().Metadata.Genre)
	w.Header().Set("icy-url", m.Config().Metadata.URL)
	w.Header().Set("icy-br", strconv.Itoa(m.Config().Metadata.Bitrate))
	w.Header().Set("Access-Control-Expose-Headers", "icy-name, icy-description, icy-genre, icy-url, icy-br")
	if r.Header.Get("Icy-MetaData") == "1" {
		s.streamICY(w, r, m)
		return
	}
	sub := m.Subscribe("http")
	stream.Copy(r.Context(), w, sub, func(n int) { s.metrics.BytesOut(m.Config().Path, "http", n) })
}

func (s *Server) handleSource(w http.ResponseWriter, r *http.Request, m *stream.Mount) {
	if !s.checkSourceAuth(r, m.Config().Source) {
		http.Error(w, "source authentication failed", http.StatusUnauthorized)
		return
	}
	if err := m.AcquireSource(); err != nil {
		http.Error(w, "source already connected", http.StatusConflict)
		return
	}
	defer m.ReleaseSource()
	s.log.Info("source connected", "mount", m.Config().Path, "method", r.Method, "proto", r.Proto, "content_length", r.ContentLength, "transfer_encoding", r.TransferEncoding, "expect", r.Header.Get("Expect"))
	if r.ProtoMajor == 1 && r.ContentLength == 0 && len(r.TransferEncoding) == 0 {
		s.handleRawSource(w, r, m)
		return
	}
	m.SetSourceCloser(r.Body)
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, m.Config().ContentType) && m.Config().Profile != "opaque" {
		s.log.Warn("source content type differs", "expected", m.Config().ContentType, "actual", ct)
	}
	err := stream.Pump(m.Config().Profile, r.Body, m.Write)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		s.log.Info("source disconnected", "mount", m.Config().Path, "reason", err)
		w.WriteHeader(http.StatusOK)
		return
	}
	if err != nil && !errors.Is(err, context.Canceled) {
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
	err = stream.Pump(m.Config().Profile, rw.Reader, m.Write)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
		s.log.Warn("raw source stream ended", "mount", m.Config().Path, "error", err)
	} else {
		s.log.Info("source disconnected", "mount", m.Config().Path, "reason", err)
	}
}

func (s *Server) streamICY(w http.ResponseWriter, r *http.Request, m *stream.Mount) {
	metaInt := m.Config().ICYMetaInterval
	w.Header().Set("icy-metaint", strconv.Itoa(metaInt))
	w.Header().Set("Transfer-Encoding", "chunked")
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	sub := m.Subscribe("http-icy")
	defer sub.Close("client_closed")
	meta := ""
	sent := 0
	for {
		select {
		case <-r.Context().Done():
			return
		case chunk, ok := <-sub.C:
			if !ok {
				return
			}
			data := chunk.Data
			for len(data) > 0 {
				need := metaInt - sent
				if len(data) < need {
					_, _ = w.Write(data)
					s.metrics.BytesOut(m.Config().Path, "http-icy", len(data))
					sent += len(data)
					data = nil
					continue
				}
				_, _ = w.Write(data[:need])
				s.metrics.BytesOut(m.Config().Path, "http-icy", need)
				data = data[need:]
				sent = 0
				meta = "StreamTitle='" + strings.ReplaceAll(m.Metadata().Title, "'", "\\'") + "';"
				if len(meta) > 4080 {
					meta = meta[:4080]
				}
				block := []byte{byte((len(meta) + 16 - 1) / 16)}
				padded := append([]byte(meta), make([]byte, int(block[0])*16-len(meta))...)
				block = append(block, padded...)
				_, _ = w.Write(block)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
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
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	lastID, _ := strconv.ParseUint(r.Header.Get("Last-Event-ID"), 10, 64)
	ch, cancel := m.SubscribeEvents(lastID)
	defer cancel()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.ID, ev.Type, b)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprint(w, ": heartbeat\n\n")
			flusher.Flush()
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
	defer c.Close(websocket.StatusNormalClosure, "")
	ctx := r.Context()
	sub := m.Subscribe("websocket")
	events, cancel := m.SubscribeEvents(0)
	defer cancel()
	hello, _ := json.Marshal(map[string]any{"type": "hello", "mount": m.Config().Path, "profile": m.Config().Profile, "content_type": m.Config().ContentType})
	_ = c.Write(ctx, websocket.MessageText, hello)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			if err := c.Write(ctx, websocket.MessageText, b); err != nil {
				return
			}
		case chunk, ok := <-sub.C:
			if !ok {
				return
			}
			if err := c.Write(ctx, websocket.MessageBinary, chunk.Data); err != nil {
				return
			}
			s.metrics.BytesOut(m.Config().Path, "websocket", len(chunk.Data))
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
	fmt.Fprintf(w, "#EXTM3U\n#EXTINF:-1,Kite\n%s\n", mount)
}
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	items := make([]stream.Status, 0)
	for _, st := range s.hub.Status() {
		if m, ok := s.Config().Mount(st.Path); ok && !m.Hidden && m.Metadata.Public {
			items = append(items, st)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"icestats": map[string]any{"source": items}})
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
	if !s.checkSourceAuth(r, m.Config().Source) {
		http.Error(w, "unauthorized", 401)
		return
	}
	title := r.FormValue("song")
	if title == "" {
		title = r.FormValue("title")
	}
	m.SetMetadata(stream.Metadata{Title: title, URL: r.FormValue("url")})
	writeJSON(w, 200, map[string]any{"ok": true, "metadata": m.Metadata()})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		w.Header().Set("ETag", s.store.ETag())
		writeJSON(w, 200, redactedConfig(s.Config()))
		return
	}
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", 405)
		return
	}
	if match := r.Header.Get("If-Match"); match != "" && match != s.store.ETag() {
		http.Error(w, "etag mismatch", 412)
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
	rev, err := s.store.Commit(next, s.applyConfig)
	if errors.Is(err, ErrRestartRequired) {
		http.Error(w, err.Error(), 409)
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
	rev, err := s.store.Commit(next, s.applyConfig)
	if errors.Is(err, ErrRestartRequired) {
		http.Error(w, err.Error(), 409)
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
func (s *Server) applyConfig(next *config.Config) error { return s.hub.Apply(next) }

func (s *Server) restoreRedactedSecrets(next *config.Config) {
	current := s.Config()
	for i := range next.Mounts {
		if next.Mounts[i].Source.PasswordBcrypt != "<redacted>" {
			continue
		}
		if old, ok := current.Mount(next.Mounts[i].Path); ok {
			next.Mounts[i].Source.PasswordBcrypt = old.Source.PasswordBcrypt
		}
	}
}

func redactedConfig(current *config.Config) *config.Config {
	copyConfig := *current
	copyConfig.Mounts = append([]config.Mount(nil), current.Mounts...)
	for i := range copyConfig.Mounts {
		if copyConfig.Mounts[i].Source.PasswordBcrypt != "" {
			copyConfig.Mounts[i].Source.PasswordBcrypt = "<redacted>"
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

func (s *Server) adminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := s.adminToken()
		if token == "" {
			http.Error(w, "admin token is not configured", 503)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
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
func (s *Server) checkSourceAuth(r *http.Request, cred config.SourceCredential) bool {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	if user != cred.Username {
		return false
	}
	expected := cred.PasswordBcrypt
	if cred.PasswordEnv != "" {
		b, _ := config.ResolveSecret(cred.PasswordEnv, "")
		expected = string(b)
	}
	if cred.PasswordFile != "" {
		b, _ := config.ResolveSecret("", cred.PasswordFile)
		expected = string(b)
	}
	if strings.HasPrefix(expected, "$2") {
		return bcrypt.CompareHashAndPassword([]byte(expected), []byte(pass)) == nil
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(pass)) == 1
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

func BasicAuthHeader(user, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
}
