package server

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Master290/kite/internal/config"
)

func TestHeadReturnsWithoutCreatingListener(t *testing.T) {
	s, _ := apiServer(t)
	mux := http.NewServeMux()
	s.registerPublic(mux)
	req := httptest.NewRequest(http.MethodHead, "http://radio.example/radio", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if got := s.hub.Status()[0].Listeners; got != 0 {
		t.Fatalf("listeners=%d", got)
	}
}

func TestPlaylistUsesAbsoluteStreamURL(t *testing.T) {
	s, _ := apiServer(t)
	req := httptest.NewRequest(http.MethodGet, "https://radio.example/_kite/v1/playlist.m3u?mount=/radio", nil)
	rec := httptest.NewRecorder()
	s.handlePlaylist(rec, req)
	if !strings.Contains(rec.Body.String(), "https://radio.example/radio") {
		t.Fatalf("playlist=%q", rec.Body.String())
	}
}

func TestMissingSourceSecretCannotAuthenticate(t *testing.T) {
	s, _ := apiServer(t)
	req := httptest.NewRequest(http.MethodPut, "http://radio.example/radio", nil)
	req.SetBasicAuth("source", "")
	if s.checkSourceAuth(req, config.SourceCredential{Username: "source", PasswordEnv: "UNSET_SOURCE_PASSWORD"}) {
		t.Fatal("missing secret authenticated")
	}
}

func TestReadyEndpointReflectsServerState(t *testing.T) {
	s, _ := apiServer(t)
	mux := http.NewServeMux()
	s.registerAdmin(mux)
	req := httptest.NewRequest(http.MethodGet, "http://admin/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rec.Code)
	}
	s.ready.Store(true)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestConcurrentShutdownWaitsForSameResult(t *testing.T) {
	s, _ := apiServer(t)
	s.ready.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 2)
	go func() { done <- s.Shutdown(ctx) }()
	go func() { done <- s.Shutdown(ctx) }()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if s.ready.Load() {
		t.Fatal("server remained ready")
	}
}

func TestStartCleansUpWhenAdminBindFails(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	t.Setenv("SOURCE_PASSWORD", "source")
	cfg, err := config.Parse([]byte("version: 1\nserver: {http_address: \"127.0.0.1:0\"}\nadmin: {address: \""+occupied.Addr().String()+"\"}\ntls: {mode: development}\nmounts:\n  - path: /radio\n    profile: mp3\n    source: {password_env: SOURCE_PASSWORD}\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cfg, t.TempDir()+"/kite.yaml", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("expected bind failure")
	}
	if len(s.listeners) == 0 {
		t.Fatal("expected public listener to be staged")
	}
	address := s.listeners[0].Addr().String()
	conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Fatalf("listener %s remained open", address)
	}
}
