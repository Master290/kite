package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Master290/kite/internal/config"
)

func apiServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	t.Setenv("ADMIN_TOKEN", "admin")
	t.Setenv("SOURCE_PASSWORD", "source")
	cfg, err := config.Parse([]byte(`version: 1
server: {http_address: "127.0.0.1:8000"}
admin: {address: "127.0.0.1:9090", token_env: ADMIN_TOKEN}
tls: {mode: development}
mounts:
  - path: /radio
    profile: mp3
    source: {password_env: SOURCE_PASSWORD}
    metadata: {public: true}
`), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cfg, t.TempDir()+"/kite.yaml", nil)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.registerAdmin(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() { ts.Close(); s.Shutdown(context.Background()) })
	return s, ts
}

func TestAdminConfigRedactsAndUsesETag(t *testing.T) {
	s, ts := apiServer(t)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/config", nil)
	req.Header.Set("Authorization", "Bearer admin")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if resp.Header.Get("ETag") == "" {
		t.Fatal("missing etag")
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	mounts := body["mounts"].([]any)
	source := mounts[0].(map[string]any)["source"].(map[string]any)
	if _, ok := source["password_env"]; !ok {
		t.Fatal("password env missing")
	}
	if source["password_env"] != "SOURCE_PASSWORD" {
		t.Fatalf("unexpected secret=%v", source)
	}
	_ = s
}

func TestAdminValidateRejectsUnknownFields(t *testing.T) {
	_, ts := apiServer(t)
	body := strings.NewReader("version: 1\nunknown: true\n")
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/config/validate", body)
	req.Header.Set("Authorization", "Bearer admin")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestAdminCommitDoesNotActivateOnPersistenceFailure(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "admin")
	cfg := config.Default()
	cfg.Server.HTTPAddress = ":8000"
	cfg.Server.HTTPSAddress = ""
	cfg.Server.HTTP3Address = ""
	cfg.Mounts = nil
	s := NewConfigStore(t.TempDir(), &cfg)
	prepared, activated := 0, 0
	next := cfg
	next.Admin.TokenEnv = "OTHER"
	_, err := s.Commit(&next, func(*config.Config) error { prepared++; return nil }, func(*config.Config) { activated++ })
	if err == nil {
		t.Fatal("expected persistence failure")
	}
	if prepared != 1 || activated != 0 {
		t.Fatalf("prepared=%d activated=%d", prepared, activated)
	}
}

func TestAdminCommitDoesNotPersistOnPrepareFailure(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/kite.yaml"
	cfg := config.Default()
	cfg.Server.HTTPAddress = ":8000"
	cfg.Server.HTTPSAddress = ""
	cfg.Server.HTTP3Address = ""
	original := []byte("original\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewConfigStore(path, &cfg)
	next := cfg
	next.Admin.TokenEnv = "OTHER"
	activated := false
	_, err := store.Commit(&next, func(*config.Config) error { return errors.New("invalid fallback") }, func(*config.Config) { activated = true })
	if err == nil {
		t.Fatal("expected prepare failure")
	}
	if activated {
		t.Fatal("config activated")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(original) {
		t.Fatalf("file changed to %q", got)
	}
}

func TestAdminConfigRedactsRelayPassword(t *testing.T) {
	cfg := config.Default()
	cfg.Mounts = []config.Mount{
		{
			Path:    "/relayed",
			Profile: "mp3",
			Relay: &config.RelayConfig{
				URL:      "http://example.com/stream",
				Username: "user",
				Password: "supersecretpassword",
			},
		},
	}
	s := &Server{store: NewConfigStore("", &cfg)}

	redacted := redactedConfig(&cfg)
	if redacted.Mounts[0].Relay.Password != "<redacted>" {
		t.Fatalf("expected <redacted>, got %q", redacted.Mounts[0].Relay.Password)
	}
	if cfg.Mounts[0].Relay.Password != "supersecretpassword" {
		t.Fatalf("original password mutated: %q", cfg.Mounts[0].Relay.Password)
	}

	incoming := *redacted
	s.restoreRedactedSecrets(&incoming)
	if incoming.Mounts[0].Relay.Password != "supersecretpassword" {
		t.Fatalf("expected restored password, got %q", incoming.Mounts[0].Relay.Password)
	}
}

