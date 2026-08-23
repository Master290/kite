package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestParseNormalizesMount(t *testing.T) {
	t.Setenv("KITE_SOURCE_PASSWORD", "secret")
	cfg, err := Parse([]byte(`
version: 1
server:
  http_address: "127.0.0.1:8000"
admin:
  address: "127.0.0.1:9090"
tls:
  mode: development
mounts:
  - path: /radio
    profile: mp3
    source:
      password_env: KITE_SOURCE_PASSWORD
`), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := cfg.Mounts[0]
	if m.ContentType != "audio/mpeg" {
		t.Fatalf("content type=%q", m.ContentType)
	}
	if m.Source.Username != "source" {
		t.Fatalf("username=%q", m.Source.Username)
	}
	if m.SourceTimeout.Duration() != 3*time.Second {
		t.Fatalf("source timeout=%s", m.SourceTimeout.Duration())
	}
}

func TestParseRejectsFallbackCycle(t *testing.T) {
	_, err := Parse([]byte(`
version: 1
server: {http_address: "127.0.0.1:8000"}
tls: {mode: development}
mounts:
  - path: /a
    profile: mp3
    source: {password_env: A_PASSWORD}
    fallback: [{mount: /b}]
  - path: /b
    profile: mp3
    source: {password_env: B_PASSWORD}
    fallback: [{mount: /a}]
`), t.TempDir())
	if err == nil {
		t.Fatal("expected fallback cycle error")
	}
}

func TestRelativeSecretFilesResolveFromConfig(t *testing.T) {
	dir := t.TempDir()
	cfg, err := Parse([]byte(`
version: 1
server: {http_address: "127.0.0.1:8000"}
admin: {address: "127.0.0.1:9090", token_file: secrets/admin}
tls: {mode: development}
mounts:
  - path: /radio
    profile: mp3
    source: {password_file: secrets/source}
`), dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Admin.TokenFile != filepath.Join(dir, "secrets", "admin") {
		t.Fatalf("admin file=%q", cfg.Admin.TokenFile)
	}
	if cfg.Mounts[0].Source.PasswordFile != filepath.Join(dir, "secrets", "source") {
		t.Fatalf("source file=%q", cfg.Mounts[0].Source.PasswordFile)
	}
}
