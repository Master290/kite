package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Master290/kite/internal/config"
)

func TestStatusPageServedOnRootAndAliases(t *testing.T) {
	_, ts := publicServer(t)

	paths := []string{"/", "/index.html", "/status.xsl"}
	for _, p := range paths {
		resp, err := http.Get(ts.URL + p)
		if err != nil {
			t.Fatalf("GET %s failed: %v", p, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s status=%d, want %d", p, resp.StatusCode, http.StatusOK)
		}
		ct := resp.Header.Get("Content-Type")
		if !strings.HasPrefix(ct, "text/html") {
			t.Errorf("GET %s content-type=%q, want text/html", p, ct)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("reading body: %v", err)
		}
		if !strings.Contains(string(body), "Kite") {
			t.Errorf("GET %s body does not contain 'Kite'", p)
		}
	}
}

func TestStatusPageHeadRequest(t *testing.T) {
	_, ts := publicServer(t)

	resp, err := http.Head(ts.URL + "/")
	if err != nil {
		t.Fatalf("HEAD / failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Fatalf("expected empty body for HEAD, got %d bytes", len(body))
	}
}

func TestStatusPageCanBeDisabled(t *testing.T) {
	disabled := false
	_, ts := publicServer(t, func(cfg *config.Config) {
		cfg.Server.StatusPageEnabled = &disabled
	})

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d, want 404 when status page is disabled", resp.StatusCode)
	}
}
