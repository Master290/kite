package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Master290/kite/internal/config"
)

func TestDemoRouteServesEmbeddedPlayer(t *testing.T) {
	s, ts := publicServer(t)
	resp, err := http.Get(ts.URL + "/demo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("content-type=%q", ct)
	}
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])
	if !strings.Contains(body, "<audio") || !strings.Contains(body, "EventSource") {
		t.Fatalf("demo page missing player markup: %q", body)
	}
	for _, st := range s.hub.Status() {
		if st.Listeners != 0 {
			t.Fatalf("demo page created listeners: %+v", st)
		}
	}
}

func TestDemoRouteCanBeDisabled(t *testing.T) {
	s, _ := publicServer(t, func(c *config.Config) {
		disabled := false
		c.Server.DemoEnabled = &disabled
	})
	req := httptest.NewRequest(http.MethodGet, "http://radio.example/demo", nil)
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	s.registerPublic(mux)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestPprofDisabledByDefault(t *testing.T) {
	_, ts := apiServer(t)
	resp, err := http.Get(ts.URL + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestPprofEnabledRequiresAdminToken(t *testing.T) {
	t.Setenv("KITE_PPROF", "1")
	_, ts := apiServer(t)

	resp, err := http.Get(ts.URL + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", resp.StatusCode)
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/debug/pprof/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer admin")
	okResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer okResp.Body.Close()
	if okResp.StatusCode != http.StatusOK {
		t.Fatalf("authenticated status=%d", okResp.StatusCode)
	}
	buf := make([]byte, 2048)
	n, _ := okResp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "goroutine") {
		t.Fatalf("pprof index unexpected body %q", buf[:n])
	}
}
