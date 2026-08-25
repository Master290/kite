package server

import (
	"net/http"
	"strings"
	"testing"
)

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
