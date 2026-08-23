package server

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Master290/kite/internal/config"
)

func TestFFmpegStyleZeroLengthSourceBody(t *testing.T) {
	t.Setenv("SOURCE_PASSWORD", "secret")
	cfg, err := config.Parse([]byte(`
version: 1
server: {http_address: "127.0.0.1:8000"}
admin: {address: "127.0.0.1:9090", token_env: ADMIN_TOKEN}
tls: {mode: development}
mounts:
  - path: /radio
    profile: mp3
    source: {password_env: SOURCE_PASSWORD}
`), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cfg, t.TempDir()+"/kite.yaml", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.hub.Close()
	mux := http.NewServeMux()
	s.registerPublic(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	u, _ := url.Parse(ts.URL)
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	request := fmt.Sprintf("PUT /radio HTTP/1.1\r\nHost: %s\r\nAuthorization: %s\r\nContent-Type: audio/mpeg\r\nContent-Length: 0\r\nExpect: 100-continue\r\n\r\n", u.Host, BasicAuthHeader("source", "secret"))
	if _, err = conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "200 OK") {
		t.Fatalf("status=%q", status)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	if _, err = conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		st := s.hub.Status()[0]
		if st.BytesIn == uint64(len(frame)) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("audio was not ingested: %+v", s.hub.Status()[0])
}
