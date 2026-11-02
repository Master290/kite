package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Master290/kite/internal/config"
	"github.com/Master290/kite/internal/stream"
	"github.com/coder/websocket"
)

// publicServer wires a Server with two mounts behind an httptest.Server:
// /radio is public and listed in Icecast status, /private is hidden.
// Optional mutators adjust the config before the server is built.
func publicServer(t *testing.T, mutators ...func(*config.Config)) (*Server, *httptest.Server) {
	t.Helper()
	t.Setenv("SOURCE_PASSWORD", "source")
	source := config.SourceCredential{Username: "source", PasswordEnv: "SOURCE_PASSWORD"}
	cfg := &config.Config{
		Defaults: config.Defaults{
			SourceTimeout:    config.Duration(250 * time.Millisecond),
			FailbackDelay:    config.Duration(250 * time.Millisecond),
			BufferDuration:   config.Duration(250 * time.Millisecond),
			WriteInterval:    0,
			ICYMetaInterval:  256,
			MaxSourceBitrate: 128000,
		},
		Mounts: []config.Mount{
			{Path: "/radio", Profile: "mp3", ContentType: "audio/mpeg", Source: source, ICYMetaInterval: 256, Metadata: config.Metadata{Name: "Kite Test", Public: true}, CORSOrigins: []string{"*"}},
			{Path: "/private", Profile: "mp3", ContentType: "audio/mpeg", Source: source, Metadata: config.Metadata{Name: "Hidden", Public: false}, Hidden: true},
		},
	}
	for _, mutate := range mutators {
		mutate(cfg)
	}
	s, err := New(cfg, t.TempDir()+"/kite.yaml", nil)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.registerPublic(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() { ts.Close(); s.Shutdown(context.Background()) })
	return s, ts
}

func waitForListeners(t *testing.T, s *Server, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, st := range s.hub.Status() {
			if st.Path == path && st.Listeners >= want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener on %s not registered: %+v", path, s.hub.Status())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func acquireTestSource(t *testing.T, m *stream.Mount) {
	t.Helper()
	if err := m.AcquireSource(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.ReleaseSource)
}

type sseMessage struct {
	id    string
	event string
	data  string
}

func readSSEMessages(r io.Reader, out chan<- sseMessage) {
	scanner := bufio.NewScanner(r)
	msg := sseMessage{}
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			msg.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			msg.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			msg.data = strings.TrimPrefix(line, "data: ")
		case line == "":
			if msg.id != "" || msg.event != "" || msg.data != "" {
				out <- msg
			}
			msg = sseMessage{}
		}
	}
}

func expectSSE(t *testing.T, msgs <-chan sseMessage) sseMessage {
	t.Helper()
	select {
	case msg := <-msgs:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE message")
		return sseMessage{}
	}
}

func TestSSEReplaysInitialStateAndDeliversMetadata(t *testing.T) {
	s, ts := publicServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/_kite/v1/events?mount=/radio", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content-type=%q", got)
	}
	msgs := make(chan sseMessage, 16)
	go readSSEMessages(resp.Body, msgs)

	first := expectSSE(t, msgs)
	if first.id != "1" || first.event != "source" {
		t.Fatalf("first=%+v", first)
	}
	second := expectSSE(t, msgs)
	if second.id != "2" || second.event != "metadata" {
		t.Fatalf("second=%+v", second)
	}

	m, _ := s.hub.Get("/radio")
	m.SetMetadata(stream.Metadata{Title: "Live Title", URL: "https://example.com"})
	third := expectSSE(t, msgs)
	if third.event != "metadata" || !strings.Contains(third.data, "Live Title") {
		t.Fatalf("third=%+v", third)
	}
	if third.id != "3" {
		t.Fatalf("live event id=%s, want 3", third.id)
	}
}

func TestSSEResumesAfterLastEventID(t *testing.T) {
	s, ts := publicServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/_kite/v1/events?mount=/radio", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Last-Event-ID", "2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	msgs := make(chan sseMessage, 16)
	go readSSEMessages(resp.Body, msgs)

	select {
	case msg := <-msgs:
		t.Fatalf("unexpected replayed event %+v", msg)
	case <-time.After(100 * time.Millisecond):
	}

	m, _ := s.hub.Get("/radio")
	m.SetMetadata(stream.Metadata{Title: "Resumed"})
	msg := expectSSE(t, msgs)
	if msg.id != "3" || msg.event != "metadata" || !strings.Contains(msg.data, "Resumed") {
		t.Fatalf("resumed=%+v", msg)
	}
}

func TestSSEReturns404ForUnknownMount(t *testing.T) {
	_, ts := publicServer(t)
	resp, err := http.Get(ts.URL + "/_kite/v1/events?mount=/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestWebSocketDeliversHelloAudioAndEvents(t *testing.T) {
	s, ts := publicServer(t)
	m, _ := s.hub.Get("/radio")
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/_kite/v1/ws?mount=/radio"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()

	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("hello type=%v", typ)
	}
	var hello map[string]any
	if err := json.Unmarshal(data, &hello); err != nil {
		t.Fatal(err)
	}
	if hello["type"] != "hello" || hello["mount"] != "/radio" || hello["profile"] != "mp3" {
		t.Fatalf("hello=%v", hello)
	}

	acquireTestSource(t, m)
	m.SetMetadata(stream.Metadata{Title: "WS Live"})
	frame := []byte("ws-audio-frame")
	if err := m.Write(frame); err != nil {
		t.Fatal(err)
	}

	sawAudio, sawEvent := false, false
	for !sawAudio || !sawEvent {
		typ, data, err = c.Read(ctx)
		if err != nil {
			t.Fatalf("read after audio=%v events=%v: %v", sawAudio, sawEvent, err)
		}
		switch typ {
		case websocket.MessageBinary:
			if !bytes.Equal(data, frame) {
				t.Fatalf("binary payload=%q", data)
			}
			sawAudio = true
		case websocket.MessageText:
			var ev map[string]any
			if err := json.Unmarshal(data, &ev); err != nil {
				t.Fatal(err)
			}
			if ev["type"] == "metadata" && strings.Contains(string(data), "WS Live") {
				sawEvent = true
			}
		}
	}
}

func TestWebSocketRejectsUnknownMount(t *testing.T) {
	_, ts := publicServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http")+"/_kite/v1/ws?mount=/missing", nil)
	if err == nil {
		t.Fatal("expected dial failure")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%v", resp)
	}
}

func TestStreamICYEmitsMetadataBlocks(t *testing.T) {
	s, ts := publicServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/radio", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Icy-MetaData", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("icy-metaint"); got != "256" {
		t.Fatalf("icy-metaint=%q", got)
	}
	if got := resp.Header.Get("Content-Type"); got != "audio/mpeg" {
		t.Fatalf("content-type=%q", got)
	}
	waitForListeners(t, s, "/radio", 1)

	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)
	m.SetMetadata(stream.Metadata{Title: "It's a Test"})
	payload := bytes.Repeat([]byte{0xAA}, 256)
	if err := m.Write(payload); err != nil {
		t.Fatal(err)
	}

	buf := readBodyBytes(t, resp.Body, len(payload)+1+32, 3*time.Second)
	if !bytes.Equal(buf[:len(payload)], payload) {
		t.Fatal("audio passthrough corrupted before metadata block")
	}
	blockLen := int(buf[len(payload)])
	if blockLen == 0 {
		t.Fatal("empty metadata block")
	}
	meta := buf[len(payload)+1 : len(payload)+1+blockLen*16]
	want := "StreamTitle='It\\'s a Test';"
	if !bytes.Contains(meta, []byte(want)) {
		t.Fatalf("metadata block %q missing %q", meta, want)
	}
	if pad := meta[len(want):]; bytes.IndexFunc(pad, func(r rune) bool { return r != 0 }) != -1 {
		t.Fatalf("nonzero padding %q", pad)
	}
}

func TestStreamICYTruncatesHugeTitles(t *testing.T) {
	s, ts := publicServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/radio", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Icy-MetaData", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	waitForListeners(t, s, "/radio", 1)

	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)
	m.SetMetadata(stream.Metadata{Title: strings.Repeat("x", 5000)})
	if err := m.Write(bytes.Repeat([]byte{0xBB}, 256)); err != nil {
		t.Fatal(err)
	}

	buf := readBodyBytes(t, resp.Body, 256+1+4080, 3*time.Second)
	blockLen := int(buf[256])
	if blockLen != 255 {
		t.Fatalf("block length byte=%d, want 255", blockLen)
	}
	meta := buf[257 : 257+blockLen*16]
	if len(meta) != 4080 {
		t.Fatalf("meta length=%d", len(meta))
	}
	if !bytes.HasPrefix(meta, []byte("StreamTitle='")) {
		t.Fatalf("meta prefix=%q", meta[:20])
	}
}

// readBodyBytes accumulates n bytes from body or fails after timeout.
func readBodyBytes(t *testing.T, body io.Reader, n int, timeout time.Duration) []byte {
	t.Helper()
	buf := make([]byte, 0, n+64)
	tmp := make([]byte, 512)
	type result struct {
		n   int
		err error
	}
	filled := make(chan struct{})
	go func() {
		defer close(filled)
		for len(buf) < n {
			read := make(chan result, 1)
			go func() {
				k, err := body.Read(tmp)
				read <- result{k, err}
			}()
			select {
			case r := <-read:
				if r.err != nil && r.n == 0 {
					return
				}
				buf = append(buf, tmp[:r.n]...)
			case <-time.After(timeout):
				return
			}
		}
	}()
	select {
	case <-filled:
	case <-time.After(timeout + time.Second):
	}
	if len(buf) < n {
		t.Fatalf("received %d bytes, want %d (buf tail=%q)", len(buf), n, buf)
	}
	return buf
}

func TestStatusJSONFiltersHiddenMounts(t *testing.T) {
	s, ts := publicServer(t)
	waitForListeners(t, s, "/radio", 0)
	resp, err := http.Get(ts.URL + "/status-json.xsl")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var doc struct {
		Icestats struct {
			Source []struct {
				Path      string `json:"path"`
				Listeners int    `json:"listeners"`
			} `json:"source"`
		} `json:"icestats"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Icestats.Source) != 1 {
		t.Fatalf("sources=%+v", doc.Icestats.Source)
	}
	if doc.Icestats.Source[0].Path != "/radio" {
		t.Fatalf("path=%q", doc.Icestats.Source[0].Path)
	}
}

func TestAdminMetadataRequiresAuthAndUpdatesMount(t *testing.T) {
	s, ts := publicServer(t)

	resp, err := http.Get(ts.URL + "/admin/metadata?mount=/radio&mode=updinfo&song=No%20Auth")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); !strings.HasPrefix(got, `Basic realm=`) {
		t.Fatalf("www-authenticate=%q", got)
	}

	sourceReq, err := http.NewRequest(http.MethodPut, ts.URL+"/radio", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	sourceResp, err := http.DefaultClient.Do(sourceReq)
	if err != nil {
		t.Fatal(err)
	}
	sourceResp.Body.Close()
	if sourceResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("source status=%d", sourceResp.StatusCode)
	}
	if got := sourceResp.Header.Get("WWW-Authenticate"); !strings.HasPrefix(got, `Basic realm=`) {
		t.Fatalf("source www-authenticate=%q", got)
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/admin/metadata?mount=/missing&song=X", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp404, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp404.Body.Close()
	if resp404.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown mount status=%d", resp404.StatusCode)
	}

	req, err = http.NewRequest(http.MethodGet, ts.URL+"/admin/metadata?mount=/radio&mode=updinfo&song=Artist%20-%20Title&url=https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("source", "source")
	okResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer okResp.Body.Close()
	if okResp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", okResp.StatusCode)
	}
	var body struct {
		OK       bool `json:"ok"`
		Metadata struct {
			Title string `json:"title"`
			URL   string `json:"url"`
		} `json:"metadata"`
	}
	if err := json.NewDecoder(okResp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if !body.OK || body.Metadata.Title != "Artist - Title" || body.Metadata.URL != "https://example.com" {
		t.Fatalf("body=%+v", body)
	}
	m, _ := s.hub.Get("/radio")
	if got := m.Metadata(); got.Title != "Artist - Title" {
		t.Fatalf("hub metadata=%+v", got)
	}
}

func metricsOutput(t *testing.T, s *Server) string {
	t.Helper()
	mux := http.NewServeMux()
	s.registerAdmin(mux)
	req := httptest.NewRequest(http.MethodGet, "http://admin/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status=%d", rec.Code)
	}
	return rec.Body.String()
}

func TestStalledHTTPListenerDisconnectedWithinWriteTimeout(t *testing.T) {
	writeTimeout := 100 * time.Millisecond
	s, ts := publicServer(t, func(cfg *config.Config) {
		cfg.Defaults.ListenerWriteTimeout = config.Duration(writeTimeout)
		cfg.Defaults.BufferDuration = config.Duration(10 * time.Second)
		cfg.Defaults.MaxSourceBitrate = 64_000_000
	})
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/radio", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	waitForListeners(t, s, "/radio", 1)

	initial := bytes.Repeat([]byte{0xAA}, 1024)
	if err := m.Write(initial); err != nil {
		t.Fatal(err)
	}
	_ = readBodyBytes(t, resp.Body, len(initial), 2*time.Second)

	// Client stops reading from resp.Body; keep writing chunks until the socket buffer fills
	// and the write deadline fires.
	stopPump := make(chan struct{})
	defer close(stopPump)
	chunk := bytes.Repeat([]byte{0xBB}, 64<<10)
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPump:
				return
			case <-ticker.C:
				_ = m.Write(chunk)
			}
		}
	}()

	deadline := time.Now().Add(writeTimeout * 10)
	for time.Now().Before(deadline) {
		if m.Status().Listeners == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.Status().Listeners; got != 0 {
		t.Fatalf("stalled HTTP listener was not removed within timeout: listeners=%d", got)
	}
	metrics := metricsOutput(t, s)
	if !strings.Contains(metrics, `kite_listeners{mount="/radio",transport="http"} 0`) {
		t.Fatalf("expected kite_listeners gauge to be 0, got:\n%s", metrics)
	}
	if !strings.Contains(metrics, `kite_listener_disconnects_total{mount="/radio",reason="write_timeout",transport="http"} 1`) {
		t.Fatalf("expected write_timeout disconnect metric, got:\n%s", metrics)
	}
}

func TestStalledICYListenerDisconnectedWithinWriteTimeout(t *testing.T) {
	writeTimeout := 100 * time.Millisecond
	s, ts := publicServer(t, func(cfg *config.Config) {
		cfg.Defaults.ListenerWriteTimeout = config.Duration(writeTimeout)
		cfg.Defaults.BufferDuration = config.Duration(10 * time.Second)
		cfg.Defaults.MaxSourceBitrate = 64_000_000
		cfg.Mounts[0].ICYMetaInterval = 16000
	})
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/radio", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Icy-MetaData", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	waitForListeners(t, s, "/radio", 1)

	stopPump := make(chan struct{})
	defer close(stopPump)
	chunk := bytes.Repeat([]byte{0xDD}, 64<<10)
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPump:
				return
			case <-ticker.C:
				_ = m.Write(chunk)
			}
		}
	}()

	deadline := time.Now().Add(writeTimeout * 10)
	for time.Now().Before(deadline) {
		if m.Status().Listeners == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.Status().Listeners; got != 0 {
		t.Fatalf("stalled ICY listener was not removed within timeout: listeners=%d", got)
	}
	metrics := metricsOutput(t, s)
	if !strings.Contains(metrics, `kite_listeners{mount="/radio",transport="http-icy"} 0`) {
		t.Fatalf("expected kite_listeners http-icy gauge to be 0, got:\n%s", metrics)
	}
	if !strings.Contains(metrics, `kite_listener_disconnects_total{mount="/radio",reason="write_timeout",transport="http-icy"} 1`) {
		t.Fatalf("expected http-icy write_timeout disconnect metric, got:\n%s", metrics)
	}
}

func TestStalledWebSocketListenerDisconnectedWithinWriteTimeout(t *testing.T) {
	writeTimeout := 100 * time.Millisecond
	s, ts := publicServer(t, func(cfg *config.Config) {
		cfg.Defaults.ListenerWriteTimeout = config.Duration(writeTimeout)
		cfg.Defaults.BufferDuration = config.Duration(10 * time.Second)
		cfg.Defaults.MaxSourceBitrate = 64_000_000
	})
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/_kite/v1/ws?mount=/radio"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()

	// Read hello message, then stop reading from the WebSocket connection.
	if _, _, err := c.Read(ctx); err != nil {
		t.Fatal(err)
	}
	waitForListeners(t, s, "/radio", 1)

	stopPump := make(chan struct{})
	defer close(stopPump)
	chunk := bytes.Repeat([]byte{0xCC}, 64<<10)
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPump:
				return
			case <-ticker.C:
				_ = m.Write(chunk)
			}
		}
	}()

	deadline := time.Now().Add(writeTimeout * 10)
	for time.Now().Before(deadline) {
		if m.Status().Listeners == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.Status().Listeners; got != 0 {
		t.Fatalf("stalled WebSocket listener was not removed within timeout: listeners=%d", got)
	}
	metrics := metricsOutput(t, s)
	if !strings.Contains(metrics, `kite_listeners{mount="/radio",transport="websocket"} 0`) {
		t.Fatalf("expected kite_listeners websocket gauge to be 0, got:\n%s", metrics)
	}
	if !strings.Contains(metrics, `kite_listener_disconnects_total{mount="/radio",reason="write_timeout",transport="websocket"} 1`) {
		t.Fatalf("expected websocket write_timeout disconnect metric, got:\n%s", metrics)
	}
}

func TestHealthyListenerSurvivesMultipleWriteTimeoutsAndIdleGaps(t *testing.T) {
	writeTimeout := 50 * time.Millisecond
	s, _ := publicServer(t, func(cfg *config.Config) {
		cfg.Defaults.ListenerWriteTimeout = config.Duration(writeTimeout)
	})
	mux := http.NewServeMux()
	s.registerPublic(mux)
	ts := httptest.NewUnstartedServer(mux)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/radio", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	waitForListeners(t, s, "/radio", 1)

	chunk := []byte("healthy-audio-frame")
	for i := 0; i < 4; i++ {
		if err := m.Write(chunk); err != nil {
			t.Fatal(err)
		}
		got := readBodyBytes(t, resp.Body, len(chunk), time.Second)
		if !bytes.Equal(got, chunk) {
			t.Fatalf("chunk %d=%q", i, got)
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Pause longer than 2x writeTimeout (120ms > 50ms) and verify the listener is not dropped.
	time.Sleep(120 * time.Millisecond)
	if err := m.Write(chunk); err != nil {
		t.Fatal(err)
	}
	got := readBodyBytes(t, resp.Body, len(chunk), time.Second)
	if !bytes.Equal(got, chunk) {
		t.Fatalf("post-idle chunk=%q", got)
	}
	if m.Status().Listeners != 1 {
		t.Fatalf("healthy listener dropped: listeners=%d", m.Status().Listeners)
	}
}

type noDeadlineResponseWriter struct {
	header  http.Header
	written bytes.Buffer
	flushed int
}

func (w *noDeadlineResponseWriter) Header() http.Header         { return w.header }
func (w *noDeadlineResponseWriter) WriteHeader(int)             {}
func (w *noDeadlineResponseWriter) Write(p []byte) (int, error) { return w.written.Write(p) }
func (w *noDeadlineResponseWriter) Flush()                      { w.flushed++ }

func TestListenerIgnoresErrNotSupportedFromSetWriteDeadline(t *testing.T) {
	rw := &noDeadlineResponseWriter{header: make(http.Header)}
	rc := http.NewResponseController(rw)
	if err := rc.SetWriteDeadline(time.Now().Add(time.Second)); err == nil {
		t.Fatal("expected SetWriteDeadline to return ErrNotSupported on noDeadlineResponseWriter")
	}

	iw := newIntervalWriter(rw, 0, 50*time.Millisecond)
	payload := []byte("transport-without-deadline")
	n, err := iw.Write(payload)
	if err != nil {
		t.Fatalf("unexpected error when SetWriteDeadline is unsupported: %v", err)
	}
	if n != len(payload) || !bytes.Equal(rw.written.Bytes(), payload) {
		t.Fatalf("written=%q", rw.written.Bytes())
	}
	if rw.flushed == 0 {
		t.Fatal("expected Flush to be called")
	}
}

type failingDeadlineWriter struct {
	header  http.Header
	once    sync.Once
	entered chan struct{}
	block   chan struct{}
}

func (w *failingDeadlineWriter) Header() http.Header { return w.header }
func (w *failingDeadlineWriter) WriteHeader(int)     {}
func (w *failingDeadlineWriter) Write(p []byte) (int, error) {
	if w.entered != nil {
		w.once.Do(func() { close(w.entered) })
	}
	if w.block != nil {
		<-w.block
	}
	return 0, context.DeadlineExceeded
}
func (w *failingDeadlineWriter) Flush() {}

func TestListenerCounterDecrementedOnceOnConcurrentTimeoutAndDisconnect(t *testing.T) {
	s, _ := publicServer(t, func(cfg *config.Config) {
		cfg.Defaults.ListenerWriteTimeout = config.Duration(50 * time.Millisecond)
	})
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "http://radio.example/radio", nil).WithContext(ctx)
	rw := &failingDeadlineWriter{header: make(http.Header), entered: make(chan struct{}), block: make(chan struct{})}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleListener(rw, req)
	}()

	waitForListeners(t, s, "/radio", 1)
	if err := m.Write([]byte("trigger-write")); err != nil {
		t.Fatal(err)
	}
	<-rw.entered

	// Trigger write deadline failure and client context cancellation simultaneously.
	close(rw.block)
	cancel()
	<-done

	if got := m.Status().Listeners; got != 0 {
		t.Fatalf("listeners=%d, want 0", got)
	}
	metrics := metricsOutput(t, s)
	if !strings.Contains(metrics, `kite_listeners{mount="/radio",transport="http"} 0`) {
		t.Fatalf("expected kite_listeners gauge to be 0, got:\n%s", metrics)
	}
	if !strings.Contains(metrics, `kite_listener_disconnects_total{mount="/radio",reason="write_timeout",transport="http"} 1`) {
		t.Fatalf("expected exactly 1 write_timeout disconnect, got:\n%s", metrics)
	}
	if strings.Contains(metrics, `kite_listener_disconnects_total{mount="/radio",reason="client_closed",transport="http"}`) {
		t.Fatalf("unexpected double disconnect in metrics:\n%s", metrics)
	}
}
