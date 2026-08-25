package server

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Master290/kite/internal/config"
	"github.com/Master290/kite/internal/stream"
)

func hlsTestFrame() []byte {
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	for i := 4; i < len(frame); i++ {
		frame[i] = byte(i * 7)
	}
	return frame
}

// feedHLSFrames writes frames in small bursts so the shared ring buffer
// never evicts data ahead of the HLS packager's cursor.
func feedHLSFrames(t *testing.T, m *stream.Mount, count int) {
	t.Helper()
	frame := hlsTestFrame()
	const burst = 20
	for written := 0; written < count; {
		n := burst
		if remaining := count - written; remaining < n {
			n = remaining
		}
		for i := 0; i < n; i++ {
			if err := m.Write(frame); err != nil {
				t.Fatal(err)
			}
		}
		written += n
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHLSPlaylistAndSegmentFlow(t *testing.T) {
	s, ts := publicServer(t)
	m, _ := s.hub.Get("/radio")

	// First request starts the packager; nothing packaged yet.
	resp, err := http.Get(ts.URL + "/radio.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("cold playlist status=%d", resp.StatusCode)
	}

	waitForListeners(t, s, "/radio", 1)
	acquireTestSource(t, m)
	feedHLSFrames(t, m, 170) // ~4.3 s of audio

	var body string
	deadline := time.Now().Add(3 * time.Second)
	for {
		pl, err := http.Get(ts.URL + "/radio.m3u8")
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4096)
		n, _ := pl.Body.Read(buf)
		pl.Body.Close()
		body = string(buf[:n])
		if strings.Contains(body, "#EXTINF") || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(body, "#EXTM3U") ||
		!strings.Contains(body, "#EXT-X-TARGETDURATION:4") ||
		!strings.Contains(body, "#EXT-X-MEDIA-SEQUENCE:0") {
		t.Fatalf("playlist=%q", body)
	}
	line := ""
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, "/radio.hls/") {
			line = l
			break
		}
	}
	if line == "" {
		t.Fatalf("no segment URI in %q", body)
	}

	segResp, err := http.Get(ts.URL + line)
	if err != nil {
		t.Fatal(err)
	}
	defer segResp.Body.Close()
	if segResp.StatusCode != http.StatusOK {
		t.Fatalf("segment status=%d", segResp.StatusCode)
	}
	if ct := segResp.Header.Get("Content-Type"); ct != "video/mp2t" {
		t.Fatalf("content-type=%q", ct)
	}
	if cc := segResp.Header.Get("Cache-Control"); !strings.Contains(cc, "immutable") {
		t.Fatalf("cache-control=%q", cc)
	}
	var size int
	buf := make([]byte, 8192)
	for {
		n, err := segResp.Body.Read(buf)
		size += n
		if err != nil {
			break
		}
	}
	if size == 0 || size%188 != 0 {
		t.Fatalf("segment size=%d", size)
	}

	miss, err := http.Get(ts.URL + "/radio.hls/9999.ts")
	if err != nil {
		t.Fatal(err)
	}
	miss.Body.Close()
	if miss.StatusCode != http.StatusNotFound {
		t.Fatalf("stale segment status=%d", miss.StatusCode)
	}
}

func TestHLSDisabledGlobally(t *testing.T) {
	s, ts := publicServer(t, func(c *config.Config) {
		disabled := false
		c.Server.HLSEnabled = &disabled
	})
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)
	feedHLSFrames(t, m, 170)

	resp, err := http.Get(ts.URL + "/radio.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestHLSUnsupportedProfileReturnsNotFound(t *testing.T) {
	s, _ := publicServer(t)

	req := httptest.NewRequest(http.MethodGet, "http://radio.example/ogg-opus.m3u8", nil)
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	s.registerPublic(mux)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown mount status=%d", rec.Code)
	}

	// A mount with an unsupported profile exists but must not be packaged.
	cfg := s.Config()
	cfg.Mounts = append(cfg.Mounts, config.Mount{Path: "/oggstream", Profile: "ogg-opus", ContentType: "audio/ogg"})
	s.applyConfig(cfg)
	req = httptest.NewRequest(http.MethodGet, "http://radio.example/oggstream.m3u8", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("ogg profile status=%d", rec.Code)
	}
}

func TestHLSPlaylistSegmentURIsAreSequential(t *testing.T) {
	s, ts := publicServer(t)
	m, _ := s.hub.Get("/radio")
	resp, err := http.Get(ts.URL + "/radio.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	waitForListeners(t, s, "/radio", 1)
	acquireTestSource(t, m)
	feedHLSFrames(t, m, 340) // two segments

	deadline := time.Now().Add(3 * time.Second)
	var seqs []int
	for time.Now().Before(deadline) {
		pl, err := http.Get(ts.URL + "/radio.m3u8")
		if err != nil {
			t.Fatal(err)
		}
		raw := make([]byte, 8192)
		n, _ := pl.Body.Read(raw)
		pl.Body.Close()
		seqs = nil
		for _, l := range strings.Split(string(raw[:n]), "\n") {
			if strings.HasPrefix(l, "/radio.hls/") && strings.HasSuffix(l, ".ts") {
				mid := strings.TrimSuffix(strings.TrimPrefix(l, "/radio.hls/"), ".ts")
				v, err := strconv.Atoi(mid)
				if err == nil {
					seqs = append(seqs, v)
				}
			}
		}
		if len(seqs) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(seqs) < 2 || seqs[1] != seqs[0]+1 {
		t.Fatalf("non-consecutive sequences %v", seqs)
	}
}
