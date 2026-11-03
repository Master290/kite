package stream

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Master290/kite/internal/config"
)

func makeTestMP3Frame() []byte {
	// 417-byte MPEG-1 Layer 3 frame (128 kbps, 44.1 kHz, padded=0)
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	return frame
}

func TestRelayWorkerPullsStreamAndMetadata(t *testing.T) {
	metaint := 500
	metaStr := "StreamTitle='Relayed Track Title';"
	blocks := (len(metaStr) + 15) / 16
	paddedMeta := make([]byte, blocks*16)
	copy(paddedMeta, metaStr)

	frame := makeTestMP3Frame()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Icy-MetaData") != "1" {
			t.Errorf("expected Icy-MetaData: 1 header, got %q", r.Header.Get("Icy-MetaData"))
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		w.Header().Set("icy-metaint", fmt.Sprintf("%d", metaint))
		w.Header().Set("icy-name", "Test Relay Station")
		w.WriteHeader(http.StatusOK)

		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}

		// Write stream: send audio bytes, then at metaint boundary send metadata, then more audio
		sentAudio := 0
		flusher.Flush()

		for i := 0; i < 20; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
			}

			// Write in slices of audio
			writtenInFrame := 0
			for writtenInFrame < len(frame) {
				chunkSize := len(frame) - writtenInFrame
				neededForMeta := metaint - (sentAudio % metaint)
				if chunkSize > neededForMeta {
					chunkSize = neededForMeta
				}

				if _, err := w.Write(frame[writtenInFrame : writtenInFrame+chunkSize]); err != nil {
					return
				}
				writtenInFrame += chunkSize
				sentAudio += chunkSize

				if sentAudio%metaint == 0 {
					// Write metadata block: length byte followed by metadata
					if _, err := w.Write([]byte{byte(blocks)}); err != nil {
						return
					}
					if _, err := w.Write(paddedMeta); err != nil {
						return
					}
				}
			}
			flusher.Flush()
			time.Sleep(15 * time.Millisecond)
		}
	}))
	defer server.Close()

	mountCfg := config.Mount{
		Path:            "/relay-mount",
		Profile:         "mp3",
		ContentType:     "audio/mpeg",
		SourceTimeout:   config.Duration(500 * time.Millisecond),
		FailbackDelay:   config.Duration(250 * time.Millisecond),
		BufferDuration:  config.Duration(500 * time.Millisecond),
		ICYMetaInterval: 16000,
		Relay: &config.RelayConfig{
			URL:        server.URL,
			RetryDelay: config.Duration(100 * time.Millisecond),
		},
	}

	h, err := NewHub(testConfig(mountCfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	m, ok := h.Get("/relay-mount")
	if !ok {
		t.Fatal("mount not found")
	}

	sub := m.Subscribe("test-listener")
	defer sub.Close("done")

	// Wait for source to be connected and metadata to arrive
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for {
		status := m.Status()
		meta := m.Metadata()
		if status.Source && meta.Title == "Relayed Track Title" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for relay source: status=%+v meta=%+v", status, meta)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Verify that listener receives valid frames without metadata bytes
	gotChunk, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("sub.Next failed: %v", err)
	}
	if len(gotChunk.Data) == 0 {
		t.Fatal("received empty chunk")
	}
	// Check MP3 sync word
	if gotChunk.Data[0] != 0xff || gotChunk.Data[1]&0xe0 != 0xe0 {
		t.Fatalf("expected valid MP3 sync header, got 0x%02x 0x%02x", gotChunk.Data[0], gotChunk.Data[1])
	}
}

func TestRelayWorkerBasicAuth(t *testing.T) {
	var authReceived atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if ok && u == "relayuser" && p == "relaypass" {
			authReceived.Store(true)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(makeTestMP3Frame())
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			time.Sleep(100 * time.Millisecond)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	mountCfg := config.Mount{
		Path:           "/auth-relay",
		Profile:        "mp3",
		ContentType:    "audio/mpeg",
		SourceTimeout:  config.Duration(500 * time.Millisecond),
		BufferDuration: config.Duration(500 * time.Millisecond),
		Relay: &config.RelayConfig{
			URL:        server.URL,
			Username:   "relayuser",
			Password:   "relaypass",
			RetryDelay: config.Duration(50 * time.Millisecond),
		},
	}

	h, err := NewHub(testConfig(mountCfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for {
		if authReceived.Load() {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for authenticated relay request")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestRelayWorkerReconnectAndLifecycle(t *testing.T) {
	var attempts atomic.Int32
	frame := makeTestMP3Frame()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		if n == 1 {
			// First attempt drops immediately
			return
		}
		// Subsequent attempts stream data
		for i := 0; i < 10; i++ {
			if _, err := w.Write(frame); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer server.Close()

	mountCfg := config.Mount{
		Path:           "/reconnect-relay",
		Profile:        "mp3",
		ContentType:    "audio/mpeg",
		SourceTimeout:  config.Duration(500 * time.Millisecond),
		BufferDuration: config.Duration(500 * time.Millisecond),
		Relay: &config.RelayConfig{
			URL:        server.URL,
			RetryDelay: config.Duration(50 * time.Millisecond),
		},
	}

	h, err := NewHub(testConfig(mountCfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	m, ok := h.Get("/reconnect-relay")
	if !ok {
		t.Fatal("mount not found")
	}

	// Verify reconnect happened (attempts >= 2) and source connected
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for {
		if attempts.Load() >= 2 && m.Status().Source {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for reconnect: attempts=%d source=%v", attempts.Load(), m.Status().Source)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Test Update to remove relay
	noRelayCfg := mountCfg
	noRelayCfg.Relay = nil
	m.Update(noRelayCfg, config.Defaults{})

	time.Sleep(100 * time.Millisecond)
	currentAttempts := attempts.Load()
	time.Sleep(150 * time.Millisecond)
	if attempts.Load() > currentAttempts+1 {
		t.Errorf("relay worker still attempting connections after being removed: %d -> %d", currentAttempts, attempts.Load())
	}
}

func TestRelayWorkerDirectDJPriority(t *testing.T) {
	frame := makeTestMP3Frame()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		for {
			if _, err := w.Write(frame); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}))
	defer server.Close()

	mountCfg := config.Mount{
		Path:           "/dj-priority",
		Profile:        "mp3",
		ContentType:    "audio/mpeg",
		SourceTimeout:  config.Duration(500 * time.Millisecond),
		BufferDuration: config.Duration(500 * time.Millisecond),
		Relay: &config.RelayConfig{
			URL:        server.URL,
			RetryDelay: config.Duration(50 * time.Millisecond),
		},
	}

	h, err := NewHub(testConfig(mountCfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	m, ok := h.Get("/dj-priority")
	if !ok {
		t.Fatal("mount not found")
	}

	// Wait for relay to become active
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for !m.Status().Source {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for relay to connect")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Direct DJ disconnects existing source (the relay)
	m.DisconnectSource()

	// Direct DJ acquires source
	if err := m.AcquireSource(); err != nil {
		t.Fatalf("direct DJ failed to acquire source: %v", err)
	}

	// Direct DJ writes custom frame
	djFrame := bytes.Repeat([]byte{0xaa}, 417)
	copy(djFrame, []byte{0xff, 0xfb, 0x90, 0x64}) // valid mp3 sync
	if err := m.Write(djFrame); err != nil {
		t.Fatalf("direct DJ write failed: %v", err)
	}

	// Release direct DJ
	m.ReleaseSource()

	// Verify relay re-acquires source after DJ disconnects
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	for !m.Status().Source {
		select {
		case <-ctx2.Done():
			t.Fatal("timed out waiting for relay to re-acquire source after direct DJ left")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestRelayWorkerSilentUpstreamReconnectsWithinIdleTimeout(t *testing.T) {
	var attempts atomic.Int32
	stalledSince := make(chan time.Time, 1)
	reconnectedAt := make(chan time.Time, 1)
	frame := makeTestMP3Frame()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		w.Header().Set("Content-Type", "audio/mpeg")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)

		if attempt == 1 {
			for i := 0; i < 4; i++ {
				if _, err := w.Write(frame); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
			select {
			case stalledSince <- time.Now():
			default:
			}
			// Stop sending without closing the connection; wait for relay to close it.
			<-r.Context().Done()
			return
		}

		select {
		case reconnectedAt <- time.Now():
		default:
		}
		for {
			if _, err := w.Write(frame); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(15 * time.Millisecond):
			}
		}
	}))
	defer server.Close()

	idleTimeout := 150 * time.Millisecond
	mountCfg := config.Mount{
		Path:             "/silent-relay",
		Profile:          "mp3",
		ContentType:      "audio/mpeg",
		SourceTimeout:    config.Duration(250 * time.Millisecond),
		BufferDuration:   config.Duration(250 * time.Millisecond),
		RelayIdleTimeout: config.Duration(idleTimeout),
		Relay: &config.RelayConfig{
			URL:              server.URL,
			RetryDelay:       config.Duration(30 * time.Millisecond),
			RelayIdleTimeout: config.Duration(idleTimeout),
		},
	}

	h, err := NewHub(testConfig(mountCfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	var stallStart time.Time
	select {
	case stallStart = <-stalledSince:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial relay connection to stall")
	}

	select {
	case reconn := <-reconnectedAt:
		elapsed := reconn.Sub(stallStart)
		if elapsed > 600*time.Millisecond {
			t.Fatalf("expected reconnect shortly after %v idle timeout, took %v", idleTimeout, elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("relay did not reconnect after upstream went silent (attempts=%d)", attempts.Load())
	}
}

func TestRelayInternalSubscriberExcludedFromPublicListeners(t *testing.T) {
	h, err := NewHub(testConfig(testMount("/radio")), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	m, ok := h.Get("/radio")
	if !ok {
		t.Fatal("mount not found")
	}

	relaySub := m.SubscribeInternal("relay")
	defer relaySub.Close("done")

	ctxInternal := m.SubscribeContext(WithInternalSubscriber(context.Background()), "http-icy")
	defer ctxInternal.Close("done")

	if got := m.Status().Listeners; got != 0 {
		t.Fatalf("expected internal subscribers to be excluded from public listener count, got %d", got)
	}

	// External subscriber without the internal flag must be counted even if transport is "relay".
	externalSub := m.Subscribe("relay")
	defer externalSub.Close("done")

	if got := m.Status().Listeners; got != 1 {
		t.Fatalf("expected 1 public listener, got %d", got)
	}
}

func TestRelayBackoffJitterAndCap(t *testing.T) {
	base := 100 * time.Millisecond
	for i := 0; i < 50; i++ {
		got := jitterRelayDelay(base)
		if got < 80*time.Millisecond || got > 120*time.Millisecond {
			t.Fatalf("jitterRelayDelay(%v) = %v, want within [80ms, 120ms]", base, got)
		}
	}
	for i := 0; i < 50; i++ {
		got := jitterRelayDelay(60 * time.Second)
		if got < 24*time.Second || got > 30*time.Second {
			t.Fatalf("jitterRelayDelay(60s) = %v, want within [24s, 30s]", got)
		}
	}
}

func TestRelayFastRecoveryAfterOutageAndMetrics(t *testing.T) {
	var available atomic.Bool
	frame := makeTestMP3Frame()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !available.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodHead {
			return
		}
		flusher, _ := w.(http.Flusher)
		for {
			if _, err := w.Write(frame); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(15 * time.Millisecond):
			}
		}
	}))
	defer server.Close()

	obs := &recordingRelayObserver{reasons: make(map[string]int)}
	mountCfg := config.Mount{
		Path:           "/outage-relay",
		Profile:        "mp3",
		ContentType:    "audio/mpeg",
		SourceTimeout:  config.Duration(250 * time.Millisecond),
		BufferDuration: config.Duration(250 * time.Millisecond),
		Relay: &config.RelayConfig{
			URL:        server.URL,
			RetryDelay: config.Duration(40 * time.Millisecond),
		},
	}

	h, err := NewHub(testConfig(mountCfg), obs)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	m, ok := h.Get("/outage-relay")
	if !ok {
		t.Fatal("mount not found")
	}

	// Let several failed attempts accumulate so backoff grows.
	deadline := time.Now().Add(2 * time.Second)
	for obs.reconnectCount("bad_status") < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for failed attempts, got %d", obs.reconnectCount("bad_status"))
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Bring upstream back online and assert fast recovery via probe/backoff reset.
	recoveredStart := time.Now()
	available.Store(true)

	for !m.Status().Source {
		if time.Since(recoveredStart) > 400*time.Millisecond {
			t.Fatalf("relay took too long to recover after outage: %v", time.Since(recoveredStart))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if obs.connected.Load() < 1 {
		t.Fatal("expected RelayConnected observer callback")
	}
}

type recordingRelayObserver struct {
	nopObserver
	mu        sync.Mutex
	reasons   map[string]int
	connected atomic.Int32
}

func (o *recordingRelayObserver) RelayConnected(string) {
	o.connected.Add(1)
}

func (o *recordingRelayObserver) RelayReconnect(_ string, reason string) {
	o.mu.Lock()
	o.reasons[reason]++
	o.mu.Unlock()
}

func (o *recordingRelayObserver) reconnectCount(reason string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reasons[reason]
}

func TestPrimarySwitchClearsFallbackTitle(t *testing.T) {
	dir := t.TempDir()
	fallbackPath := filepath.Join(dir, "fallback.mp3")
	frame := makeTestMP3Frame()
	if err := os.WriteFile(fallbackPath, bytes.Repeat(frame, 8), 0o644); err != nil {
		t.Fatal(err)
	}

	mountCfg := config.Mount{
		Path:           "/fallback-title",
		Profile:        "mp3",
		ContentType:    "audio/mpeg",
		SourceTimeout:  config.Duration(100 * time.Millisecond),
		FailbackDelay:  config.Duration(50 * time.Millisecond),
		BufferDuration: config.Duration(250 * time.Millisecond),
		Metadata:       config.Metadata{Name: "Configured Station Name", Bitrate: 128000},
		Fallback: []config.Fallback{
			{File: fallbackPath, Title: "Fallback Track Title"},
		},
	}

	h, err := NewHub(testConfig(mountCfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	m, ok := h.Get("/fallback-title")
	if !ok {
		t.Fatal("mount not found")
	}

	// Wait for fallback to activate and set fallback title.
	deadline := time.Now().Add(2 * time.Second)
	for m.Metadata().Title != "Fallback Track Title" {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for fallback title, got %q (active=%q)", m.Metadata().Title, m.Status().Active)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Connect primary source without setting an ICY title; on switch back to primary,
	// the fallback title must be cleared to the mount's configured name.
	if err := m.AcquireSource(); err != nil {
		t.Fatal(err)
	}
	defer m.ReleaseSource()

	deadline = time.Now().Add(2 * time.Second)
	for m.Status().Active != "primary" {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for switch back to primary, active=%q", m.Status().Active)
		}
		if err := m.Write(frame); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := m.Metadata().Title; got != "Configured Station Name" {
		t.Fatalf("expected fallback title to be cleared to configured mount name, got %q", got)
	}
}
