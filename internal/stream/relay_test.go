package stream

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
