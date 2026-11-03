package stream

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Master290/kite/internal/config"
)

func testConfig(mounts ...config.Mount) *config.Config {
	return &config.Config{Defaults: config.Defaults{SourceTimeout: config.Duration(250 * time.Millisecond), FailbackDelay: config.Duration(250 * time.Millisecond), BufferDuration: config.Duration(250 * time.Millisecond), MaxSourceBitrate: 128000}, Mounts: mounts}
}
func testMount(path string) config.Mount {
	return config.Mount{Path: path, Profile: "mp3", ContentType: "audio/mpeg", Source: config.SourceCredential{PasswordEnv: "TEST_PASSWORD"}, SourceTimeout: config.Duration(250 * time.Millisecond), FailbackDelay: config.Duration(250 * time.Millisecond), BufferDuration: config.Duration(250 * time.Millisecond), ICYMetaInterval: 16000}
}

func TestHubBroadcastsCompleteChunk(t *testing.T) {
	h, err := NewHub(testConfig(testMount("/radio")), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	m, _ := h.Get("/radio")
	sub := m.Subscribe("test")
	defer sub.Close("done")
	if err := m.AcquireSource(); err != nil {
		t.Fatal(err)
	}
	defer m.ReleaseSource()
	want := []byte("audio-frame")
	if err := m.Write(want); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != string(want) {
		t.Fatalf("got %q", got.Data)
	}
}

func TestHubSwitchesToFallbackMount(t *testing.T) {
	primary := testMount("/radio")
	primary.Fallback = []config.Fallback{{Mount: "/backup"}}
	backup := testMount("/backup")
	h, err := NewHub(testConfig(primary, backup), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	p, _ := h.Get("/radio")
	b, _ := h.Get("/backup")
	if err := b.AcquireSource(); err != nil {
		t.Fatal(err)
	}
	defer b.ReleaseSource()
	sub := p.Subscribe("test")
	defer sub.Close("done")
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if b.Write([]byte("backup-frame")) != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		got, err := sub.Next(ctx)
		if err != nil {
			t.Fatalf("timed out; status=%+v err=%v", p.Status(), err)
		}
		if string(got.Data) == "backup-frame" {
			return
		}
	}
}

func TestHubPromotesLiveBackupOverFile(t *testing.T) {
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	file := t.TempDir() + "/emergency.mp3"
	if err := os.WriteFile(file, frame, 0o600); err != nil {
		t.Fatal(err)
	}
	primary := testMount("/radio")
	primary.Metadata.Bitrate = 128000
	primary.Fallback = []config.Fallback{{Mount: "/backup"}, {File: file}}
	backup := testMount("/backup")
	h, err := NewHub(testConfig(primary, backup), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	p, _ := h.Get("/radio")
	b, _ := h.Get("/backup")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && p.Status().Active != "file:"+file {
		time.Sleep(20 * time.Millisecond)
	}
	if p.Status().Active != "file:"+file {
		t.Fatalf("file fallback not active: %+v", p.Status())
	}
	if err := b.AcquireSource(); err != nil {
		t.Fatal(err)
	}
	defer b.ReleaseSource()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	deadline = time.Now().Add(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			_ = b.Write(frame)
			if p.Status().Active == "/backup" {
				return
			}
		case <-time.After(time.Until(deadline)):
			t.Fatalf("backup was not promoted: %+v", p.Status())
		}
	}
}

func TestSharedRingRejectsSlowListener(t *testing.T) {
	mCfg := testMount("/radio")
	mCfg.Metadata.Bitrate = 8000
	h, err := NewHub(testConfig(mCfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	m, _ := h.Get("/radio")
	sub := m.Subscribe("test")
	defer sub.Close("done")
	if err := m.AcquireSource(); err != nil {
		t.Fatal(err)
	}
	defer m.ReleaseSource()
	payload := make([]byte, 10<<10)
	for i := 0; i < 3; i++ {
		if err := m.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		m.mu.RLock()
		first := m.ringFirst
		m.mu.RUnlock()
		if first > 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := sub.Next(ctx); err != ErrSlowListener {
		t.Fatalf("error=%v status=%+v", err, m.Status())
	}
}

func TestEventSubscriberReceivesInitialState(t *testing.T) {
	h, err := NewHub(testConfig(testMount("/radio")), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	m, _ := h.Get("/radio")
	events, cancel := m.SubscribeEvents(0)
	defer cancel()
	first := <-events
	second := <-events
	if first.Type != "source" || second.Type != "metadata" {
		t.Fatalf("events=%s,%s", first.Type, second.Type)
	}
}

func TestBurstOnConnectDefaultAndPerMountOverride(t *testing.T) {
	defaultMount := testMount("/default-burst")
	customMount := testMount("/custom-burst")
	customMount.BurstBytes = 417 * 10 // 10 MP3 frames (4170 bytes)

	h, err := NewHub(testConfig(defaultMount, customMount), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})

	mDefault, _ := h.Get("/default-burst")
	if err := mDefault.AcquireSource(); err != nil {
		t.Fatal(err)
	}
	defer mDefault.ReleaseSource()

	// Write 200 frames (83,400 bytes > 65,536 default burst_bytes) before listener subscribes.
	for i := 0; i < 200; i++ {
		f := append([]byte(nil), frame...)
		f[4] = byte(i)
		if err := mDefault.Write(f); err != nil {
			t.Fatal(err)
		}
	}

	subDefault := mDefault.Subscribe("http")
	defer subDefault.Close("done")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var burstTotal int
	var firstChunk Chunk
	for subDefault.HasBuffered() {
		ch, err := subDefault.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if burstTotal == 0 {
			firstChunk = ch
		}
		if len(ch.Data) < 4 || ch.Data[0] != 0xff || ch.Data[1]&0xe0 != 0xe0 {
			t.Fatalf("burst chunk at seq %d is not frame-aligned", ch.Sequence)
		}
		burstTotal += len(ch.Data)
	}

	// 65536 / 417 = 157 frames = 65,469 bytes (starting at frame index 43, seq 44).
	wantFrames := DefaultBurstBytes / len(frame)
	wantBytes := wantFrames * len(frame)
	if burstTotal != wantBytes {
		t.Fatalf("default burst bytes=%d, want %d (%d frames)", burstTotal, wantBytes, wantFrames)
	}
	if firstChunk.Data[4] != byte(200-wantFrames) {
		t.Fatalf("first burst frame marker=%d, want %d", firstChunk.Data[4], 200-wantFrames)
	}

	// Verify per-mount burst_bytes override and mid-frame alignment.
	mCustom, _ := h.Get("/custom-burst")
	if err := mCustom.AcquireSource(); err != nil {
		t.Fatal(err)
	}
	defer mCustom.ReleaseSource()

	for i := 0; i < 30; i++ {
		f := append([]byte(nil), frame...)
		f[4] = byte(i)
		if err := mCustom.Write(f); err != nil {
			t.Fatal(err)
		}
	}

	subCustom := mCustom.Subscribe("http")
	defer subCustom.Close("done")

	var customBytes int
	var firstCustom Chunk
	for subCustom.HasBuffered() {
		ch, err := subCustom.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if customBytes == 0 {
			firstCustom = ch
		}
		customBytes += len(ch.Data)
	}
	if customBytes != 417*10 {
		t.Fatalf("custom burst bytes=%d, want %d", customBytes, 417*10)
	}
	if firstCustom.Data[4] != 20 {
		t.Fatalf("first custom burst frame marker=%d, want 20", firstCustom.Data[4])
	}
}
