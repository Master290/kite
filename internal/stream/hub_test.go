package stream

import (
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
	select {
	case got := <-sub.C:
		if string(got.Data) != string(want) {
			t.Fatalf("got %q", got.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
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
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = b.Write([]byte("backup-frame"))
		case got := <-sub.C:
			if string(got.Data) != "backup-frame" {
				t.Fatalf("got %q", got.Data)
			}
			return
		case <-deadline:
			t.Fatalf("timed out; status=%+v", p.Status())
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
