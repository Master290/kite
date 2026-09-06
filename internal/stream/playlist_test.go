package stream

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func makeTestMP3(t *testing.T, dir, filename string, artist, title string) string {
	t.Helper()
	filePath := filepath.Join(dir, filename)

	var content []byte
	if artist != "" || title != "" {
		tag := buildTestID3v2(artist, title)
		content = append(content, tag...)
	}

	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	content = append(content, frame...)

	if err := os.WriteFile(filePath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return filePath
}

func TestScanAudioFolder(t *testing.T) {
	dir := t.TempDir()

	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}

	f1 := makeTestMP3(t, dir, "b_song.mp3", "", "")
	f2 := makeTestMP3(t, sub, "a_song.mp3", "", "")
	_ = os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not audio"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "other.ogg"), []byte("ogg file"), 0o644)

	// Scan mp3
	mp3s, err := scanAudioFolder(dir, "mp3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mp3s) != 2 {
		t.Fatalf("expected 2 mp3s, got %d: %v", len(mp3s), mp3s)
	}
	if mp3s[0] != f1 || mp3s[1] != f2 {
		t.Errorf("expected sorted mp3s [%s, %s], got %v", f1, f2, mp3s)
	}

	// Empty dir
	emptyDir := t.TempDir()
	if _, err := scanAudioFolder(emptyDir, "mp3"); err == nil {
		t.Fatal("expected error scanning empty dir, got nil")
	}

	// Non-existent dir
	if _, err := scanAudioFolder(filepath.Join(dir, "nonexistent"), "mp3"); err == nil {
		t.Fatal("expected error scanning non-existent dir, got nil")
	}
}

func TestParsePlaylist(t *testing.T) {
	dir := t.TempDir()
	f1 := makeTestMP3(t, dir, "track1.mp3", "", "")
	f2 := makeTestMP3(t, dir, "track2.mp3", "", "")

	m3uContent := "#EXTM3U\n# Comment\n\ntrack1.mp3\n" + f2 + "\nnonexistent.mp3\n"
	m3uPath := filepath.Join(dir, "list.m3u")
	if err := os.WriteFile(m3uPath, []byte(m3uContent), 0o644); err != nil {
		t.Fatal(err)
	}

	files, err := parsePlaylist(m3uPath, "mp3")
	if err != nil {
		t.Fatalf("parsePlaylist failed: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d: %v", len(files), files)
	}
	if files[0] != f1 || files[1] != f2 {
		t.Errorf("expected [%s, %s], got %v", f1, f2, files)
	}
}

func TestValidateFolderAndPlaylist(t *testing.T) {
	dir := t.TempDir()
	makeTestMP3(t, dir, "track.mp3", "Artist", "Title")

	if err := ValidateFolder("mp3", dir); err != nil {
		t.Fatalf("ValidateFolder failed: %v", err)
	}

	m3uPath := filepath.Join(dir, "playlist.m3u")
	if err := os.WriteFile(m3uPath, []byte("track.mp3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePlaylist("mp3", m3uPath); err != nil {
		t.Fatalf("ValidatePlaylist failed: %v", err)
	}

	// Bad folder
	emptyDir := t.TempDir()
	if err := ValidateFolder("mp3", emptyDir); err == nil {
		t.Fatal("expected error on empty folder")
	}

	// Bad playlist
	badM3U := filepath.Join(emptyDir, "bad.m3u")
	if err := os.WriteFile(badM3U, []byte("ghost.mp3\n"), 0o644); err == nil {
		if err := ValidatePlaylist("mp3", badM3U); err == nil {
			t.Fatal("expected error on invalid playlist")
		}
	}
}

func TestPumpPlaylist(t *testing.T) {
	dir := t.TempDir()
	f1 := makeTestMP3(t, dir, "track1.mp3", "Daft Punk", "One More Time")
	f2 := makeTestMP3(t, dir, "track2.mp3", "Justice", "Genesis")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var titles []string
	onTrack := func(md Metadata) {
		mu.Lock()
		titles = append(titles, md.Title)
		mu.Unlock()
	}

	out := make(chan []byte, 16)
	go pumpPlaylist(ctx, []string{f1, f2}, false, "mp3", 10_000_000, "", onTrack, out)

	// Consume chunks
	received := 0
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	for {
		select {
		case _, ok := <-out:
			if !ok {
				t.Fatal("out channel closed prematurely")
			}
			received++
			mu.Lock()
			count := len(titles)
			mu.Unlock()
			if count >= 2 {
				cancel()
				// Drain out
				for range out {
				}
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for 2 tracks, titles=%v received=%d", titles, received)
		}
	}
}
