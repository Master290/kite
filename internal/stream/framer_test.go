package stream

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
)

func TestPumpMP3Frames(t *testing.T) {
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	input := append([]byte(nil), frame...)
	input = append(input, frame...)
	var frames [][]byte
	err := Pump("mp3", bytes.NewReader(input), func(b []byte) error { frames = append(frames, append([]byte(nil), b...)); return nil })
	if !errors.Is(err, io.EOF) {
		t.Fatalf("error=%v", err)
	}
	if len(frames) != 2 || len(frames[0]) != 417 {
		t.Fatalf("frames=%d size=%d", len(frames), len(frames[0]))
	}
}

func TestValidateFile(t *testing.T) {
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	path := t.TempDir() + "/valid.mp3"
	if err := os.WriteFile(path, frame, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFile("mp3", path); err != nil {
		t.Fatal(err)
	}
	bad := t.TempDir() + "/bad.mp3"
	if err := os.WriteFile(bad, []byte("not audio"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFile("mp3", bad); err == nil {
		t.Fatal("invalid media accepted")
	}
}

func TestPumpADTSFrames(t *testing.T) {
	frame := make([]byte, 20)
	copy(frame, []byte{0xff, 0xf1, 0x50, 0x80, 0x02, 0x80, 0xfc})
	var frames [][]byte
	err := Pump("aac-adts", bytes.NewReader(frame), func(b []byte) error { frames = append(frames, append([]byte(nil), b...)); return nil })
	if !errors.Is(err, io.EOF) {
		t.Fatalf("error=%v", err)
	}
	if len(frames) != 1 || len(frames[0]) != 20 {
		t.Fatalf("frames=%v", frames)
	}
}

func TestPumpOggPages(t *testing.T) {
	page := make([]byte, 33)
	copy(page, []byte("OggS"))
	page[26] = 1
	page[27] = 5
	copy(page[28:], []byte("Opus!"))
	var pages [][]byte
	err := Pump("ogg-opus", bytes.NewReader(page), func(b []byte) error { pages = append(pages, append([]byte(nil), b...)); return nil })
	if !errors.Is(err, io.EOF) {
		t.Fatalf("error=%v", err)
	}
	if len(pages) != 1 || len(pages[0]) != 33 {
		t.Fatalf("pages=%v", pages)
	}
}
