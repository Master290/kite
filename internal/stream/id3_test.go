package stream

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func buildTestID3v2(artist, title string) []byte {
	var frames []byte
	sizeBuf := make([]byte, 4)

	if title != "" {
		tit2Payload := append([]byte{0x00}, []byte(title)...)
		var tit2Frame []byte
		tit2Frame = append(tit2Frame, []byte("TIT2")...)
		binary.BigEndian.PutUint32(sizeBuf, uint32(len(tit2Payload)))
		tit2Frame = append(tit2Frame, sizeBuf...)
		tit2Frame = append(tit2Frame, 0, 0) // flags
		tit2Frame = append(tit2Frame, tit2Payload...)
		frames = append(frames, tit2Frame...)
	}

	if artist != "" {
		tpe1Payload := append([]byte{0x00}, []byte(artist)...)
		var tpe1Frame []byte
		tpe1Frame = append(tpe1Frame, []byte("TPE1")...)
		binary.BigEndian.PutUint32(sizeBuf, uint32(len(tpe1Payload)))
		tpe1Frame = append(tpe1Frame, sizeBuf...)
		tpe1Frame = append(tpe1Frame, 0, 0) // flags
		tpe1Frame = append(tpe1Frame, tpe1Payload...)
		frames = append(frames, tpe1Frame...)
	}

	// ID3v2 Header: "ID3" + version(2.3) + flags(0) + synchsafe size(4)
	var tag []byte
	tag = append(tag, []byte("ID3")...)
	tag = append(tag, 3, 0, 0) // v2.3.0, flags=0
	totalSize := len(frames)
	tag = append(tag, byte((totalSize>>21)&0x7f), byte((totalSize>>14)&0x7f), byte((totalSize>>7)&0x7f), byte(totalSize&0x7f))
	tag = append(tag, frames...)
	return tag
}

func TestExtractTrackMetadataID3v2(t *testing.T) {
	artist := "Solar Stone"
	title := "Seven Cities"
	tag := buildTestID3v2(artist, title)
	tag = append(tag, makeTestMP3Frame()...)

	tmp := filepath.Join(t.TempDir(), "track.mp3")
	if err := os.WriteFile(tmp, tag, 0o600); err != nil {
		t.Fatal(err)
	}

	gotArtist, gotTitle := ExtractTrackMetadata(tmp)
	if gotArtist != artist {
		t.Fatalf("expected artist %q, got %q", artist, gotArtist)
	}
	if gotTitle != title {
		t.Fatalf("expected title %q, got %q", title, gotTitle)
	}

	formatted := FormatTrackTitle(tmp, "")
	expected := "Solar Stone - Seven Cities"
	if formatted != expected {
		t.Fatalf("expected formatted %q, got %q", expected, formatted)
	}
}

func TestExtractTrackMetadataID3v1(t *testing.T) {
	// Construct an MP3 file with ID3v1 tag at the end
	content := makeTestMP3Frame()

	var tag1 [128]byte
	copy(tag1[:3], "TAG")
	copy(tag1[3:33], "Children")
	copy(tag1[33:63], "Robert Miles")

	content = append(content, tag1[:]...)

	tmp := filepath.Join(t.TempDir(), "ambient.mp3")
	if err := os.WriteFile(tmp, content, 0o600); err != nil {
		t.Fatal(err)
	}

	gotArtist, gotTitle := ExtractTrackMetadata(tmp)
	if gotArtist != "Robert Miles" {
		t.Fatalf("expected artist 'Robert Miles', got %q", gotArtist)
	}
	if gotTitle != "Children" {
		t.Fatalf("expected title 'Children', got %q", gotTitle)
	}

	formatted := FormatTrackTitle(tmp, "")
	if formatted != "Robert Miles - Children" {
		t.Fatalf("expected 'Robert Miles - Children', got %q", formatted)
	}
}

func TestExtractTrackMetadataFallbackToFilename(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "01_Daft_Punk_-_One_More_Time.mp3")
	if err := os.WriteFile(tmp, makeTestMP3Frame(), 0o600); err != nil {
		t.Fatal(err)
	}

	formatted := FormatTrackTitle(tmp, "")
	if formatted != "01 Daft Punk - One More Time" {
		t.Fatalf("expected filename cleanup, got %q", formatted)
	}
}
