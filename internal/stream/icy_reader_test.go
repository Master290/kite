package stream

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestParseICYMetadata(t *testing.T) {
	raw := []byte("StreamTitle='Daft Punk - One More Time';StreamUrl='https://radio.example';\x00\x00")
	title, url := ParseICYMetadata(raw)
	if title != "Daft Punk - One More Time" {
		t.Fatalf("unexpected title %q", title)
	}
	if url != "https://radio.example" {
		t.Fatalf("unexpected url %q", url)
	}

	withSemicolon := []byte("StreamTitle='Artist; feat. B - Song';StreamUrl='https://radio.example/track;1';\x00")
	title, url = ParseICYMetadata(withSemicolon)
	if title != "Artist; feat. B - Song" {
		t.Fatalf("expected title with semicolon, got %q", title)
	}
	if url != "https://radio.example/track;1" {
		t.Fatalf("expected url with semicolon, got %q", url)
	}

	malformedAndSpaced := []byte("=x; garbage;  StreamTitle = 'It's Live';  StreamUrl = 'https://radio.example';")
	title, url = ParseICYMetadata(malformedAndSpaced)
	if title != "It's Live" {
		t.Fatalf("expected title after malformed pairs, got %q", title)
	}
	if url != "https://radio.example" {
		t.Fatalf("expected url after malformed pairs, got %q", url)
	}
}

func TestICYMetadataRoundTrip(t *testing.T) {
	cases := []string{
		`Dir C:\`,
		`It's`,
		`Artist; feat. B`,
		`Artist; feat. B - It's a Song (Dir C:\)`,
		`Привет — мир; тест '123'`,
	}
	for _, want := range cases {
		block := FormatICYMetadataBlock(want)
		if len(block) < 17 || int(block[0])*16 != len(block)-1 {
			t.Fatalf("invalid block framing for %q: len=%d header=%d", want, len(block), block[0])
		}
		got, _ := ParseICYMetadata(block[1:])
		if got != want {
			t.Fatalf("round-trip mismatch for %q: got %q (raw=%q)", want, got, block[1:])
		}
	}

	// Verify UTF-8 rune-boundary truncation and trailing "';"
	longUTF8 := strings.Repeat("Я", 2500) // 5000 bytes of 2-byte UTF-8 runes
	formatted := FormatICYMetadata(longUTF8)
	if len(formatted) > maxICYMetadataBytes {
		t.Fatalf("formatted length %d exceeds %d", len(formatted), maxICYMetadataBytes)
	}
	if !strings.HasSuffix(formatted, "';") {
		t.Fatalf("formatted metadata must end with \"';\", got suffix %q", formatted[len(formatted)-10:])
	}
	if !utf8.ValidString(formatted) {
		t.Fatal("truncated metadata is not valid UTF-8")
	}
	got, _ := ParseICYMetadata([]byte(formatted))
	if !utf8.ValidString(got) || len(got) == 0 || !strings.HasPrefix(longUTF8, got) {
		t.Fatalf("unexpected truncated round-trip title len=%d valid=%v", len(got), utf8.ValidString(got))
	}
}

func TestICYReaderExtractsMetadataAndPreservesAudio(t *testing.T) {
	audio1 := []byte("0123456789")
	audio2 := []byte("abcdefghij")

	metaStr := "StreamTitle='Foo';"
	pad := (16 - (len(metaStr) % 16)) % 16
	metaPayload := append([]byte(metaStr), make([]byte, pad)...)
	metaLenByte := byte(len(metaPayload) / 16)

	var streamData bytes.Buffer
	streamData.Write(audio1)
	streamData.WriteByte(metaLenByte)
	streamData.Write(metaPayload)
	streamData.Write(audio2)

	var extractedTitle string
	reader := newICYReader(&streamData, 10, func(m Metadata) {
		extractedTitle = m.Title
	})

	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	expectedAudio := append(audio1, audio2...)
	if !bytes.Equal(out, expectedAudio) {
		t.Fatalf("expected audio %q, got %q", expectedAudio, out)
	}
	if extractedTitle != "Foo" {
		t.Fatalf("expected title 'Foo', got %q", extractedTitle)
	}
}
