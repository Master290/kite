package stream

import (
	"bytes"
	"io"
	"testing"
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
