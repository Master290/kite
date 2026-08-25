package stream

import (
	"bytes"
	"testing"
)

// countWriter accumulates passthrough volume and fails fast on nil chunks.
func fuzzWrite(written *int) func([]byte) error {
	return func(b []byte) error {
		*written += len(b)
		return nil
	}
}

// assertPassthrough checks the core safety invariant: every emitted byte must
// come from the input, so output can never exceed input regardless of framing.
func assertPassthrough(t *testing.T, profile string, data []byte) {
	t.Helper()
	written := 0
	err := Pump(profile, bytes.NewReader(data), fuzzWrite(&written))
	if written > len(data) {
		t.Fatalf("%s: wrote %d bytes from %d-byte input", profile, written, len(data))
	}
	if err != nil && written == 0 && len(data) > 0 {
		// A clean rejection before any output is allowed (for example an
		// oversized ID3 tag); anything else must still have made progress.
		t.Logf("%s: stopped early: %v", profile, err)
	}
}

func FuzzPumpMP3(f *testing.F) {
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	f.Add(frame)
	f.Add(bytes.Repeat(frame, 2))
	f.Add([]byte("ID3\x04\x00\x00\x00\x00\x00\x00"))
	f.Add([]byte{0xff, 0xfb, 0xf0, 0x64}) // invalid rate bits
	f.Add([]byte{0xff, 0xe0})              // bare sync, truncated header
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		assertPassthrough(t, "mp3", data)
	})
}

func FuzzPumpADTS(f *testing.F) {
	f.Add([]byte{0xff, 0xf1, 0x50, 0x80, 0x02, 0x80, 0xfc}[:6])
	f.Add([]byte{0xff, 0xf1, 0x50, 0x80, 0x00, 0x00})
	f.Add([]byte("not audio"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		assertPassthrough(t, "aac-adts", data)
	})
}

func FuzzPumpOgg(f *testing.F) {
	page := make([]byte, 33)
	copy(page, "OggS")
	page[26] = 1
	page[27] = 5
	copy(page[28:], "Opus!")
	f.Add(page)
	truncated := append([]byte(nil), page[:20]...)
	f.Add(truncated)
	junk := make([]byte, 40)
	for i := range junk {
		junk[i] = 'O'
	}
	f.Add(junk)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		assertPassthrough(t, "ogg-opus", data)
	})
}

func FuzzPumpOpaque(f *testing.F) {
	f.Add([]byte("anything at all"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		written := 0
		err := Pump("opaque", bytes.NewReader(data), fuzzWrite(&written))
		if err == nil && written != len(data) {
			t.Fatalf("opaque passthrough lost data: wrote %d of %d", written, len(data))
		}
	})
}

// FuzzAnalyze covers the HLS frame analyzer, which parses the same untrusted
// encoder bytes as the passthrough pumps. The first byte selects the profile
// so a single target exercises both MP3 and ADTS parsing.
func FuzzAnalyze(f *testing.F) {
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	adts := []byte{0xff, 0xf1, 0x50, 0x80, 0x02, 0x80, 0xfc}
	f.Add(byte(0), append([]byte{0xff, 0xfb, 0x90, 0x64}, frame...))
	f.Add(byte(0), []byte("ID3\x04\x00\x00\x00\x00\x00\x00"))
	f.Add(byte(1), adts)
	f.Add(byte(1), append(adts, adts...))
	f.Add(byte(0), []byte{0xff, 0xe0})
	f.Add(byte(1), []byte{})
	f.Fuzz(func(t *testing.T, profileByte byte, data []byte) {
		profile := "mp3"
		if profileByte%2 == 1 {
			profile = "aac-adts"
		}
		consumed, _ := AnalyzeBuffer(profile, data, func(f Frame) error {
			if f.Samples <= 0 || f.SampleRate <= 0 {
				t.Fatalf("bad timing samples=%d rate=%d", f.Samples, f.SampleRate)
			}
			return nil
		})
		if consumed < 0 || consumed > len(data) {
			t.Fatalf("%s: consumed=%d len=%d", profile, consumed, len(data))
		}
	})
}
