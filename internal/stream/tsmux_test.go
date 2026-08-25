package stream

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestCRC32MPEGKnownVector(t *testing.T) {
	if got := crc32MPEG([]byte("123456789")); got != 0x0376E6E7 {
		t.Fatalf("crc=%08X want 0376E6E7", got)
	}
}

func tsPackets(t *testing.T, data []byte, name string) [][]byte {
	t.Helper()
	if len(data)%tsPacketSize != 0 {
		t.Fatalf("%s: size %d not multiple of %d", name, len(data), tsPacketSize)
	}
	var out [][]byte
	for off := 0; off < len(data); off += tsPacketSize {
		pkt := data[off : off+tsPacketSize]
		if pkt[0] != tsSyncByte {
			t.Fatalf("%s: bad sync byte at offset %d", name, off)
		}
		out = append(out, pkt)
	}
	return out
}

func TestTSMuxerPATandPMT(t *testing.T) {
	m := newTSMuxer("mp3")
	packets := tsPackets(t, m.patPMT, "patpmt")
	if len(packets) != 2 {
		t.Fatalf("psi packets=%d", len(packets))
	}
	if pid := uint16(packets[0][1]&0x1F)<<8 | uint16(packets[0][2]); pid != tsPIDPAT {
		t.Fatalf("first pid=%X", pid)
	}
	if packets[0][1]&0x40 == 0 {
		t.Fatal("PAT missing PUSI")
	}
	// Continuity counters are per-PID: both PSI sections start at 0.
	if cc := packets[1][3] & 0x0F; cc != 0 {
		t.Fatalf("pmt cc=%d, want 0", cc)
	}
	// PMT section must carry the mp3 stream type on the audio PID.
	streamType := StreamTypeOrPanic(t, "mp3")
	if !bytes.Contains(packets[1], []byte{streamType, 0xE0 | byte((tsPIDAudio>>8)&0x1F), byte(tsPIDAudio & 0xFF)}) {
		t.Fatal("PMT lacks audio ES entry")
	}
}

func StreamTypeOrPanic(t *testing.T, profile string) byte {
	t.Helper()
	st, ok := StreamType(profile)
	if !ok {
		t.Fatalf("unsupported profile %s", profile)
	}
	return st
}

func decodePTS(b []byte) uint64 {
	return uint64(b[0]&0x0E)<<29 |
		uint64(b[1])<<22 |
		uint64(b[2]&0xFE)<<14 |
		uint64(b[3])<<7 |
		uint64(b[4]&0xFE)>>1
}

func TestTSMuxerAddFrameSmall(t *testing.T) {
	m := newTSMuxer("aac-adts")
	frame := make([]byte, 100)
	dst := m.AddFrame(nil, 90000*3, frame)
	packets := tsPackets(t, dst, "segment")
	if len(packets) != 1 {
		t.Fatalf("packets=%d for small frame", len(packets))
	}
	pkt := packets[0]
	if pkt[1]&0x40 == 0 {
		t.Fatal("audio PES missing PUSI")
	}
	if cc := pkt[3] & 0x0F; cc != 0 {
		t.Fatalf("first audio cc=%d", cc)
	}
	if pkt[4] < 7 || pkt[5]&0x10 == 0 {
		t.Fatalf("adaptation field without PCR: len=%d flags=%X", pkt[4], pkt[5])
	}
	// PES start code and PTS live after the adaptation field.
	afLen := int(pkt[4])
	payload := pkt[4+1+afLen:]
	if !bytes.HasPrefix(payload, []byte{0x00, 0x00, 0x01, pesStreamAudio}) {
		t.Fatalf("missing PES start code: % X", payload[:4])
	}
	got := decodePTS(payload[9:])
	if got != 90000*3 {
		t.Fatalf("pts=%d", got)
	}
}

func TestTSMuxerAddFrameMultiPacket(t *testing.T) {
	m := newTSMuxer("mp3")
	frame := make([]byte, 5000)
	for i := range frame {
		frame[i] = byte(i)
	}
	dst := m.AddFrame(nil, 90000, frame)
	packets := tsPackets(t, dst, "segment")

	want := 1 + (pesAudioHeader+len(frame)-firstPayloadCap+185)/186
	if len(packets) != want {
		t.Fatalf("packets=%d want %d", len(packets), want)
	}
	for i, pkt := range packets {
		pid := uint16(pkt[1]&0x1F)<<8 | uint16(pkt[2])
		if pid != tsPIDAudio {
			t.Fatalf("packet %d pid=%X", i, pid)
		}
	}
	// Continuity counters advance across every audio packet.
	cc := packets[0][3] & 0x0F
	for _, pkt := range packets[1:] {
		next := pkt[3] & 0x0F
		if next != (cc+1)&0x0F {
			t.Fatalf("cc jump %d -> %d", cc, next)
		}
		cc = next
	}
}

func TestTSMuxerPTSContinuityAcrossFrames(t *testing.T) {
	m := newTSMuxer("aac-adts")
	frame := make([]byte, 50)
	var pts []uint64
	for i := 0; i < 5; i++ {
		dst := m.AddFrame(nil, uint64(i)*1024*90000/48000, frame)
		pkt := tsPackets(t, dst, "frame")[0]
		afLen := int(pkt[4])
		pts = append(pts, decodePTS(pkt[4+1+afLen+9:]))
	}
	for i := 1; i < len(pts); i++ {
		if pts[i]-pts[i-1] != 1024*90000/48000 {
			t.Fatalf("delta at %d: %d", i, pts[i]-pts[i-1])
		}
	}
	_ = binary.BigEndian
}

func TestStreamTypes(t *testing.T) {
	if st, _ := StreamType("ogg-opus"); st != 0 {
		t.Fatalf("ogg stream_type=%d", st)
	}
}
