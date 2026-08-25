package stream

// Minimal MPEG-TS muxer for audio-only HLS segments. It emits 188-byte
// packets forming a single-program transport stream with one elementary
// audio stream (MP3 or AAC ADTS), PTS timing, and PCR on every frame start.
//
// Continuity counters run for the lifetime of the muxer so packets stay
// consistent across segment boundaries of the same packaging session.

import "encoding/binary"

const (
	tsPacketSize    = 188
	tsSyncByte      = 0x47
	tsPIDPAT        = 0x0000
	tsPIDPMT        = 0x1000
	tsPIDAudio      = 0x0100
	pesStreamAudio  = 0xC0
	ptsTicksPerSec  = 90000
	pcrLeadTicks    = 3600 // PCR runs 40 ms behind PTS
	pesAudioHeader  = 14   // start code(3)+stream_id(1)+length(2)+flags(3)+PTS(5)
	firstPayloadCap = tsPacketSize - 4 - 8 // minus TS header and AF{len,flags,PCR}
)

// StreamType returns the MPEG-TS stream_type byte for a Kite profile.
func StreamType(profile string) (byte, bool) {
	switch profile {
	case "mp3":
		return 0x03, true // ISO/IEC 11172-3 audio
	case "aac-adts":
		return 0x0F, true // AAC ADTS
	default:
		return 0, false
	}
}

// crc32MPEG computes CRC-32/MPEG-2 used by PSI sections
// (poly 0x04C11DB7, init and xorout 0xFFFFFFFF, no reflection).
func crc32MPEG(data []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, b := range data {
		crc ^= uint32(b) << 24
		for i := 0; i < 8; i++ {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04C11DB7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

type tsMuxer struct {
	patCC   byte
	pmtCC   byte
	audioCC byte
	patPMT  []byte
}

func newTSMuxer(profile string) *tsMuxer {
	streamType, _ := StreamType(profile)
	m := &tsMuxer{}
	pat := m.psiPacket(tsPIDPAT, &m.patCC, finishSection(0x00, buildPAT()))
	pmt := m.psiPacket(tsPIDPMT, &m.pmtCC, finishSection(0x02, buildPMT(streamType)))
	m.patPMT = append(m.patPMT, pat...)
	m.patPMT = append(m.patPMT, pmt...)
	return m
}

func buildPAT() []byte {
	return []byte{
		0x00, 0x01, // transport_stream_id 1
		0xC1,       // version 0, current_next_apply 1
		0x00, 0x00, // section_number / last_section_number
		0x00, 0x01,                                 // program_number 1
		0xE0 | byte((tsPIDPMT>>8)&0x1F), byte(tsPIDPMT & 0xFF), // reserved + program_map_PID
	}
}

func buildPMT(streamType byte) []byte {
	return []byte{
		0x00, 0x01, // program_number 1
		0xC1,       // version 0, current_next_apply 1
		0x00, 0x00, // section_number / last_section_number
		0xE0 | byte((tsPIDAudio>>8)&0x1F), byte(tsPIDAudio & 0xFF), // reserved + PCR PID
		0xF0, 0x00, // reserved + program_info_length 0
		streamType,
		0xE0 | byte((tsPIDAudio>>8)&0x1F), byte(tsPIDAudio & 0xFF), // reserved + elementary_PID
		0xF0, 0x00, // reserved + ES_info_length 0
	}
}

// finishSection wraps body in a table header and appends the CRC32.
func finishSection(tableID byte, body []byte) []byte {
	length := len(body) + 4 // +CRC32
	section := make([]byte, 0, 3+len(body)+4)
	section = append(section, tableID, 0xB0|byte(length>>8), byte(length))
	section = append(section, body...)
	return binary.BigEndian.AppendUint32(section, crc32MPEG(section))
}

func (m *tsMuxer) nextCC(cc *byte) byte {
	v := *cc
	*cc = (*cc + 1) & 0x0F
	return v
}

// psiPacket packs one complete PSI section into a single padded TS packet.
func (m *tsMuxer) psiPacket(pid uint16, cc *byte, section []byte) []byte {
	packet := make([]byte, tsPacketSize)
	packet[0] = tsSyncByte
	packet[1] = 0x40 | byte(pid>>8) // payload_unit_start_indicator set
	packet[2] = byte(pid & 0xFF)
	packet[3] = 0x10 | m.nextCC(cc) // adaptation_field_control '01': payload only
	packet[4] = 0x00 // pointer_field: section starts right here
	n := copy(packet[5:], section)
	for i := 5 + n; i < tsPacketSize; i++ {
		packet[i] = 0xFF
	}
	return packet
}

// StartSegment appends the PAT and PMT packets that open a segment file.
func (m *tsMuxer) StartSegment(dst []byte) []byte {
	return append(dst, m.patPMT...)
}

// AddFrame muxes one elementary frame as a PES packet starting at pts
// (90 kHz clock) and appends resulting TS packets to dst.
func (m *tsMuxer) AddFrame(dst []byte, pts uint64, frame []byte) []byte {
	pes := make([]byte, pesAudioHeader+len(frame))
	pes[0], pes[1], pes[2] = 0x00, 0x00, 0x01
	pes[3] = pesStreamAudio
	pesLength := pesAudioHeader - 6 + len(frame)
	pes[4] = byte(pesLength >> 8)
	pes[5] = byte(pesLength)
	pes[6] = 0x80 // flags: marker '10', no scrambling, no priority flags
	pes[7] = 0x80 // flags: PTS_DTS_flags '10' (PTS present)
	pes[8] = 0x05 // header_data_length: the 5-byte PTS
	writePTS(pes[9:], pts)
	copy(pes[pesAudioHeader:], frame)

	var pcr uint64
	if pts > pcrLeadTicks {
		pcr = pts - pcrLeadTicks
	}

	// First packet always carries an adaptation field with PCR; when the PES
	// is shorter than the payload capacity the adaptation field grows with
	// 0xFF stuffing so the packet stays exactly 188 bytes.
	afLen := 7
	if len(pes) < firstPayloadCap {
		afLen += firstPayloadCap - len(pes)
	}
	dst = appendTSHeader(dst, true, true, m.nextCC(&m.audioCC))
	dst = append(dst, byte(afLen), 0x10)
	dst = appendPCR(dst, pcr)
	for i := 7; i < afLen; i++ {
		dst = append(dst, 0xFF)
	}
	n := min(len(pes), firstPayloadCap)
	dst = append(dst, pes[:n]...)

	for rest := pes[n:]; len(rest) > 0; {
		if full := tsPacketSize - 4; len(rest) >= full {
			dst = appendTSHeader(dst, false, false, m.nextCC(&m.audioCC))
			dst = append(dst, rest[:full]...)
			rest = rest[full:]
			continue
		}
		afLen = tsPacketSize - 5 - len(rest)
		dst = appendTSHeader(dst, false, true, m.nextCC(&m.audioCC))
		dst = append(dst, byte(afLen), 0x00)
		for i := 0; i < afLen-1; i++ {
			dst = append(dst, 0xFF)
		}
		dst = append(dst, rest...)
		rest = nil
	}
	return dst
}

// appendTSHeader writes a 4-byte TS header for the audio PID: sync byte,
// PUSI flag plus PID high bits, PID low byte, then scrambling (clear),
// adaptation_field_control ('11' with field, '01' payload only) and CC.
func appendTSHeader(dst []byte, pusi, af bool, cc byte) []byte {
	b1 := byte(tsPIDAudio >> 8)
	if pusi {
		b1 |= 0x40 // payload_unit_start_indicator
	}
	b3 := byte(0x10 | cc&0x0F) // payload present
	if af {
		b3 |= 0x20 // adaptation field follows
	}
	return append(dst, tsSyncByte, b1, byte(tsPIDAudio&0xFF), b3)
}

func writePTS(b []byte, v uint64) {
	v &= (1 << 33) - 1
	b[0] = 0x21 | byte(v>>29)&0x0E
	b[1] = byte(v >> 22)
	b[2] = 0x01 | byte(v>>14)&0xFE
	b[3] = byte(v >> 7)
	b[4] = 0x01 | byte(v<<1)&0xFE
}

func appendPCR(dst []byte, ticks uint64) []byte {
	base := (ticks / 300) & ((1 << 33) - 1)
	ext := ticks % 300
	return append(dst,
		byte(base>>25),
		byte(base>>17),
		byte(base>>9),
		byte(base>>1),
		byte(base<<7)|0x7E|byte(ext>>8),
		byte(ext),
	)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
