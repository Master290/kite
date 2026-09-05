package stream

import (
	"bytes"
	"io"
	"strings"
)

// icyReader wraps an audio stream that contains interleaved ICY metadata blocks.
// Every metaint bytes of audio data, a 1-byte length field L appears, followed by
// L * 16 bytes of metadata string (e.g. StreamTitle='...';StreamUrl='...';).
type icyReader struct {
	r       io.Reader
	metaint int
	remain  int
	onMeta  func(Metadata)
	metaBuf []byte
}

func newICYReader(r io.Reader, metaint int, onMeta func(Metadata)) io.Reader {
	if metaint <= 0 {
		return r
	}
	return &icyReader{
		r:       r,
		metaint: metaint,
		remain:  metaint,
		onMeta:  onMeta,
		metaBuf: make([]byte, 4096),
	}
}

func (ir *icyReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	// If we reached a metadata boundary, consume the metadata block first.
	for ir.remain == 0 {
		var lenBuf [1]byte
		if _, err := io.ReadFull(ir.r, lenBuf[:]); err != nil {
			return 0, err
		}
		metaLen := int(lenBuf[0]) * 16
		if metaLen > 0 {
			buf := ir.metaBuf
			if metaLen > len(buf) {
				buf = make([]byte, metaLen)
			}
			if _, err := io.ReadFull(ir.r, buf[:metaLen]); err != nil {
				return 0, err
			}
			if ir.onMeta != nil {
				title, url := ParseICYMetadata(buf[:metaLen])
				if title != "" || url != "" {
					ir.onMeta(Metadata{Title: title, URL: url})
				}
			}
		}
		ir.remain = ir.metaint
	}

	// Read at most ir.remain audio bytes so we don't accidentally read into metadata
	toRead := len(p)
	if toRead > ir.remain {
		toRead = ir.remain
	}

	n, err := ir.r.Read(p[:toRead])
	if n > 0 {
		ir.remain -= n
	}
	return n, err
}

// ParseICYMetadata parses raw ICY metadata block into title and url.
// Format: StreamTitle='...';StreamUrl='...';
func ParseICYMetadata(raw []byte) (title string, url string) {
	// Metadata might be null-padded
	raw = bytes.TrimRight(raw, "\x00")
	if len(raw) == 0 {
		return "", ""
	}
	s := string(raw)
	for len(s) > 0 {
		idx := strings.IndexByte(s, ';')
		var part string
		if idx >= 0 {
			part = s[:idx]
			s = s[idx+1:]
		} else {
			part = s
			s = ""
		}
		part = strings.TrimSpace(part)
		if eq := strings.IndexByte(part, '='); eq > 0 {
			key := strings.TrimSpace(part[:eq])
			val := strings.TrimSpace(part[eq+1:])
			if len(val) >= 2 && ((val[0] == '\'' && val[len(val)-1] == '\'') || (val[0] == '"' && val[len(val)-1] == '"')) {
				val = val[1 : len(val)-1]
			}
			switch strings.ToLower(key) {
			case "streamtitle":
				title = val
			case "streamurl":
				url = val
			}
		}
	}
	return title, url
}
