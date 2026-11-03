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
		s = strings.TrimLeft(s, " \t\r\n;")
		if len(s) == 0 {
			break
		}
		eq := strings.IndexByte(s, '=')
		if eq <= 0 {
			break
		}
		key := strings.TrimSpace(s[:eq])
		rest := strings.TrimLeft(s[eq+1:], " \t")
		var val string
		if len(rest) > 0 && (rest[0] == '\'' || rest[0] == '"') {
			quote := rest[0]
			closing := string([]byte{quote, ';'})
			content := rest[1:]
			end := findICYClosing(content, closing)
			if end >= 0 {
				val = strings.ReplaceAll(content[:end], `\`+string(quote), string(quote))
				s = content[end+len(closing):]
			} else if len(content) > 0 && content[len(content)-1] == quote {
				val = strings.ReplaceAll(content[:len(content)-1], `\`+string(quote), string(quote))
				s = ""
			} else {
				val = content
				s = ""
			}
		} else {
			if idx := strings.IndexByte(rest, ';'); idx >= 0 {
				val = strings.TrimSpace(rest[:idx])
				s = rest[idx+1:]
			} else {
				val = strings.TrimSpace(rest)
				s = ""
			}
		}
		switch strings.ToLower(key) {
		case "streamtitle":
			title = val
		case "streamurl":
			url = val
		}
	}
	return title, url
}

func findICYClosing(s, closing string) int {
	offset := 0
	for {
		idx := strings.Index(s[offset:], closing)
		if idx < 0 {
			return -1
		}
		pos := offset + idx
		backslashes := 0
		for i := pos - 1; i >= 0 && s[i] == '\\'; i-- {
			backslashes++
		}
		if backslashes%2 == 0 {
			return pos
		}
		offset = pos + len(closing)
	}
}
