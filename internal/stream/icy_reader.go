package stream

import (
	"bytes"
	"io"
	"strings"
	"unicode/utf8"
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

const maxICYMetadataBytes = 4080

// FormatICYMetadata formats a StreamTitle metadata string in Icecast style
// without escaping single quotes, stripping/replacing only framing-breaking
// characters, truncating on a UTF-8 rune boundary, and always ending with "';".
func FormatICYMetadata(title string) string {
	const (
		prefix        = "StreamTitle='"
		suffix        = "';"
		maxTitleBytes = maxICYMetadataBytes - len(prefix) - len(suffix)
	)
	clean := strings.Map(func(r rune) rune {
		switch r {
		case 0:
			return -1
		case '\r', '\n':
			return ' '
		default:
			return r
		}
	}, title)
	for strings.Contains(clean, suffix) {
		clean = strings.ReplaceAll(clean, suffix, "' ")
	}
	if len(clean) > maxTitleBytes {
		cut := maxTitleBytes
		for cut > 0 && !utf8.RuneStart(clean[cut]) {
			cut--
		}
		clean = clean[:cut]
		for len(clean) > 0 && !utf8.ValidString(clean) {
			clean = clean[:len(clean)-1]
		}
	}
	return prefix + clean + suffix
}

// FormatICYMetadataBlock encodes a length-prefixed, 16-byte zero-padded ICY metadata block.
func FormatICYMetadataBlock(title string) []byte {
	meta := FormatICYMetadata(title)
	blocks := (len(meta) + 15) / 16
	out := make([]byte, 1+blocks*16)
	out[0] = byte(blocks)
	copy(out[1:], meta)
	return out
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
		if eq < 0 {
			break
		}
		if semi := strings.IndexByte(s[:eq], ';'); semi >= 0 {
			s = s[semi+1:]
			continue
		}
		key := strings.TrimSpace(s[:eq])
		rest := strings.TrimLeft(s[eq+1:], " \t")
		var val string
		if len(rest) > 0 && (rest[0] == '\'' || rest[0] == '"') {
			quote := rest[0]
			closing := string([]byte{quote, ';'})
			content := rest[1:]
			if end := strings.Index(content, closing); end >= 0 {
				val = content[:end]
				s = content[end+len(closing):]
			} else if len(content) > 0 && content[len(content)-1] == quote {
				val = content[:len(content)-1]
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
		if key == "" {
			continue
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
