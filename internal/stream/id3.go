package stream

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf16"
)

// ExtractTrackMetadata inspects the file at path for ID3v2, ID3v1, or fallback filename metadata.
func ExtractTrackMetadata(path string) (artist string, title string) {
	f, err := os.Open(path)
	if err != nil {
		return "", cleanFilename(path)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return "", cleanFilename(path)
	}

	// 1. Try ID3v2 first (at start of file)
	artist, title = readID3v2(f)
	if artist != "" || title != "" {
		return artist, title
	}

	// 2. Try ID3v1 (last 128 bytes)
	artist, title = readID3v1(f, fi.Size())
	if artist != "" || title != "" {
		return artist, title
	}

	// 3. Fallback to cleaned filename
	return "", cleanFilename(path)
}

// FormatTrackTitle returns "Artist - Title", "Title", or defaultTitle/filename.
func FormatTrackTitle(path string, defaultTitle string) string {
	if defaultTitle != "" {
		return defaultTitle
	}
	artist, title := ExtractTrackMetadata(path)
	if artist != "" && title != "" {
		return artist + " - " + title
	}
	if title != "" {
		return title
	}
	return cleanFilename(path)
}

func cleanFilename(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	if ext != "" {
		base = strings.TrimSuffix(base, ext)
	}
	base = strings.ReplaceAll(base, "_", " ")
	return strings.TrimSpace(base)
}

func readID3v2(r io.Reader) (artist string, title string) {
	var head [10]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return "", ""
	}
	if string(head[:3]) != "ID3" {
		return "", ""
	}
	version := head[3]
	tagSize := synchsafe(head[6:10])
	if tagSize <= 0 || tagSize > 10<<20 { // Cap at 10MB to avoid abnormal allocation
		return "", ""
	}

	tagData := make([]byte, tagSize)
	if _, err := io.ReadFull(r, tagData); err != nil {
		return "", ""
	}

	if version == 2 { // ID3v2.2
		return parseID3v22(tagData)
	}
	// ID3v2.3 and ID3v2.4
	return parseID3v23And4(tagData, version)
}

func parseID3v23And4(b []byte, version byte) (artist string, title string) {
	for len(b) >= 10 {
		frameID := string(b[:4])
		// Check for padding (null bytes)
		if b[0] == 0 {
			break
		}
		var frameSize int
		if version == 4 {
			frameSize = synchsafe(b[4:8])
		} else {
			frameSize = int(binary.BigEndian.Uint32(b[4:8]))
		}
		b = b[10:] // Skip header (4 id + 4 size + 2 flags)
		if frameSize < 0 || frameSize > len(b) {
			break
		}
		payload := b[:frameSize]
		b = b[frameSize:]

		if len(payload) > 1 {
			encoding := payload[0]
			text := decodeText(payload[1:], encoding)
			switch frameID {
			case "TIT2":
				title = text
			case "TPE1":
				artist = text
			}
		}
		if artist != "" && title != "" {
			break
		}
	}
	return artist, title
}

func parseID3v22(b []byte) (artist string, title string) {
	for len(b) >= 6 {
		frameID := string(b[:3])
		if b[0] == 0 {
			break
		}
		frameSize := int(b[3])<<16 | int(b[4])<<8 | int(b[5])
		b = b[6:]
		if frameSize < 0 || frameSize > len(b) {
			break
		}
		payload := b[:frameSize]
		b = b[frameSize:]

		if len(payload) > 1 {
			encoding := payload[0]
			text := decodeText(payload[1:], encoding)
			switch frameID {
			case "TT2":
				title = text
			case "TP1":
				artist = text
			}
		}
		if artist != "" && title != "" {
			break
		}
	}
	return artist, title
}

func readID3v1(r io.ReaderAt, size int64) (artist string, title string) {
	if size < 128 {
		return "", ""
	}
	var buf [128]byte
	if _, err := r.ReadAt(buf[:], size-128); err != nil {
		return "", ""
	}
	if string(buf[:3]) != "TAG" {
		return "", ""
	}
	title = strings.TrimSpace(strings.TrimRight(string(buf[3:33]), "\x00"))
	artist = strings.TrimSpace(strings.TrimRight(string(buf[33:63]), "\x00"))
	return artist, title
}

func decodeText(b []byte, encoding byte) string {
	if len(b) == 0 {
		return ""
	}
	switch encoding {
	case 0, 3: // Latin1, UTF-8
		return strings.TrimSpace(strings.TrimRight(string(b), "\x00"))
	case 1, 2: // UTF-16 with/without BOM
		if len(b) < 2 {
			return ""
		}
		var u16s []uint16
		start := 0
		bigEndian := true
		if b[0] == 0xfe && b[1] == 0xff {
			start = 2
			bigEndian = true
		} else if b[0] == 0xff && b[1] == 0xfe {
			start = 2
			bigEndian = false
		}
		for i := start; i+1 < len(b); i += 2 {
			if bigEndian {
				u16s = append(u16s, binary.BigEndian.Uint16(b[i:i+2]))
			} else {
				u16s = append(u16s, binary.LittleEndian.Uint16(b[i:i+2]))
			}
		}
		return strings.TrimSpace(strings.TrimRight(string(utf16.Decode(u16s)), "\x00"))
	default:
		return strings.TrimSpace(strings.TrimRight(string(b), "\x00"))
	}
}
