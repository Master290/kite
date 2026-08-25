package stream

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const maxFrameSize = 1 << 20

var errValidated = errors.New("media frame validated")

func ValidateFile(profile, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	err = Pump(profile, file, func([]byte) error { return errValidated })
	if errors.Is(err, errValidated) {
		return nil
	}
	return err
}

func Pump(profile string, r io.Reader, write func([]byte) error) error {
	br := bufio.NewReaderSize(r, 64<<10)
	switch profile {
	case "mp3":
		return pumpMP3(br, write)
	case "aac-adts":
		return pumpADTS(br, write)
	case "ogg-opus":
		return pumpOgg(br, write)
	case "opaque":
		return pumpOpaque(br, write)
	default:
		return fmt.Errorf("unsupported profile %q", profile)
	}
}

func pumpOpaque(r *bufio.Reader, write func([]byte) error) error {
	buf := make([]byte, 16<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if werr := write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err != nil {
			return err
		}
	}
}

func pumpMP3(r *bufio.Reader, write func([]byte) error) error {
	if header, _ := r.Peek(3); len(header) == 3 && string(header) == "ID3" {
		head, err := r.Peek(10)
		if err != nil {
			return err
		}
		size := 10 + synchsafe(head[6:10])
		if size > maxFrameSize {
			return errors.New("ID3 tag too large")
		}
		tag, err := readN(r, size)
		if err != nil {
			return err
		}
		if err := write(tag); err != nil {
			return err
		}
	}
	for {
		header, err := seekSync(r, func(b []byte) bool { return b[0] == 0xff && b[1]&0xe0 == 0xe0 }, 4)
		if err != nil {
			return err
		}
		length, err := mp3FrameLength(header)
		if err != nil {
			_, _ = r.ReadByte()
			continue
		}
		frame, err := readN(r, length)
		if err != nil {
			return err
		}
		if err := write(frame); err != nil {
			return err
		}
	}
}

func mp3FrameLength(h []byte) (int, error) {
	length, _, _, err := mp3FrameInfo(h)
	return length, err
}

func pumpADTS(r *bufio.Reader, write func([]byte) error) error {
	for {
		header, err := seekSync(r, func(b []byte) bool { return b[0] == 0xff && b[1]&0xf6 == 0xf0 }, 7)
		if err != nil {
			return err
		}
		length := int(header[3]&3)<<11 | int(header[4])<<3 | int(header[5]>>5)
		if length < 7 || length > maxFrameSize {
			_, _ = r.ReadByte()
			continue
		}
		frame, err := readN(r, length)
		if err != nil {
			return err
		}
		if err := write(frame); err != nil {
			return err
		}
	}
}

func pumpOgg(r *bufio.Reader, write func([]byte) error) error {
	for {
		header, err := seekSync(r, func(b []byte) bool { return bytes.Equal(b[:4], []byte("OggS")) }, 27)
		if err != nil {
			return err
		}
		segments := int(header[26])
		full, err := r.Peek(27 + segments)
		if err != nil {
			return err
		}
		size := 27 + segments
		for _, v := range full[27:] {
			size += int(v)
		}
		if size > maxFrameSize {
			return errors.New("ogg page too large")
		}
		page, err := readN(r, size)
		if err != nil {
			return err
		}
		if err := write(page); err != nil {
			return err
		}
	}
}

func seekSync(r *bufio.Reader, valid func([]byte) bool, needed int) ([]byte, error) {
	for {
		b, err := r.Peek(needed)
		if err != nil {
			return nil, err
		}
		if valid(b) {
			return b, nil
		}
		if _, err := r.ReadByte(); err != nil {
			return nil, err
		}
	}
}
func readN(r io.Reader, n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}
func synchsafe(b []byte) int {
	if len(b) < 4 {
		return 0
	}
	return int(b[0]&0x7f)<<21 | int(b[1]&0x7f)<<14 | int(b[2]&0x7f)<<7 | int(b[3]&0x7f)
}

func OggSerial(page []byte) uint32 {
	if len(page) < 18 {
		return 0
	}
	return binary.LittleEndian.Uint32(page[14:18])
}
