package stream

import "errors"

// Frame is one complete elementary audio frame with timing metadata
// derived from its header, enough to build a continuous PTS timeline.
type Frame struct {
	Data       []byte
	Samples    int
	SampleRate int
}

var ErrHLSUnsupportedProfile = errors.New("profile does not support HLS packaging")

// AnalyzeBuffer parses profile-framed media from buf, calling fn once per
// complete frame. It returns how many leading bytes were consumed so callers
// can carry a partial trailing frame over to the next chunk. Bytes between
// frames (junk, skipped ID3 tags) are consumed silently. The function never
// consumes an incomplete trailing frame.
//
// The slice-based implementation keeps carry-over exact: nothing is read
// ahead behind the caller's back.
func AnalyzeBuffer(profile string, buf []byte, fn func(Frame) error) (int, error) {
	switch profile {
	case "mp3":
		return analyzeMP3Buf(buf, fn)
	case "aac-adts":
		return analyzeADTSBuf(buf, fn)
	default:
		return 0, ErrHLSUnsupportedProfile
	}
}

func analyzeMP3Buf(buf []byte, fn func(Frame) error) (int, error) {
	pos := 0
	if len(buf) >= 10 && string(buf[:3]) == "ID3" {
		size := 10 + synchsafe(buf[6:10])
		if size > maxFrameSize {
			return pos, errors.New("ID3 tag too large")
		}
		if size > len(buf) {
			return 0, nil // wait for the rest of the tag
		}
		pos = size
	}
	for {
		start := pos
		for start+4 <= len(buf) && !(buf[start] == 0xff && buf[start+1]&0xe0 == 0xe0) {
			start++
		}
		pos = start
		if start+4 > len(buf) {
			return pos, nil
		}
		length, samples, rate, err := mp3FrameInfo(buf[start : start+4])
		if err != nil {
			pos = start + 1 // false sync: rescan from the next byte
			continue
		}
		if start+length > len(buf) {
			return start, nil // partial trailing frame stays in the buffer
		}
		frame := buf[start : start+length]
		if err := fn(Frame{Data: frame, Samples: samples, SampleRate: rate}); err != nil {
			return start, err
		}
		pos = start + length
	}
}

func analyzeADTSBuf(buf []byte, fn func(Frame) error) (int, error) {
	pos := 0
	for {
		start := pos
		for start+7 <= len(buf) && !(buf[start] == 0xff && buf[start+1]&0xf6 == 0xf0) {
			start++
		}
		pos = start
		if start+7 > len(buf) {
			return pos, nil
		}
		header := buf[start : start+7]
		length := int(header[3]&3)<<11 | int(header[4])<<3 | int(header[5]>>5)
		freqIndex := int(header[2]>>2) & 0x0F
		rate := 0
		if freqIndex < len(aacSampleRates) {
			rate = aacSampleRates[freqIndex]
		}
		if length < 7 || length > maxFrameSize || rate <= 0 {
			pos = start + 1 // false sync or unsupported rate: rescan
			continue
		}
		if start+length > len(buf) {
			return start, nil
		}
		frame := buf[start : start+length]
		if err := fn(Frame{Data: frame, Samples: 1024, SampleRate: rate}); err != nil {
			return start, err
		}
		pos = start + length
	}
}

var aacSampleRates = []int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}

// mp3FrameInfo parses a 4-byte MP3 header into frame length and timing.
// Only Layer III is accepted, matching the passthrough framer.
func mp3FrameInfo(h []byte) (length, samples, sampleRate int, err error) {
	versionBits := (h[1] >> 3) & 3
	layerBits := (h[1] >> 1) & 3
	if versionBits == 1 || layerBits != 1 {
		return 0, 0, 0, errors.New("unsupported MPEG header")
	}
	bitrateIndex := (h[2] >> 4) & 0xf
	sampleIndex := (h[2] >> 2) & 3
	padding := int((h[2] >> 1) & 1)
	if bitrateIndex == 0 || bitrateIndex == 15 || sampleIndex == 3 {
		return 0, 0, 0, errors.New("invalid MPEG rate")
	}
	mpeg1 := []int{0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0}
	mpeg2 := []int{0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0}
	baseRates := []int{44100, 48000, 32000}
	sampleRate = baseRates[sampleIndex]
	bitrate := mpeg1[bitrateIndex]
	coefficient := 144
	samples = 1152
	if versionBits != 3 {
		bitrate = mpeg2[bitrateIndex]
		coefficient = 72
		samples = 576
		if versionBits == 2 {
			sampleRate /= 2
		} else {
			sampleRate /= 4
		}
	}
	length = coefficient*bitrate*1000/sampleRate + padding
	if length < 4 || length > maxFrameSize {
		return 0, 0, 0, errors.New("invalid MPEG frame length")
	}
	return length, samples, sampleRate, nil
}
