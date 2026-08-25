package stream

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Master290/kite/internal/config"
)

func hlsMP3Frame() []byte {
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64}) // MPEG1 Layer III 128 kbps 44100 Hz
	return frame
}

// feedHLS pushes n MP3 frames into the packager as one chunk.
func feedHLS(t *testing.T, p *hlsPackager, seq *uint64, frames ...[]byte) {
	t.Helper()
	buf := make([]byte, 0, 512*len(frames))
	for _, f := range frames {
		buf = append(buf, f...)
	}
	p.consume(Chunk{Sequence: *seq, Data: buf})
	*seq++
}

func newHLSMount(t *testing.T) (*Mount, *hlsPackager) {
	t.Helper()
	cfg := testConfig(testMount("/radio"))
	cfg.Mounts[0].Metadata.Bitrate = 128000
	h, err := NewHub(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	m, _ := h.Get("/radio")
	p := m.ensureHLSPackager()
	if p == nil {
		t.Fatal("packager did not start")
	}
	return m, p
}

func TestHLSSegmentationAndWindow(t *testing.T) {
	_, p := newHLSMount(t)
	frame := hlsMP3Frame()
	seq := uint64(1)

	burst := make([][]byte, 160)
	for i := range burst {
		burst[i] = frame
	}
	feedHLS(t, p, &seq, burst...)
	feedHLS(t, p, &seq, burst...)

	m, _ := p.mount.Hub().Get("/radio")
	infos, target, ok := m.HLSSnapshot()
	if !ok || target != hlsTargetDurationSec {
		t.Fatalf("snapshot ok=%v target=%d", ok, target)
	}
	if len(infos) < 2 {
		t.Fatalf("segments=%d", len(infos))
	}
	for i, info := range infos {
		if info.Seq != uint64(i) {
			t.Fatalf("seq[%d]=%d", i, info.Seq)
		}
		if info.Duration < 3.9 || info.Duration > 4.05 {
			t.Fatalf("duration[%d]=%.3f", i, info.Duration)
		}
		data, ok := m.HLSSegment(info.Seq)
		if !ok || len(data)%tsPacketSize != 0 {
			t.Fatalf("segment %d bad size %d", info.Seq, len(data))
		}
	}
	// Window stays bounded even after far more material.
	for i := 0; i < 20; i++ {
		feedHLS(t, p, &seq, burst...)
	}
	infos, _, _ = m.HLSSnapshot()
	if len(infos) > hlsWindowSegments {
		t.Fatalf("window grew to %d", len(infos))
	}
}

func (m *Mount) Hub() *Hub { return m.hub }

func TestHLSDiscontinuityOnSequenceGap(t *testing.T) {
	_, p := newHLSMount(t)
	frame := hlsMP3Frame()
	seq := uint64(1)
	burst := make([][]byte, 160)
	for i := range burst {
		burst[i] = frame
	}
	feedHLS(t, p, &seq, burst...)

	// Simulate dropped chunks (ring eviction or source reset).
	gapped := seq + 5
	p.consume(Chunk{Sequence: gapped, Data: bytesOf(frame, 30)})
	seq = gapped + 1
	feedHLS(t, p, &seq, burst...)
	feedHLS(t, p, &seq, burst...)

	m, _ := p.mount.Hub().Get("/radio")
	infos, _, _ := m.HLSSnapshot()
	if len(infos) < 2 {
		t.Fatalf("segments=%d", len(infos))
	}
	var sawDisc bool
	for _, info := range infos[:len(infos)-1] {
		sawDisc = sawDisc || info.Discontinuity
	}
	if !sawDisc {
		t.Fatalf("no discontinuity marked: %+v", infos)
	}
	if infos[len(infos)-1].Discontinuity {
		t.Fatal("discontinuity leaked into later segments")
	}
}

func bytesOf(b []byte, n int) []byte {
	out := make([]byte, 0, len(b)*n)
	for i := 0; i < n; i++ {
		out = append(out, b...)
	}
	return out
}

func TestHLSDiscontinuityOnRateChange(t *testing.T) {
	cfg := testConfig(configMountWithProfile("/aac", "aac-adts"))
	cfg.Mounts[0].Metadata.Bitrate = 128000
	h, err := NewHub(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	m, _ := h.Get("/aac")
	p := m.ensureHLSPackager()
	if p == nil {
		t.Fatal("packager did not start")
	}

	seq := uint64(1)
	burst := make([][]byte, 190) // 1024*190/48000 ≈ 4.05 s
	for i := range burst {
		burst[i] = makeADTS(3, 30) // 48 kHz
	}
	feedHLS(t, p, &seq, burst...)

	aac44100 := makeADTS(4, 30)
	p.consume(Chunk{Sequence: seq, Data: aac44100})
	seq++
	feedHLS(t, p, &seq, burst...)
	feedHLS(t, p, &seq, burst...)

	infos, _, _ := m.HLSSnapshot()
	if len(infos) == 0 || !infos[len(infos)-1].Discontinuity {
		t.Fatalf("rate change not marked: %+v", infos)
	}
}

// makeADTS builds a minimal valid ADTS frame header for AAC-LC.
func makeADTS(freqIndex int, length int) []byte {
	return []byte{
		0xFF,
		0xF1, // MPEG-4, Layer 0, no CRC
		byte(0x40 | freqIndex<<2),
		byte(length >> 11 & 0x03),
		byte(length >> 3),
		byte(length&0x07)<<5 | 0x1F,
		0xFC,
	}
}

func TestHLSUnsupportedProfile(t *testing.T) {
	cfg := testConfig(configMountWithProfile("/ogg", "ogg-opus"))
	h, err := NewHub(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	m, _ := h.Get("/ogg")
	if _, _, ok := m.HLSSnapshot(); ok {
		t.Fatal("ogg-opus unexpectedly packaged")
	}
}

func configMountWithProfile(path, profile string) config.Mount {
	m := testMount(path)
	m.Profile = profile
	switch profile {
	case "ogg-opus":
		m.ContentType = "audio/ogg"
	case "aac-adts":
		m.ContentType = "audio/aac"
	}
	return m
}

func TestHLSFFProbeAcceptsSegment(t *testing.T) {
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe not installed")
	}
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.mp3")
	gen := exec.Command(ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100",
		"-t", "5", "-codec:a", "libmp3lame", "-b:a", "128k", src)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg source generation failed: %v %s", err, out)
	}
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	var frames []Frame
	if _, err := AnalyzeBuffer("mp3", raw, func(f Frame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	const totalFrames = 153 // 1152*153/44100 ≈ 3.996 s
	if len(frames) < totalFrames+1 {
		t.Fatalf("only %d frames parsed from source", len(frames))
	}
	frames = frames[1 : totalFrames+1] // drop the LAME/Xing info frame

	m := newTSMuxer("mp3")
	seg := m.StartSegment(nil)
	pts := uint64(hlsPTSBase)
	for start := 0; start < totalFrames; start += hlsFramesPerPES {
		count := min(hlsFramesPerPES, totalFrames-start)
		payload := make([]byte, 0, count*len(frames[0].Data))
		for i := 0; i < count; i++ {
			payload = append(payload, frames[start+i].Data...)
		}
		seg = m.AddFrame(seg, pts, payload)
		pts += uint64(count) * uint64(frames[0].Samples) * ptsTicksPerSec / uint64(frames[0].SampleRate)
	}

	path := filepath.Join(dir, "seg0.ts")
	if err := os.WriteFile(path, seg, 0o600); err != nil {
		t.Fatal(err)
	}
	if p := os.Getenv("KITE_DEBUG_SEG"); p != "" {
		if err := os.WriteFile(p, seg, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	probeCmd := exec.Command(ffprobe, "-v", "error", "-print_format", "json",
		"-show_streams", "-show_format", path)
	var stdout, stderr bytes.Buffer
	probeCmd.Stdout = &stdout
	probeCmd.Stderr = &stderr
	if err := probeCmd.Run(); err != nil {
		t.Fatalf("ffprobe rejected segment: %v %s", err, stderr.String())
	}
	if stderr.Len() > 0 {
		t.Fatalf("ffprobe decode warnings: %s", stderr.String())
	}
	out := stdout.Bytes()
	if !bytes.Contains(out, []byte(`"codec_name"`)) {
		t.Fatalf("unexpected probe body: %s", out)
	}
	var probe struct {
		Streams []struct {
			CodecName  string `json:"codec_name"`
			SampleRate string `json:"sample_rate"`
			Duration   string `json:"duration"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &probe); err != nil {
		t.Fatal(err)
	}
	if len(probe.Streams) != 1 || probe.Streams[0].CodecName != "mp3" {
		t.Fatalf("streams=%+v", probe.Streams)
	}
	if probe.Streams[0].SampleRate != "44100" {
		t.Fatalf("sample_rate=%q", probe.Streams[0].SampleRate)
	}
	duration := probe.Format.Duration
	if duration == "" || duration == "N/A" {
		duration = probe.Streams[0].Duration
	}
	seconds, parseErr := strconv.ParseFloat(duration, 64)
	if parseErr != nil || seconds < 3.5 || seconds > 4.5 {
		t.Fatalf("probed duration=%q err=%v", duration, parseErr)
	}
}
