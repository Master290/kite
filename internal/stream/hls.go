package stream

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// HLS packaging turns the mount's live chunk flow into MPEG-TS segments and
// a sliding-window playlist. The packager subscribes like any listener,
// starts lazily on the first HTTP request, and stops after an idle timeout.

const (
	hlsTargetDurationSec = 4
	hlsWindowSegments    = 6
	hlsIdleTimeout       = 30 * time.Second
	hlsPTSBase           = 2 * ptsTicksPerSec // start at t=2s to avoid zero-edge quirks
	hlsMaxLeftover       = 512 << 10
	hlsFramesPerPES      = 8 // batch frames so decoders see contiguous data
)

// HLSSegmentInfo describes one segment in the sliding window.
type HLSSegmentInfo struct {
	Seq           uint64
	Duration      float64
	Discontinuity bool // applies before this segment
}

type hlsSegment struct {
	info HLSSegmentInfo
	data []byte
}

type hlsPackager struct {
	mount     *Mount
	muxer     *tsMuxer
	ctx       context.Context
	cancel    context.CancelFunc
	lastRead  atomic.Int64
	mu        sync.Mutex
	segments  []hlsSegment // oldest first, bounded by hlsWindowSegments
	nextSeq   uint64
	cur       []byte   // in-flight segment bytes
	curPts    uint64   // PTS of the first frame in cur
	samples   uint64   // accumulated samples inside cur
	rate      int      // sample rate of frames inside cur
	pts       uint64   // running timeline for the next frame
	leftover  []byte   // bytes of a partial trailing frame
	batch     []byte   // frames queued for the current PES packet
	batchLen  int
	batchPts  uint64
	lastSeq   uint64
	haveSeq   bool
	pendingDisc bool
}

func hlsSupportedProfile(profile string) bool {
	_, ok := StreamType(profile)
	return ok
}

// HLSSupportedProfile reports whether a mount profile can be packaged for HLS.
func HLSSupportedProfile(profile string) bool { return hlsSupportedProfile(profile) }
// ensureHLSPackager lazily starts packaging for this mount.
func (m *Mount) ensureHLSPackager() *hlsPackager {
	if p := m.hls.Load(); p != nil {
		return p
	}
	cfg := m.Config()
	if !hlsSupportedProfile(cfg.Profile) {
		return nil
	}
	m.hlsMu.Lock()
	defer m.hlsMu.Unlock()
	if p := m.hls.Load(); p != nil {
		return p
	}
	ctx, cancel := context.WithCancel(m.ctx)
	p := &hlsPackager{
		mount:  m,
		muxer:  newTSMuxer(cfg.Profile),
		ctx:    ctx,
		cancel: cancel,
		pts:    hlsPTSBase,
	}
	m.hls.Store(p)
	go p.run()
	return p
}

// HLSSnapshot returns the current playlist window, starting the packager on
// first use. It reports false when the mount profile cannot be packaged.
func (m *Mount) HLSSnapshot() ([]HLSSegmentInfo, int, bool) {
	p := m.ensureHLSPackager()
	if p == nil {
		return nil, 0, false
	}
	p.lastRead.Store(time.Now().UnixNano())
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]HLSSegmentInfo, len(p.segments))
	for i, s := range p.segments {
		out[i] = s.info
	}
	return out, hlsTargetDurationSec, true
}

// HLSSegment returns one packaged segment by sequence number.
func (m *Mount) HLSSegment(seq uint64) ([]byte, bool) {
	p := m.hls.Load()
	if p == nil {
		return nil, false
	}
	p.lastRead.Store(time.Now().UnixNano())
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.segments {
		if s.info.Seq == seq {
			return s.data, true
		}
	}
	return nil, false
}

func (p *hlsPackager) run() {
	sub := p.mount.Subscribe("hls")
	defer sub.Close("hls_stopped")
	type result struct {
		chunk Chunk
		err   error
	}
	frames := make(chan result, 1)
	go func() {
		for {
			chunk, err := sub.Next(p.ctx)
			select {
			case frames <- result{chunk, err}:
			case <-p.ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	idle := time.NewTicker(5 * time.Second)
	defer idle.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-idle.C:
			if time.Since(time.Unix(0, p.lastRead.Load())) > hlsIdleTimeout {
				p.mount.hls.CompareAndSwap(p, nil)
				p.cancel()
				return
			}
		case r := <-frames:
			if r.err != nil {
				return
			}
			p.consume(r.chunk)
		}
	}
}

func (p *hlsPackager) consume(chunk Chunk) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.haveSeq && chunk.Sequence != p.lastSeq+1 {
		// Ring eviction or source reset dropped data: close the segment and
		// declare a discontinuity so players resync cleanly.
		p.finalize(true)
		p.leftover = nil
	}
	p.lastSeq = chunk.Sequence
	p.haveSeq = true

	p.leftover = append(p.leftover, chunk.Data...)
	consumed, _ := AnalyzeBuffer(p.mount.Profile(), p.leftover, func(f Frame) error {
		if p.rate != 0 && f.SampleRate != p.rate {
			// Codec parameters changed: cut with a discontinuity marker.
			p.flushBatch()
			p.finalize(true)
		}
		if len(p.cur) == 0 {
			p.cur = p.muxer.StartSegment(nil)
			p.curPts = p.pts
			p.samples = 0
		}
		if p.batchLen == 0 {
			p.batchPts = p.pts
		}
		p.batch = append(p.batch, f.Data...)
		p.batchLen++
		p.rate = f.SampleRate
		p.pts += uint64(f.Samples) * ptsTicksPerSec / uint64(f.SampleRate)
		p.samples += uint64(f.Samples)
		if p.batchLen >= hlsFramesPerPES || p.samples >= uint64(f.SampleRate)*hlsTargetDurationSec {
			p.flushBatch()
		}
		if p.samples >= uint64(f.SampleRate)*hlsTargetDurationSec {
			p.finalize(false)
		}
		return nil
	})
	if consumed > 0 {
		p.leftover = p.leftover[consumed:]
	}
	if len(p.leftover) > hlsMaxLeftover {
		// Unparseable tail: discard instead of growing without bound.
		p.leftover = nil
	}
}

// flushBatch muxes queued frames as one PES packet. Decoders validate
// contiguous frame headers, so batching keeps the elementary stream clean.
func (p *hlsPackager) flushBatch() {
	if p.batchLen == 0 {
		return
	}
	p.cur = p.muxer.AddFrame(p.cur, p.batchPts, p.batch)
	p.batch = p.batch[:0]
	p.batchLen = 0
}

// finalize closes the in-flight segment. disc marks a discontinuity before it.
func (p *hlsPackager) finalize(disc bool) {
	p.flushBatch()
	if len(p.cur) == 0 {
		if disc {
			p.pendingDisc = true
		}
		return
	}
	duration := float64(p.samples) / float64(max(p.rate, 1))
	info := HLSSegmentInfo{Seq: p.nextSeq, Duration: duration, Discontinuity: disc || p.pendingDisc}
	p.pendingDisc = false
	p.nextSeq++
	p.segments = append(p.segments, hlsSegment{info: info, data: p.cur})
	if len(p.segments) > hlsWindowSegments {
		p.segments = p.segments[len(p.segments)-hlsWindowSegments:]
	}
	p.cur = nil
	p.samples = 0
	p.rate = 0
}
