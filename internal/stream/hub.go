package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Master290/kite/internal/config"
)

var ErrSourceBusy = errors.New("source already connected")
var ErrSlowListener = errors.New("listener fell behind the live buffer")

type Event struct {
	ID      uint64         `json:"id"`
	Type    string         `json:"type"`
	Mount   string         `json:"mount"`
	Time    time.Time      `json:"time"`
	Payload map[string]any `json:"payload,omitempty"`
}

type Metadata struct {
	Title string `json:"title"`
	URL   string `json:"url,omitempty"`
}

type Chunk struct {
	Sequence uint64
	Data     []byte
	At       time.Time
}

type Observer interface {
	SourceConnected(mount string)
	SourceDisconnected(mount string)
	BytesIn(mount string, n int)
	BytesOut(mount, transport string, n int)
	ListenerOpened(mount, transport string)
	ListenerClosed(mount, transport, reason string)
	FallbackSwitch(mount, target string)
}

type nopObserver struct{}

func (nopObserver) SourceConnected(string)                {}
func (nopObserver) SourceDisconnected(string)             {}
func (nopObserver) BytesIn(string, int)                   {}
func (nopObserver) BytesOut(string, string, int)          {}
func (nopObserver) ListenerOpened(string, string)         {}
func (nopObserver) ListenerClosed(string, string, string) {}
func (nopObserver) FallbackSwitch(string, string)         {}

type Hub struct {
	mu       sync.RWMutex
	mounts   map[string]*Mount
	observer Observer
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewHub(cfg *config.Config, observer Observer) (*Hub, error) {
	if observer == nil {
		observer = nopObserver{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{mounts: make(map[string]*Mount), observer: observer, ctx: ctx, cancel: cancel}
	if err := h.Apply(cfg); err != nil {
		cancel()
		return nil, err
	}
	return h, nil
}

func (h *Hub) Close() { h.cancel() }

func (h *Hub) Apply(cfg *config.Config) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	next := make(map[string]*Mount, len(cfg.Mounts))
	for _, mc := range cfg.Mounts {
		if old, ok := h.mounts[mc.Path]; ok && old.Profile() == mc.Profile {
			old.Update(mc, cfg.Defaults)
			next[mc.Path] = old
			continue
		}
		m := newMount(h.ctx, h, mc, cfg.Defaults, h.observer)
		next[mc.Path] = m
	}
	for path, old := range h.mounts {
		if _, ok := next[path]; !ok {
			old.stop()
		}
	}
	h.mounts = next
	return nil
}

func (h *Hub) Get(path string) (*Mount, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	m, ok := h.mounts[path]
	return m, ok
}

func (h *Hub) Status() []Status {
	h.mu.RLock()
	items := make([]Status, 0, len(h.mounts))
	for _, m := range h.mounts {
		items = append(items, m.Status())
	}
	h.mu.RUnlock()
	return items
}

type subscription struct {
	C      <-chan Chunk
	cancel func(string)
}

func (s subscription) Close(reason string) { s.cancel(reason) }

type listener struct {
	ch        chan Chunk
	transport string
	closed    atomic.Bool
}

type ingest struct {
	data []byte
	at   time.Time
}

type Mount struct {
	hub      *Hub
	observer Observer
	ctx      context.Context
	cancel   context.CancelFunc

	cfgMu    sync.RWMutex
	cfg      config.Mount
	defaults config.Defaults

	primary chan ingest
	events  chan Event

	mu           sync.RWMutex
	listeners    map[uint64]*listener
	eventSubs    map[uint64]chan Event
	eventHistory []Event
	nextID       uint64
	sequence     uint64
	eventID      uint64
	source       bool
	sourceSince  time.Time
	lastSource   time.Time
	active       string
	metadata     Metadata
	bytesIn      uint64
	bytesOut     uint64
	initChunks   [][]byte

	sourceGuard  chan struct{}
	sourceCloser io.Closer
}

type Status struct {
	Path        string    `json:"path"`
	Profile     string    `json:"profile"`
	ContentType string    `json:"content_type"`
	Source      bool      `json:"source_connected"`
	Active      string    `json:"active_source"`
	Listeners   int       `json:"listeners"`
	Metadata    Metadata  `json:"metadata"`
	LastSource  time.Time `json:"last_source_at,omitempty"`
	BytesIn     uint64    `json:"bytes_in"`
	BytesOut    uint64    `json:"bytes_out"`
}

func newMount(parent context.Context, hub *Hub, cfg config.Mount, defaults config.Defaults, observer Observer) *Mount {
	ctx, cancel := context.WithCancel(parent)
	m := &Mount{
		hub: hub, observer: observer, ctx: ctx, cancel: cancel, cfg: cfg, defaults: defaults,
		primary: make(chan ingest, 256), events: make(chan Event, 32), listeners: make(map[uint64]*listener),
		eventSubs: make(map[uint64]chan Event), sourceGuard: make(chan struct{}, 1), active: "silence",
	}
	go m.run()
	return m
}

func (m *Mount) stop()                { m.cancel() }
func (m *Mount) Profile() string      { m.cfgMu.RLock(); defer m.cfgMu.RUnlock(); return m.cfg.Profile }
func (m *Mount) Config() config.Mount { m.cfgMu.RLock(); defer m.cfgMu.RUnlock(); return m.cfg }
func (m *Mount) Update(cfg config.Mount, defaults config.Defaults) {
	m.cfgMu.Lock()
	m.cfg = cfg
	m.defaults = defaults
	m.cfgMu.Unlock()
}

func (m *Mount) AcquireSource() error {
	select {
	case m.sourceGuard <- struct{}{}:
		m.mu.Lock()
		m.source = true
		m.sourceSince = time.Now()
		m.lastSource = m.sourceSince
		m.initChunks = nil
		m.mu.Unlock()
		m.observer.SourceConnected(m.Config().Path)
		m.publish("source", map[string]any{"connected": true})
		return nil
	default:
		return ErrSourceBusy
	}
}

func (m *Mount) ReleaseSource() {
	select {
	case <-m.sourceGuard:
	default:
		return
	}
	m.mu.Lock()
	m.source = false
	m.sourceCloser = nil
	m.mu.Unlock()
	m.observer.SourceDisconnected(m.Config().Path)
	m.publish("source", map[string]any{"connected": false})
}

func (m *Mount) SetSourceCloser(closer io.Closer) {
	m.mu.Lock()
	m.sourceCloser = closer
	m.mu.Unlock()
}

func (m *Mount) DisconnectSource() bool {
	m.mu.RLock()
	closer := m.sourceCloser
	connected := m.source
	m.mu.RUnlock()
	if !connected || closer == nil {
		return false
	}
	return closer.Close() == nil
}

func (m *Mount) Write(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	copyData := append([]byte(nil), p...)
	now := time.Now()
	if m.Profile() == "ogg-opus" && len(copyData) >= 27 && string(copyData[:4]) == "OggS" {
		headerType := copyData[5]
		if headerType&0x02 != 0 || bytesContains(copyData, []byte("OpusHead")) || bytesContains(copyData, []byte("OpusTags")) {
			m.mu.Lock()
			if len(m.initChunks) < 3 {
				m.initChunks = append(m.initChunks, append([]byte(nil), copyData...))
			}
			m.mu.Unlock()
		}
	}
	select {
	case m.primary <- ingest{data: copyData, at: now}:
		m.mu.Lock()
		m.lastSource = now
		m.bytesIn += uint64(len(p))
		m.mu.Unlock()
		m.observer.BytesIn(m.Config().Path, len(p))
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *Mount) SetMetadata(md Metadata) {
	m.mu.Lock()
	m.metadata = md
	m.mu.Unlock()
	m.publish("metadata", map[string]any{"title": md.Title, "url": md.URL})
}

func (m *Mount) Metadata() Metadata { m.mu.RLock(); defer m.mu.RUnlock(); return m.metadata }

func (m *Mount) Subscribe(transport string) subscription {
	m.mu.Lock()
	m.nextID++
	id := m.nextID
	buffer := listenerBuffer(m.Config(), m.defaults)
	l := &listener{ch: make(chan Chunk, buffer), transport: transport}
	m.listeners[id] = l
	for _, init := range m.initChunks {
		l.ch <- Chunk{Data: append([]byte(nil), init...), At: time.Now()}
	}
	m.mu.Unlock()
	m.observer.ListenerOpened(m.Config().Path, transport)
	var once sync.Once
	return subscription{C: l.ch, cancel: func(reason string) {
		once.Do(func() {
			removed := false
			m.mu.Lock()
			if current, ok := m.listeners[id]; ok {
				delete(m.listeners, id)
				current.closed.Store(true)
				close(current.ch)
				removed = true
			}
			m.mu.Unlock()
			if removed {
				m.observer.ListenerClosed(m.Config().Path, transport, reason)
			}
		})
	}}
}

func listenerBuffer(cfg config.Mount, defaults config.Defaults) int {
	bitrate := cfg.Metadata.Bitrate
	if bitrate <= 0 {
		bitrate = defaults.MaxSourceBitrate
	}
	seconds := cfg.BufferDuration.Duration().Seconds()
	chunks := int((float64(bitrate)/8*seconds)/16384) + 2
	if chunks < 4 {
		chunks = 4
	}
	if chunks > 256 {
		chunks = 256
	}
	return chunks
}

func (m *Mount) SubscribeEvents(afterID uint64) (<-chan Event, func()) {
	m.mu.Lock()
	m.nextID++
	id := m.nextID
	ch := make(chan Event, 128)
	for _, ev := range m.eventHistory {
		if ev.ID > afterID {
			ch <- ev
		}
	}
	m.eventSubs[id] = ch
	m.mu.Unlock()
	return ch, func() {
		m.mu.Lock()
		if current, ok := m.eventSubs[id]; ok {
			delete(m.eventSubs, id)
			close(current)
		}
		m.mu.Unlock()
	}
}

func (m *Mount) publish(kind string, payload map[string]any) {
	path := m.Config().Path
	m.mu.Lock()
	m.eventID++
	ev := Event{ID: m.eventID, Type: kind, Mount: path, Time: time.Now(), Payload: payload}
	m.eventHistory = append(m.eventHistory, ev)
	if len(m.eventHistory) > 128 {
		m.eventHistory = append([]Event(nil), m.eventHistory[len(m.eventHistory)-128:]...)
	}
	for _, ch := range m.eventSubs {
		select {
		case ch <- ev:
		default:
		}
	}
	m.mu.Unlock()
}

func (m *Mount) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var fallback <-chan []byte
	var fallbackCancel context.CancelFunc
	var stableSince time.Time
	for {
		select {
		case <-m.ctx.Done():
			if fallbackCancel != nil {
				fallbackCancel()
			}
			m.closeListeners("shutdown")
			return
		case in := <-m.primary:
			cfg := m.Config()
			m.mu.RLock()
			sourceSince := m.sourceSince
			active := m.active
			m.mu.RUnlock()
			if stableSince.IsZero() {
				stableSince = sourceSince
			}
			if active == "primary" || active == "silence" || time.Since(stableSince) >= cfg.FailbackDelay.Duration() {
				if active != "primary" {
					if fallbackCancel != nil {
						fallbackCancel()
						fallbackCancel = nil
						fallback = nil
					}
					previous := active
					m.setActive("primary")
					if previous != "silence" && cfg.Profile == "ogg-opus" {
						for _, init := range m.InitialChunks() {
							m.broadcast(init, time.Now())
						}
					}
				}
				m.broadcast(in.data, in.at)
			}
		case data, ok := <-fallback:
			if !ok {
				fallback = nil
				fallbackCancel = nil
				continue
			}
			m.broadcast(data, time.Now())
		case <-ticker.C:
			cfg := m.Config()
			m.mu.RLock()
			connected, last, active := m.source, m.lastSource, m.active
			m.mu.RUnlock()
			healthy := connected && !last.IsZero() && time.Since(last) <= cfg.SourceTimeout.Duration()
			if healthy {
				continue
			}
			stableSince = time.Time{}
			desired := m.desiredFallback(cfg)
			if active != desired || fallback == nil {
				if fallbackCancel != nil {
					fallbackCancel()
				}
				var ctx context.Context
				ctx, fallbackCancel = context.WithCancel(m.ctx)
				fallback = m.startFallback(ctx, cfg, desired)
			}
		}
	}
}

func (m *Mount) desiredFallback(cfg config.Mount) string {
	for _, fb := range cfg.Fallback {
		if fb.Mount != "" {
			if target, ok := m.hub.Get(fb.Mount); ok && target.Status().Active != "silence" {
				return fb.Mount
			}
			continue
		}
		if _, err := os.Stat(fb.File); err == nil {
			return "file:" + fb.File
		}
	}
	return "silence"
}

func (m *Mount) startFallback(ctx context.Context, cfg config.Mount, desired string) <-chan []byte {
	out := make(chan []byte, 16)
	if desired == "silence" {
		m.setActive("silence")
		close(out)
		return out
	}
	for _, fb := range cfg.Fallback {
		if fb.Mount != "" && fb.Mount == desired {
			target, ok := m.hub.Get(fb.Mount)
			if !ok || target.Status().Active == "silence" {
				continue
			}
			m.setActive(fb.Mount)
			sub := target.Subscribe("fallback")
			go func() {
				defer close(out)
				defer sub.Close("fallback_end")
				for {
					select {
					case <-ctx.Done():
						return
					case chunk, ok := <-sub.C:
						if !ok {
							return
						}
						select {
						case out <- chunk.Data:
						case <-ctx.Done():
							return
						}
					}
				}
			}()
			return out
		}
		if fb.File != "" && "file:"+fb.File == desired {
			m.setActive("file:" + fb.File)
			go pumpFile(ctx, fb.File, cfg.Profile, cfg.Metadata.Bitrate, out)
			return out
		}
	}
	m.setActive("silence")
	close(out)
	return out
}

func (m *Mount) InitialChunks() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	chunks := make([][]byte, len(m.initChunks))
	for i := range m.initChunks {
		chunks[i] = append([]byte(nil), m.initChunks[i]...)
	}
	return chunks
}

func pumpFile(ctx context.Context, path, profile string, bitrate int, out chan<- []byte) {
	defer close(out)
	if bitrate <= 0 {
		bitrate = 128000
	}
	for {
		f, err := os.Open(path)
		if err != nil {
			return
		}
		_ = Pump(profile, f, func(data []byte) error {
			copyData := append([]byte(nil), data...)
			select {
			case out <- copyData:
			case <-ctx.Done():
				return ctx.Err()
			}
			delay := time.Duration(float64(time.Second) * float64(len(data)*8) / float64(bitrate))
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
				return nil
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		})
		f.Close()
		if ctx.Err() != nil {
			return
		}
	}
}

func (m *Mount) setActive(active string) {
	m.mu.Lock()
	if m.active == active {
		m.mu.Unlock()
		return
	}
	m.active = active
	m.mu.Unlock()
	m.observer.FallbackSwitch(m.Config().Path, active)
	m.publish("source", map[string]any{"active": active})
}

func (m *Mount) broadcast(data []byte, at time.Time) {
	path := m.Config().Path
	m.mu.Lock()
	m.sequence++
	chunk := Chunk{Sequence: m.sequence, Data: data, At: at}
	for id, l := range m.listeners {
		select {
		case l.ch <- chunk:
		default:
			delete(m.listeners, id)
			l.closed.Store(true)
			close(l.ch)
			m.observer.ListenerClosed(path, l.transport, "slow")
		}
	}
	m.bytesOut += uint64(len(data) * len(m.listeners))
	m.mu.Unlock()
}

func (m *Mount) closeListeners(reason string) {
	path := m.Config().Path
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, l := range m.listeners {
		delete(m.listeners, id)
		if !l.closed.Swap(true) {
			close(l.ch)
			m.observer.ListenerClosed(path, l.transport, reason)
		}
	}
	for id, ch := range m.eventSubs {
		delete(m.eventSubs, id)
		close(ch)
	}
}

func (m *Mount) Status() Status {
	cfg := m.Config()
	m.mu.RLock()
	defer m.mu.RUnlock()
	listeners := 0
	for _, l := range m.listeners {
		if l.transport != "fallback" {
			listeners++
		}
	}
	return Status{Path: cfg.Path, Profile: cfg.Profile, ContentType: cfg.ContentType, Source: m.source, Active: m.active, Listeners: listeners, Metadata: m.metadata, LastSource: m.lastSource, BytesIn: m.bytesIn, BytesOut: m.bytesOut}
}

func Copy(ctx context.Context, dst io.Writer, sub subscription, onWrite func(int)) error {
	defer sub.Close("client_closed")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-sub.C:
			if !ok {
				return ErrSlowListener
			}
			n, err := dst.Write(chunk.Data)
			if n > 0 && onWrite != nil {
				onWrite(n)
			}
			if err != nil {
				return err
			}
		}
	}
}

func ValidateChunk(profile string, b []byte) error {
	if len(b) == 0 {
		return io.ErrUnexpectedEOF
	}
	switch profile {
	case "mp3":
		for i := 0; i+1 < len(b) && i < 4096; i++ {
			if b[i] == 0xff && b[i+1]&0xe0 == 0xe0 {
				return nil
			}
		}
		if len(b) >= 3 && string(b[:3]) == "ID3" {
			return nil
		}
		return fmt.Errorf("no MPEG audio frame sync found")
	case "aac-adts":
		for i := 0; i+1 < len(b) && i < 4096; i++ {
			if b[i] == 0xff && b[i+1]&0xf6 == 0xf0 {
				return nil
			}
		}
		return fmt.Errorf("no ADTS frame sync found")
	case "ogg-opus":
		if len(b) >= 4 && string(b[:4]) == "OggS" {
			return nil
		}
		return fmt.Errorf("missing Ogg capture pattern")
	default:
		return nil
	}
}

func bytesContains(b, sub []byte) bool {
	if len(sub) == 0 || len(b) < len(sub) {
		return false
	}
	for i := 0; i+len(sub) <= len(b); i++ {
		if string(b[i:i+len(sub)]) == string(sub) {
			return true
		}
	}
	return false
}
