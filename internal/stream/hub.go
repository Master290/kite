package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Master290/kite/internal/config"
)

var ErrSourceBusy = errors.New("source already connected")
var ErrSlowListener = errors.New("listener fell behind the live buffer")
var ErrSubscriptionClosed = errors.New("listener subscription closed")

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
	RelayConnected(mount string)
	RelayDisconnected(mount, reason string)
	RelayReconnect(mount, reason string)
	BytesIn(mount string, n int)
	BytesOut(mount, transport string, n int)
	ListenerOpened(mount, transport string)
	ListenerClosed(mount, transport, reason string)
	FallbackSwitch(mount, target string)
}

type nopObserver struct{}

func (nopObserver) SourceConnected(string)                {}
func (nopObserver) SourceDisconnected(string)             {}
func (nopObserver) RelayConnected(string)                 {}
func (nopObserver) RelayDisconnected(string, string)      {}
func (nopObserver) RelayReconnect(string, string)         {}
func (nopObserver) BytesIn(string, int)                   {}
func (nopObserver) BytesOut(string, string, int)          {}
func (nopObserver) ListenerOpened(string, string)         {}
func (nopObserver) ListenerClosed(string, string, string) {}
func (nopObserver) FallbackSwitch(string, string)         {}

type Hub struct {
	mu       sync.RWMutex
	mounts   map[string]*Mount
	observer Observer
	log      *slog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewHub(cfg *config.Config, observer Observer) (*Hub, error) {
	return NewHubWithLogger(cfg, observer, slog.Default())
}

func NewHubWithLogger(cfg *config.Config, observer Observer, logger *slog.Logger) (*Hub, error) {
	if observer == nil {
		observer = nopObserver{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{mounts: make(map[string]*Mount), observer: observer, log: logger, ctx: ctx, cancel: cancel}
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

type internalSubscriberKey struct{}

// WithInternalSubscriber marks a context as originating from an internal
// relay or fallback subscriber so it is excluded from public listener counts.
func WithInternalSubscriber(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalSubscriberKey{}, true)
}

// IsInternalSubscriber reports whether ctx was marked with WithInternalSubscriber.
func IsInternalSubscriber(ctx context.Context) bool {
	v, _ := ctx.Value(internalSubscriberKey{}).(bool)
	return v
}

type subscription struct {
	mount     *Mount
	id        uint64
	transport string
	internal  bool
}

func (s subscription) Close(reason string) { s.mount.removeListener(s.id, reason) }

func (s subscription) Next(ctx context.Context) (Chunk, error) {
	path := s.mount.Config().Path
	for {
		s.mount.mu.Lock()
		listener, ok := s.mount.listeners[s.id]
		if !ok {
			s.mount.mu.Unlock()
			return Chunk{}, ErrSubscriptionClosed
		}
		if listener.preludeIndex < len(listener.prelude) {
			data := listener.prelude[listener.preludeIndex]
			listener.preludeIndex++
			s.mount.mu.Unlock()
			return Chunk{Data: data, At: time.Now()}, nil
		}
		if len(s.mount.ring) > 0 && listener.cursor < s.mount.ringFirst {
			delete(s.mount.listeners, s.id)
			s.mount.mu.Unlock()
			if !listener.internal {
				s.mount.observer.ListenerClosed(path, listener.transport, "slow")
			}
			return Chunk{}, ErrSlowListener
		}
		if len(s.mount.ring) > 0 && listener.cursor >= s.mount.ringFirst && listener.cursor <= s.mount.sequence {
			index := listener.cursor - s.mount.ringFirst
			if index < uint64(len(s.mount.ring)) {
				chunk := s.mount.ring[index]
				listener.cursor++
				s.mount.mu.Unlock()
				return chunk, nil
			}
		}
		notify := s.mount.notify
		s.mount.mu.Unlock()
		select {
		case <-ctx.Done():
			return Chunk{}, ctx.Err()
		case <-notify:
		}
	}
}

func (s subscription) RecordWrite(n int) {
	if n <= 0 || s.internal {
		return
	}
	s.mount.mu.Lock()
	s.mount.bytesOut += uint64(n)
	s.mount.mu.Unlock()
	s.mount.observer.BytesOut(s.mount.Config().Path, s.transport, n)
}

type listener struct {
	cursor       uint64
	transport    string
	internal     bool
	prelude      [][]byte
	preludeIndex int
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

	mu             sync.RWMutex
	listeners      map[uint64]*listener
	eventSubs      map[uint64]chan Event
	eventHistory   []Event
	nextID         uint64
	sequence       uint64
	ring           []Chunk
	ringFirst      uint64
	ringBytes      int
	notify         chan struct{}
	eventID        uint64
	source         bool
	isRelay        bool
	sourceSince    time.Time
	lastSource     time.Time
	active         string
	metadata       Metadata
	primaryMeta    Metadata
	fallbackTitled bool
	bytesIn        uint64
	bytesOut       uint64
	initChunks     [][]byte

	sourceGuard  chan struct{}
	sourceCloser io.Closer

	hls   atomic.Pointer[hlsPackager]
	hlsMu sync.Mutex

	relayCancel context.CancelFunc
	relayMu     sync.Mutex
}

type Status struct {
	Path           string    `json:"path"`
	Profile        string    `json:"profile"`
	ContentType    string    `json:"content_type"`
	Source         bool      `json:"source_connected"`
	IsRelay        bool      `json:"is_relay,omitempty"`
	Active         string    `json:"active_source"`
	Listeners      int       `json:"listeners"`
	Metadata       Metadata  `json:"metadata"`
	LastSource     time.Time `json:"last_source_at,omitempty"`
	BytesIn        uint64    `json:"bytes_in"`
	BytesOut       uint64    `json:"bytes_out"`
	BufferBytes    int       `json:"buffer_bytes"`
	BufferLimit    int       `json:"buffer_limit_bytes"`
	OldestSequence uint64    `json:"oldest_sequence"`
	LatestSequence uint64    `json:"latest_sequence"`
}

func newMount(parent context.Context, hub *Hub, cfg config.Mount, defaults config.Defaults, observer Observer) *Mount {
	ctx, cancel := context.WithCancel(parent)
	m := &Mount{
		hub: hub, observer: observer, ctx: ctx, cancel: cancel, cfg: cfg, defaults: defaults,
		primary: make(chan ingest, 256), listeners: make(map[uint64]*listener),
		eventSubs: make(map[uint64]chan Event), sourceGuard: make(chan struct{}, 1), active: "silence", notify: make(chan struct{}),
	}
	m.eventID = 2
	m.eventHistory = []Event{
		{ID: 1, Type: "source", Mount: cfg.Path, Time: time.Now(), Payload: map[string]any{"connected": false, "active": "silence"}},
		{ID: 2, Type: "metadata", Mount: cfg.Path, Time: time.Now(), Payload: map[string]any{"title": "", "url": ""}},
	}
	if cfg.Relay != nil {
		m.startRelay(*cfg.Relay)
	}
	go m.run()
	return m
}

func (m *Mount) logger() *slog.Logger {
	if m != nil && m.hub != nil && m.hub.log != nil {
		return m.hub.log
	}
	return slog.Default()
}

func (m *Mount) stop() {
	m.stopRelay()
	m.cancel()
}

func (m *Mount) Profile() string      { m.cfgMu.RLock(); defer m.cfgMu.RUnlock(); return m.cfg.Profile }
func (m *Mount) Config() config.Mount { m.cfgMu.RLock(); defer m.cfgMu.RUnlock(); return m.cfg }
func (m *Mount) settings() (config.Mount, config.Defaults) {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return m.cfg, m.defaults
}
func (m *Mount) relayIdleTimeout() time.Duration {
	cfg, defaults := m.settings()
	if cfg.Relay != nil && cfg.Relay.RelayIdleTimeout.Duration() > 0 {
		return cfg.Relay.RelayIdleTimeout.Duration()
	}
	if cfg.RelayIdleTimeout.Duration() > 0 {
		return cfg.RelayIdleTimeout.Duration()
	}
	if defaults.RelayIdleTimeout.Duration() > 0 {
		return defaults.RelayIdleTimeout.Duration()
	}
	return 10 * time.Second
}

func (m *Mount) Update(cfg config.Mount, defaults config.Defaults) {
	oldIdle := m.relayIdleTimeout()
	m.cfgMu.Lock()
	oldRelay := m.cfg.Relay
	m.cfg = cfg
	m.defaults = defaults
	m.cfgMu.Unlock()

	if cfg.Relay != nil {
		if oldRelay == nil || *oldRelay != *cfg.Relay || oldIdle != m.relayIdleTimeout() {
			m.startRelay(*cfg.Relay)
		}
	} else if oldRelay != nil {
		m.stopRelay()
	}
}

func (m *Mount) AcquireSource() error {
	cfg, _ := m.settings()
	timeout := cfg.SourceTimeout.Duration()
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	m.mu.Lock()
	stale := m.source && !m.isRelay && !m.lastSource.IsZero() && time.Since(m.lastSource) > timeout
	if (m.source && m.isRelay) || stale {
		closer := m.sourceCloser
		m.mu.Unlock()
		if closer != nil {
			_ = closer.Close()
		}
		select {
		case m.sourceGuard <- struct{}{}:
		case <-time.After(time.Second):
			return ErrSourceBusy
		}
	} else {
		m.mu.Unlock()
		select {
		case m.sourceGuard <- struct{}{}:
		default:
			return ErrSourceBusy
		}
	}
	m.mu.Lock()
	m.source = true
	m.isRelay = false
	m.sourceSince = time.Now()
	m.lastSource = m.sourceSince
	m.primaryMeta = Metadata{}
	m.initChunks = nil
	m.mu.Unlock()
	m.observer.SourceConnected(m.Config().Path)
	m.publish("source", map[string]any{"connected": true, "is_relay": false})
	return nil
}

func (m *Mount) AcquireRelaySource() error {
	select {
	case m.sourceGuard <- struct{}{}:
		m.mu.Lock()
		m.source = true
		m.isRelay = true
		m.sourceSince = time.Now()
		m.lastSource = m.sourceSince
		m.primaryMeta = Metadata{}
		m.initChunks = nil
		m.mu.Unlock()
		m.observer.SourceConnected(m.Config().Path)
		m.publish("source", map[string]any{"connected": true, "is_relay": true})
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
	m.isRelay = false
	m.sourceCloser = nil
	m.mu.Unlock()
	m.observer.SourceDisconnected(m.Config().Path)
	m.publish("source", map[string]any{"connected": false, "is_relay": false})
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
	m.primaryMeta = md
	m.metadata = md
	m.fallbackTitled = false
	m.mu.Unlock()
	m.publish("metadata", map[string]any{"title": md.Title, "url": md.URL})
}

func (m *Mount) setFallbackMetadata(md Metadata) {
	m.mu.Lock()
	m.metadata = md
	m.fallbackTitled = true
	m.mu.Unlock()
	m.publish("metadata", map[string]any{"title": md.Title, "url": md.URL})
}

func (m *Mount) setDefaultPrimaryTitle(title string) {
	if title == "" {
		return
	}
	m.mu.Lock()
	if m.primaryMeta.Title != "" {
		m.mu.Unlock()
		return
	}
	m.primaryMeta = Metadata{Title: title}
	if !m.fallbackTitled && m.metadata.Title == "" {
		m.metadata = m.primaryMeta
		m.mu.Unlock()
		m.publish("metadata", map[string]any{"title": title, "url": ""})
		return
	}
	m.mu.Unlock()
}

func (m *Mount) restorePrimaryMetadata(cfg config.Mount) {
	m.mu.Lock()
	if !m.fallbackTitled {
		m.mu.Unlock()
		return
	}
	md := m.primaryMeta
	if md.Title == "" {
		md.Title = cfg.Metadata.Name
	}
	m.metadata = md
	m.fallbackTitled = false
	m.mu.Unlock()
	m.publish("metadata", map[string]any{"title": md.Title, "url": md.URL})
}

func (m *Mount) Metadata() Metadata { m.mu.RLock(); defer m.mu.RUnlock(); return m.metadata }

func (m *Mount) Subscribe(transport string) subscription {
	return m.subscribe(transport, false)
}

func (m *Mount) SubscribeInternal(transport string) subscription {
	return m.subscribe(transport, true)
}

func (m *Mount) SubscribeContext(ctx context.Context, transport string) subscription {
	return m.subscribe(transport, IsInternalSubscriber(ctx))
}

func (m *Mount) subscribe(transport string, internal bool) subscription {
	m.mu.Lock()
	m.nextID++
	id := m.nextID
	prelude := make([][]byte, len(m.initChunks))
	for i := range m.initChunks {
		prelude[i] = append([]byte(nil), m.initChunks[i]...)
	}
	l := &listener{cursor: m.sequence + 1, transport: transport, internal: internal, prelude: prelude}
	m.listeners[id] = l
	m.mu.Unlock()
	if !internal {
		m.observer.ListenerOpened(m.Config().Path, transport)
	}
	return subscription{mount: m, id: id, transport: transport, internal: internal}
}

func bufferBytes(cfg config.Mount, defaults config.Defaults) int {
	bitrate := cfg.Metadata.Bitrate
	if bitrate <= 0 {
		bitrate = defaults.MaxSourceBitrate
	}
	dur := cfg.BufferDuration.Duration()
	if dur <= 0 {
		dur = defaults.BufferDuration.Duration()
	}
	bytes := int(float64(bitrate) / 8 * dur.Seconds())
	if bytes < 16<<10 {
		bytes = 16 << 10
	}
	if bytes > 64<<20 {
		bytes = 64 << 20
	}
	return bytes
}

func (m *Mount) removeListener(id uint64, reason string) {
	m.mu.Lock()
	listener, ok := m.listeners[id]
	if ok {
		delete(m.listeners, id)
	}
	m.mu.Unlock()
	if ok && !listener.internal {
		m.observer.ListenerClosed(m.Config().Path, listener.transport, reason)
	}
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
					m.restorePrimaryMetadata(cfg)
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
			connected, isRelay, last, active := m.source, m.isRelay, m.lastSource, m.active
			m.mu.RUnlock()
			healthy := connected && !last.IsZero() && time.Since(last) <= cfg.SourceTimeout.Duration()
			if healthy {
				continue
			}
			staleTimeout := 2 * cfg.SourceTimeout.Duration()
			if isRelay {
				staleTimeout = m.relayIdleTimeout()
			}
			if connected && !last.IsZero() && staleTimeout > 0 && time.Since(last) >= staleTimeout {
				m.DisconnectSource()
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
		if fb.File != "" {
			if _, err := os.Stat(fb.File); err == nil {
				return "file:" + fb.File
			}
			continue
		}
		if fb.Folder != "" {
			if fi, err := os.Stat(fb.Folder); err == nil && fi.IsDir() {
				return "folder:" + fb.Folder
			}
			continue
		}
		if fb.Playlist != "" {
			if _, err := os.Stat(fb.Playlist); err == nil {
				return "playlist:" + fb.Playlist
			}
			continue
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
			sub := target.SubscribeInternal("fallback")
			go func() {
				defer close(out)
				defer sub.Close("fallback_end")
				for {
					chunk, err := sub.Next(ctx)
					if err != nil {
						return
					}
					select {
					case out <- chunk.Data:
					case <-ctx.Done():
						return
					}
				}
			}()
			return out
		}
		if fb.File != "" && "file:"+fb.File == desired {
			m.setActive("file:" + fb.File)
			title := FormatTrackTitle(fb.File, fb.Title)
			if title != "" {
				m.setFallbackMetadata(Metadata{Title: title})
			}
			go pumpFile(ctx, fb.File, cfg.Profile, cfg.Metadata.Bitrate, out)
			return out
		}
		if fb.Folder != "" && "folder:"+fb.Folder == desired {
			m.setActive("folder:" + fb.Folder)
			files, err := scanAudioFolder(fb.Folder, cfg.Profile)
			if err != nil || len(files) == 0 {
				continue
			}
			go pumpPlaylist(ctx, files, fb.Shuffle, cfg.Profile, cfg.Metadata.Bitrate, fb.Title, m.setFallbackMetadata, out)
			return out
		}
		if fb.Playlist != "" && "playlist:"+fb.Playlist == desired {
			m.setActive("playlist:" + fb.Playlist)
			files, err := parsePlaylist(fb.Playlist, cfg.Profile)
			if err != nil || len(files) == 0 {
				continue
			}
			go pumpPlaylist(ctx, files, fb.Shuffle, cfg.Profile, cfg.Metadata.Bitrate, fb.Title, m.setFallbackMetadata, out)
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
	connected := m.source
	isRelay := m.isRelay
	m.mu.Unlock()
	m.observer.FallbackSwitch(m.Config().Path, active)
	m.publish("source", map[string]any{"active": active, "connected": connected, "is_relay": isRelay})
}

func (m *Mount) broadcast(data []byte, at time.Time) {
	cfg, defaults := m.settings()
	m.mu.Lock()
	m.sequence++
	chunk := Chunk{Sequence: m.sequence, Data: data, At: at}
	if len(m.ring) == 0 {
		m.ringFirst = m.sequence
	}
	m.ring = append(m.ring, chunk)
	m.ringBytes += len(data)
	maxBytes := bufferBytes(cfg, defaults)
	for m.ringBytes > maxBytes && len(m.ring) > 1 {
		m.ringBytes -= len(m.ring[0].Data)
		m.ring[0] = Chunk{}
		m.ring = m.ring[1:]
		m.ringFirst++
	}
	oldNotify := m.notify
	m.notify = make(chan struct{})
	close(oldNotify)
	m.mu.Unlock()
}

func (m *Mount) closeListeners(reason string) {
	path := m.Config().Path
	m.mu.Lock()
	transports := make([]string, 0, len(m.listeners))
	for id, l := range m.listeners {
		delete(m.listeners, id)
		if !l.internal {
			transports = append(transports, l.transport)
		}
	}
	for id, ch := range m.eventSubs {
		delete(m.eventSubs, id)
		close(ch)
	}
	oldNotify := m.notify
	m.notify = make(chan struct{})
	close(oldNotify)
	m.mu.Unlock()
	for _, transport := range transports {
		m.observer.ListenerClosed(path, transport, reason)
	}
}

func (m *Mount) Status() Status {
	cfg, defaults := m.settings()
	m.mu.RLock()
	defer m.mu.RUnlock()
	listeners := 0
	for _, l := range m.listeners {
		if !l.internal {
			listeners++
		}
	}
	return Status{Path: cfg.Path, Profile: cfg.Profile, ContentType: cfg.ContentType, Source: m.source, IsRelay: m.isRelay, Active: m.active, Listeners: listeners, Metadata: m.metadata, LastSource: m.lastSource, BytesIn: m.bytesIn, BytesOut: m.bytesOut, BufferBytes: m.ringBytes, BufferLimit: bufferBytes(cfg, defaults), OldestSequence: m.ringFirst, LatestSequence: m.sequence}
}

func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return os.IsTimeout(err) || strings.Contains(err.Error(), os.ErrDeadlineExceeded.Error())
}

func Copy(ctx context.Context, dst io.Writer, sub subscription) error {
	defer sub.Close("client_closed")
	for {
		chunk, err := sub.Next(ctx)
		if err != nil {
			return err
		}
		n, err := dst.Write(chunk.Data)
		sub.RecordWrite(n)
		if err != nil {
			if IsTimeoutError(err) {
				sub.Close("write_timeout")
			}
			return err
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
