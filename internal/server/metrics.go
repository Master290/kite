package server

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	listeners        *prometheus.GaugeVec
	bytesIn          *prometheus.CounterVec
	bytesOut         *prometheus.CounterVec
	sources          *prometheus.GaugeVec
	disconnects      *prometheus.CounterVec
	fallbackSwitches *prometheus.CounterVec
	configRevision   prometheus.Gauge
	certExpiry       prometheus.Gauge
	registry         *prometheus.Registry
}

func NewMetrics() *Metrics {
	m := &Metrics{
		listeners:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kite_listeners", Help: "Current listener connections."}, []string{"mount", "transport"}),
		bytesIn:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kite_source_bytes_total", Help: "Audio bytes received from sources."}, []string{"mount"}),
		bytesOut:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kite_listener_bytes_total", Help: "Audio bytes sent to listeners."}, []string{"mount", "transport"}),
		sources:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kite_source_connected", Help: "Whether a source is connected."}, []string{"mount"}),
		disconnects:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kite_listener_disconnects_total", Help: "Listener disconnects by reason."}, []string{"mount", "transport", "reason"}),
		fallbackSwitches: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kite_fallback_switches_total", Help: "Fallback source switches."}, []string{"mount", "target"}),
		configRevision:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "kite_config_revision", Help: "Monotonic applied configuration revision."}),
		certExpiry:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "kite_tls_certificate_expiry_timestamp_seconds", Help: "Current certificate expiry as a Unix timestamp."}),
		registry:         prometheus.NewRegistry(),
	}
	m.registry.MustRegister(m.listeners, m.bytesIn, m.bytesOut, m.sources, m.disconnects, m.fallbackSwitches, m.configRevision, m.certExpiry)
	return m
}

func (m *Metrics) Registry() *prometheus.Registry  { return m.registry }
func (m *Metrics) SourceConnected(mount string)    { m.sources.WithLabelValues(mount).Set(1) }
func (m *Metrics) SourceDisconnected(mount string) { m.sources.WithLabelValues(mount).Set(0) }
func (m *Metrics) BytesIn(mount string, n int)     { m.bytesIn.WithLabelValues(mount).Add(float64(n)) }
func (m *Metrics) BytesOut(mount, transport string, n int) {
	m.bytesOut.WithLabelValues(mount, transport).Add(float64(n))
}
func (m *Metrics) ListenerOpened(mount, transport string) {
	m.listeners.WithLabelValues(mount, transport).Inc()
}
func (m *Metrics) ListenerClosed(mount, transport, reason string) {
	m.listeners.WithLabelValues(mount, transport).Dec()
	m.disconnects.WithLabelValues(mount, transport, reason).Inc()
}
func (m *Metrics) FallbackSwitch(mount, target string) {
	m.fallbackSwitches.WithLabelValues(mount, target).Inc()
}
func (m *Metrics) SetRevision(revision uint64)     { m.configRevision.Set(float64(revision)) }
func (m *Metrics) SetCertificateExpiry(unix int64) { m.certExpiry.Set(float64(unix)) }
