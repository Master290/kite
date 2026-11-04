package server

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Master290/kite/internal/config"
	"github.com/Master290/kite/internal/stream"
	"golang.org/x/crypto/bcrypt"
)

func testMP3Frame() []byte {
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	return frame
}

// 1) Busy mount returns 403 Forbidden (not 409 Conflict) and logs the rejected attempt.
func TestBusyMountReturns403Forbidden(t *testing.T) {
	var logBuf safeBuffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	s, ts := publicServer(t)
	s.log = logger
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/radio", bytes.NewReader(testMP3Frame()))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("source", "source")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("busy mount status=%d, want %d (403 Forbidden)", resp.StatusCode, http.StatusForbidden)
	}
	if !strings.Contains(logBuf.String(), "mount busy") || !strings.Contains(logBuf.String(), "/radio") {
		t.Fatalf("expected 403 busy mount rejection log, got: %s", logBuf.String())
	}
}

// 2) Read ice-name, ice-genre, ice-description, ice-url, ice-bitrate, ice-public
// from the source request and use them as stream metadata unless overridden in config.
// Also verify metadata is cleared when the source disconnects (ReleaseSource).
func TestSourceIceHeadersAndConfigOverrides(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "admin")
	t.Setenv("ICECAST_ADMIN_PASSWORD", "icecast-admin")
	t.Setenv("SOURCE_PASSWORD", "source")

	yamlCfg := `
version: 1
server:
  http_address: "127.0.0.1:8000"
admin:
  address: "127.0.0.1:9090"
  token_env: ADMIN_TOKEN
  icecast_admin_password_env: ICECAST_ADMIN_PASSWORD
tls:
  mode: development
mounts:
  - path: /dynamic
    profile: mp3
    source:
      password_env: SOURCE_PASSWORD
  - path: /overridden
    profile: mp3
    source:
      password_env: SOURCE_PASSWORD
    metadata:
      name: "Config Name"
      description: "Config Description"
      genre: "Config Genre"
      url: "https://config.example.com"
      bitrate: 192000
      public: false
`
	parsed, err := config.Parse([]byte(yamlCfg), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, ts := publicServer(t, func(cfg *config.Config) {
		*cfg = *parsed
	})

	frame := testMP3Frame()
	type activePut struct {
		pw   *io.PipeWriter
		done chan struct{}
	}
	var puts []activePut
	defer func() {
		for _, p := range puts {
			_ = p.pw.Close()
			<-p.done
		}
	}()

	for _, path := range []string{"/dynamic", "/overridden"} {
		pr, pw := io.Pipe()
		done := make(chan struct{})
		puts = append(puts, activePut{pw: pw, done: done})
		go func(mountPath string) {
			defer close(done)
			req, err := http.NewRequest(http.MethodPut, ts.URL+mountPath, pr)
			if err != nil {
				return
			}
			req.SetBasicAuth("source", "source")
			req.Header.Set("Content-Type", "audio/mpeg")
			req.Header.Set("ice-name", "Source Stream Name")
			req.Header.Set("ice-description", "Source Stream Description")
			req.Header.Set("ice-genre", "Synthwave")
			req.Header.Set("ice-url", "https://source.example.com")
			req.Header.Set("ice-bitrate", "128")
			req.Header.Set("ice-public", "1")
			req.Header.Set("ice-audio-info", "ice-samplerate=44100;ice-bitrate=128;ice-channels=2")
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
		}(path)
		_, _ = pw.Write(frame)

		m, _ := s.hub.Get(path)
		deadline := time.Now().Add(2 * time.Second)
		for !m.Status().Source && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if !m.Status().Source {
			t.Fatalf("mount %s did not become active", path)
		}
	}

	// Verify /dynamic uses the source's ice-* headers while connected.
	headDyn, err := http.Head(ts.URL + "/dynamic")
	if err != nil {
		t.Fatal(err)
	}
	_ = headDyn.Body.Close()
	if got := headDyn.Header.Get("icy-name"); got != "Source Stream Name" {
		t.Fatalf("dynamic icy-name=%q", got)
	}
	if got := headDyn.Header.Get("icy-description"); got != "Source Stream Description" {
		t.Fatalf("dynamic icy-description=%q", got)
	}
	if got := headDyn.Header.Get("icy-genre"); got != "Synthwave" {
		t.Fatalf("dynamic icy-genre=%q", got)
	}
	if got := headDyn.Header.Get("icy-url"); got != "https://source.example.com" {
		t.Fatalf("dynamic icy-url=%q", got)
	}
	if got := headDyn.Header.Get("icy-br"); got != "128" {
		t.Fatalf("dynamic icy-br=%q", got)
	}
	if got := headDyn.Header.Get("icy-pub"); got != "1" {
		t.Fatalf("dynamic icy-pub=%q", got)
	}

	// Verify /overridden keeps config values (and normalizes 192000 bps -> 192 kbps).
	headOvr, err := http.Head(ts.URL + "/overridden")
	if err != nil {
		t.Fatal(err)
	}
	_ = headOvr.Body.Close()
	if got := headOvr.Header.Get("icy-name"); got != "Config Name" {
		t.Fatalf("overridden icy-name=%q", got)
	}
	if got := headOvr.Header.Get("icy-description"); got != "Config Description" {
		t.Fatalf("overridden icy-description=%q", got)
	}
	if got := headOvr.Header.Get("icy-genre"); got != "Config Genre" {
		t.Fatalf("overridden icy-genre=%q", got)
	}
	if got := headOvr.Header.Get("icy-url"); got != "https://config.example.com" {
		t.Fatalf("overridden icy-url=%q", got)
	}
	if got := headOvr.Header.Get("icy-br"); got != "192" {
		t.Fatalf("overridden icy-br=%q, want 192 (normalized kbps)", got)
	}
	if got := headOvr.Header.Get("icy-pub"); got != "0" {
		t.Fatalf("overridden icy-pub=%q, want 0 (config public: false)", got)
	}

	mDyn, _ := s.hub.Get("/dynamic")
	if meta := mDyn.StreamMetadata(); !meta.Public || meta.Bitrate != 128 || meta.Name != "Source Stream Name" {
		t.Fatalf("dynamic StreamMetadata=%+v", meta)
	}
	if mDyn.SourceIP() == "" {
		t.Fatal("expected non-empty SourceIP while source is connected")
	}

	// Disconnect /dynamic source and verify stale source metadata is cleared on ReleaseSource.
	_ = puts[0].pw.Close()
	<-puts[0].done
	if meta := mDyn.StreamMetadata(); meta.Name != "" || meta.Genre != "" || meta.Bitrate != 0 || meta.Public {
		t.Fatalf("expected dynamic StreamMetadata to be cleared after source release, got %+v", meta)
	}
	if ip := mDyn.SourceIP(); ip != "" {
		t.Fatalf("expected empty SourceIP after source release, got %q", ip)
	}
}

// 3) Burst-on-connect over HTTP: new listeners receive burst_bytes aligned to a frame boundary.
func TestHTTPListenerBurstOnConnect(t *testing.T) {
	s, ts := publicServer(t, func(cfg *config.Config) {
		cfg.Defaults.WriteInterval = config.Duration(100 * time.Millisecond)
		cfg.Mounts[0].BurstBytes = 417 * 20 // 20 MP3 frames = 8,340 bytes
	})
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)

	frame := testMP3Frame()
	for i := 0; i < 50; i++ {
		f := append([]byte(nil), frame...)
		f[4] = byte(i)
		if err := m.Write(f); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/radio", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Without writing any further frames, the listener must immediately receive the 20-frame burst.
	burst := readBodyBytes(t, resp.Body, 417*20, time.Second)
	if burst[0] != 0xff || burst[1]&0xe0 != 0xe0 {
		t.Fatalf("burst does not start at MP3 sync word: %x %x", burst[0], burst[1])
	}
	if burst[4] != 30 {
		t.Fatalf("expected burst to start at frame 30 (20 frames behind 50), got %d", burst[4])
	}
}

// 4) /status-json.xsl matches Icecast 2.4 schema:
// - omitted "source" when 0 live public mounts
// - object when 1 live public mount
// - array when 2+ live public mounts
// - song= keeps full "Artist - Title" in title and omits artist; explicit artist= emits artist
// - /api/v1 remains unchanged.
func TestStatusJSONMatchesIcecast24Schema(t *testing.T) {
	const sampleIcecastSingleJSON = `{
		"icestats": {
			"admin": "icemaster@localhost",
			"host": "localhost",
			"location": "Earth",
			"server_id": "Icecast 2.4.4",
			"server_start": "Mon, 03 Nov 2026 20:00:00 +0000",
			"server_start_iso8601": "2026-11-03T20:00:00+0000",
			"source": {
				"audio_info": "ice-samplerate=44100;ice-bitrate=128;ice-channels=2",
				"bitrate": 128,
				"genre": "Electronic",
				"listener_peak": 1,
				"listeners": 1,
				"listenurl": "http://localhost:8000/radio",
				"server_description": "Main Station",
				"server_name": "Kite Test",
				"server_type": "audio/mpeg",
				"stream_start": "Mon, 03 Nov 2026 20:00:00 +0000",
				"stream_start_iso8601": "2026-11-03T20:00:00+0000",
				"title": "Daft Punk - One More Time"
			}
		}
	}`

	var sampleMap map[string]any
	if err := json.Unmarshal([]byte(sampleIcecastSingleJSON), &sampleMap); err != nil {
		t.Fatal(err)
	}
	sampleStats := sampleMap["icestats"].(map[string]any)
	sampleSource := sampleStats["source"].(map[string]any)

	s, ts := publicServer(t)

	// 4a) With 0 live sources (all mounts offline), "source" must be omitted entirely.
	resp0, err := http.Get(ts.URL + "/status-json.xsl")
	if err != nil {
		t.Fatal(err)
	}
	var gotZero map[string]any
	if err := json.NewDecoder(resp0.Body).Decode(&gotZero); err != nil {
		t.Fatal(err)
	}
	_ = resp0.Body.Close()
	zeroStats := gotZero["icestats"].(map[string]any)
	if _, hasSource := zeroStats["source"]; hasSource {
		t.Fatalf("expected 'source' key to be omitted when no mounts have a live source, got %+v", zeroStats["source"])
	}

	// 4b) Activate 1 live public mount with song= metadata (not split; artist omitted).
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)
	m.SetSourceStreamMetadata(stream.SourceStreamMetadata{
		Description: "Main Station",
		Genre:       "Electronic",
		Bitrate:     128000, // bps normalized to 128 kbps
		AudioInfo:   "ice-samplerate=44100;ice-bitrate=128000;ice-channels=2",
	})
	m.SetMetadata(stream.Metadata{Title: "Daft Punk - One More Time"})

	lCtx, lCancel := context.WithCancel(context.Background())
	defer lCancel()
	lReq, _ := http.NewRequestWithContext(lCtx, http.MethodGet, ts.URL+"/radio", nil)
	lResp, err := http.DefaultClient.Do(lReq)
	if err != nil {
		t.Fatal(err)
	}
	defer lResp.Body.Close()
	waitForListeners(t, s, "/radio", 1)

	resp, err := http.Get(ts.URL + "/status-json.xsl")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var gotMap map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&gotMap); err != nil {
		t.Fatal(err)
	}
	gotStats := gotMap["icestats"].(map[string]any)
	for k := range sampleStats {
		if _, exists := gotStats[k]; !exists {
			t.Fatalf("icestats missing required key %q in %+v", k, gotStats)
		}
	}
	gotSource, ok := gotStats["source"].(map[string]any)
	if !ok {
		t.Fatalf("expected source to be a JSON object when 1 live mount exists, got %T (%+v)", gotStats["source"], gotStats["source"])
	}
	for k := range sampleSource {
		if _, exists := gotSource[k]; !exists {
			t.Fatalf("source object missing required Icecast 2.4 key %q in %+v", k, gotSource)
		}
	}
	if _, hasArtist := gotSource["artist"]; hasArtist {
		t.Fatalf("expected artist to be omitted when metadata came from song=, got %q", gotSource["artist"])
	}
	if gotSource["server_name"] != "Kite Test" ||
		gotSource["server_description"] != "Main Station" ||
		gotSource["server_type"] != "audio/mpeg" ||
		gotSource["genre"] != "Electronic" ||
		gotSource["bitrate"] != float64(128) ||
		gotSource["title"] != "Daft Punk - One More Time" ||
		gotSource["listeners"] != float64(1) ||
		gotSource["listener_peak"] != float64(1) ||
		gotSource["audio_info"] != "ice-samplerate=44100;ice-bitrate=128;ice-channels=2" {
		t.Fatalf("unexpected source fields: %+v", gotSource)
	}

	// Verify explicit artist + title sets both artist and title.
	m.SetMetadata(stream.Metadata{Artist: "Daft Punk", Song: "One More Time"})
	respExplicit, err := http.Get(ts.URL + "/status-json.xsl")
	if err != nil {
		t.Fatal(err)
	}
	var gotExplicit map[string]any
	_ = json.NewDecoder(respExplicit.Body).Decode(&gotExplicit)
	_ = respExplicit.Body.Close()
	srcExplicit := gotExplicit["icestats"].(map[string]any)["source"].(map[string]any)
	if srcExplicit["artist"] != "Daft Punk" || srcExplicit["title"] != "One More Time" {
		t.Fatalf("explicit artist/title mismatch: %+v", srcExplicit)
	}

	// 4c) Add a second non-hidden mount with public:false (like ffmpeg default ice-public:0):
	// while offline, source must STILL be a single object; when live, it MUST be included in status-json
	// (only hidden:true excludes a live mount).
	cfg := s.Config()
	cfg.Mounts = append(cfg.Mounts, config.Mount{
		Path:        "/jazz",
		Profile:     "mp3",
		ContentType: "audio/mpeg",
		Source:      cfg.Mounts[0].Source,
		Metadata:    config.Metadata{Name: "Jazz Channel", Genre: "Jazz", Bitrate: 160000, Public: false, PublicSet: true},
	})
	s.applyConfig(cfg)

	// Also make the hidden mount (/private) live to verify hidden:true is still excluded.
	mPriv, _ := s.hub.Get("/private")
	acquireTestSource(t, mPriv)

	respOffline2, err := http.Get(ts.URL + "/status-json.xsl")
	if err != nil {
		t.Fatal(err)
	}
	var gotStillOne map[string]any
	_ = json.NewDecoder(respOffline2.Body).Decode(&gotStillOne)
	_ = respOffline2.Body.Close()
	if _, ok := gotStillOne["icestats"].(map[string]any)["source"].(map[string]any); !ok {
		t.Fatalf("expected offline mount /jazz and hidden mount /private to be excluded from status-json, got %T", gotStillOne["icestats"].(map[string]any)["source"])
	}

	// Now connect a live source (with ice-public:0) to /jazz and verify source becomes a 2-element array.
	mJazz, _ := s.hub.Get("/jazz")
	acquireTestSource(t, mJazz)
	mJazz.SetSourceStreamMetadata(stream.SourceStreamMetadata{Public: false, PublicSet: true})

	resp2, err := http.Get(ts.URL + "/status-json.xsl")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	var gotMulti map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&gotMulti); err != nil {
		t.Fatal(err)
	}
	multiStats := gotMulti["icestats"].(map[string]any)
	multiSources, ok := multiStats["source"].([]any)
	if !ok || len(multiSources) != 2 {
		t.Fatalf("expected source to be a 2-element array when 2 live mounts exist, got %T (%+v)", multiStats["source"], multiStats["source"])
	}

	// Verify /api/v1/mounts remains unchanged.
	adminMux := http.NewServeMux()
	s.registerAdmin(adminMux)
	apiReq, _ := http.NewRequest(http.MethodGet, "http://admin/api/v1/mounts", nil)
	apiReq.Header.Set("Authorization", "Bearer admin")
	apiRec := newTestRecorder()
	adminMux.ServeHTTP(apiRec, apiReq)
	if apiRec.Code != http.StatusOK {
		t.Fatalf("/api/v1/mounts status=%d", apiRec.Code)
	}
	var v1Mounts []stream.Status
	if err := json.Unmarshal(apiRec.Body.Bytes(), &v1Mounts); err != nil {
		t.Fatalf("/api/v1/mounts decode failed: %v", err)
	}
	if len(v1Mounts) != 3 {
		t.Fatalf("/api/v1/mounts len=%d, want 3", len(v1Mounts))
	}
}

// 5) XML /admin/stats, /admin/listmounts, /admin/listclients?mount=,
// /admin/killclient?mount=&id=, /admin/killsource?mount= with separate Icecast admin Basic auth.
func TestIcecastAdminXMLEndpoints(t *testing.T) {
	const (
		sampleAdminStatsXML = `<?xml version="1.0"?>
<icestats>
  <admin>icemaster@localhost</admin>
  <clients>2</clients>
  <connections>2</connections>
  <host>localhost</host>
  <listeners>1</listeners>
  <location>Earth</location>
  <server_id>Icecast 2.4.4</server_id>
  <sources>1</sources>
  <source mount="/radio">
    <bitrate>128</bitrate>
    <genre>Rock</genre>
    <listener_peak>1</listener_peak>
    <listeners>1</listeners>
    <listenurl>http://localhost:8000/radio</listenurl>
    <public>1</public>
    <server_description>Rock Desc</server_description>
    <server_name>Kite Test</server_name>
    <server_type>audio/mpeg</server_type>
    <title>Band - Track</title>
  </source>
</icestats>`
		sampleListMountsXML = `<?xml version="1.0"?>
<icestats>
  <source mount="/radio">
    <listeners>1</listeners>
    <Connected>10</Connected>
    <content-type>audio/mpeg</content-type>
  </source>
</icestats>`
		sampleListClientsXML = `<?xml version="1.0"?>
<icestats>
  <source mount="/radio">
    <listeners>1</listeners>
    <listener id="1">
      <ID>1</ID>
      <IP>127.0.0.1</IP>
      <UserAgent>WinampMPEG/5.0</UserAgent>
      <Connected>3</Connected>
    </listener>
  </source>
</icestats>`
		sampleActionResponseXML = `<?xml version="1.0"?>
<iceresponse>
  <message>Source removed</message>
  <return>1</return>
</iceresponse>`
	)

	var statsSample xmlIceStats
	if err := xml.Unmarshal([]byte(sampleAdminStatsXML), &statsSample); err != nil {
		t.Fatal(err)
	}
	var mountsSample xmlListMounts
	if err := xml.Unmarshal([]byte(sampleListMountsXML), &mountsSample); err != nil {
		t.Fatal(err)
	}
	var clientsSample xmlListClients
	if err := xml.Unmarshal([]byte(sampleListClientsXML), &clientsSample); err != nil {
		t.Fatal(err)
	}
	var respSample xmlIceResponse
	if err := xml.Unmarshal([]byte(sampleActionResponseXML), &respSample); err != nil {
		t.Fatal(err)
	}

	s, ts := publicServer(t)

	// Verify all 5 endpoints require Icecast admin Basic auth.
	for _, ep := range []string{
		"/admin/stats",
		"/admin/listmounts",
		"/admin/listclients?mount=/radio",
		"/admin/killclient?mount=/radio&id=1",
		"/admin/killsource?mount=/radio",
	} {
		unauthResp, err := http.Get(ts.URL + ep)
		if err != nil {
			t.Fatal(err)
		}
		_ = unauthResp.Body.Close()
		if unauthResp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET %s without auth status=%d, want 401", ep, unauthResp.StatusCode)
		}
		if !strings.HasPrefix(unauthResp.Header.Get("WWW-Authenticate"), "Basic ") {
			t.Fatalf("GET %s WWW-Authenticate=%q", ep, unauthResp.Header.Get("WWW-Authenticate"))
		}
	}
	// Requests without an Authorization header must not be counted as failures.
	if gotLen := s.getAuthLimiter().len(); gotLen != 0 {
		t.Fatalf("unauthenticated requests without Authorization header should not record failures, got len=%d", gotLen)
	}

	// Start a live HTTP PUT source that stays open until killed via /admin/killsource.
	pr, pw := io.Pipe()
	defer pw.Close()
	sourceDone := make(chan struct{})
	go func() {
		defer close(sourceDone)
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/radio", pr)
		req.SetBasicAuth("source", "source")
		req.Header.Set("Content-Type", "audio/mpeg")
		req.Header.Set("ice-genre", "Rock")
		req.Header.Set("ice-description", "Rock Desc")
		req.Header.Set("ice-bitrate", "128")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	_, _ = pw.Write(testMP3Frame())

	m, _ := s.hub.Get("/radio")
	deadline := time.Now().Add(2 * time.Second)
	for !m.Status().Source && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !m.Status().Source {
		t.Fatal("timed out waiting for live source")
	}
	m.SetMetadata(stream.Metadata{Title: "Band - Track"})

	// Connect a listener with a custom User-Agent.
	lCtx, lCancel := context.WithCancel(context.Background())
	defer lCancel()
	lReq, _ := http.NewRequestWithContext(lCtx, http.MethodGet, ts.URL+"/radio", nil)
	lReq.Header.Set("User-Agent", "WinampMPEG/5.0")
	lResp, err := http.DefaultClient.Do(lReq)
	if err != nil {
		t.Fatal(err)
	}
	defer lResp.Body.Close()
	waitForListeners(t, s, "/radio", 1)

	doAdminXML := func(path string, target any) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth("admin", "icecast-admin")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("GET %s status=%d body=%s", path, resp.StatusCode, b)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "xml") {
			t.Fatalf("GET %s Content-Type=%q, want xml", path, ct)
		}
		if err := xml.NewDecoder(resp.Body).Decode(target); err != nil {
			t.Fatalf("GET %s XML decode error: %v", path, err)
		}
	}

	// 5a) /admin/stats: <sources> must count only live sources (1, not 2), and offline /private must have <connected>0</connected>.
	var gotStats xmlIceStats
	doAdminXML("/admin/stats", &gotStats)
	if gotStats.ServerID != statsSample.ServerID || gotStats.Listeners != 1 || gotStats.Sources != 1 {
		t.Fatalf("/admin/stats unexpected: %+v", gotStats)
	}
	var radioStats, privStats *xmlStatsSource
	for i := range gotStats.Source {
		switch gotStats.Source[i].Mount {
		case "/radio":
			radioStats = &gotStats.Source[i]
		case "/private":
			privStats = &gotStats.Source[i]
		}
	}
	if radioStats == nil || radioStats.Genre != "Rock" || radioStats.Bitrate != 128 || radioStats.Artist != "" || radioStats.Title != "Band - Track" || radioStats.Listeners != 1 {
		t.Fatalf("/admin/stats /radio=%+v", radioStats)
	}
	if privStats == nil || privStats.Connected != 0 || privStats.SourceIP != "" {
		t.Fatalf("/admin/stats offline /private=%+v", privStats)
	}

	// 5b) /admin/listmounts: must list ONLY mounts with a live source (1 mount /radio, omitting offline /private).
	var gotMounts xmlListMounts
	doAdminXML("/admin/listmounts", &gotMounts)
	if len(gotMounts.Source) != 1 || gotMounts.Source[0].Mount != "/radio" || gotMounts.Source[0].ContentType != "audio/mpeg" {
		t.Fatalf("/admin/listmounts=%+v", gotMounts)
	}

	// 5c) /admin/listclients?mount=/radio
	var gotClients xmlListClients
	doAdminXML("/admin/listclients?mount=/radio", &gotClients)
	if gotClients.Source.Mount != "/radio" || gotClients.Source.Listeners != 1 || len(gotClients.Source.Listener) != 1 {
		t.Fatalf("/admin/listclients=%+v", gotClients)
	}
	clientEntry := gotClients.Source.Listener[0]
	if clientEntry.UserAgent != "WinampMPEG/5.0" || clientEntry.ID == 0 || clientEntry.IP == "" {
		t.Fatalf("listener entry=%+v", clientEntry)
	}

	// 5d) /admin/killclient?mount=/radio&id=<id>
	var killClientResp xmlIceResponse
	doAdminXML("/admin/killclient?mount=/radio&id="+itoa(int(clientEntry.ID)), &killClientResp)
	if killClientResp.Return != 1 {
		t.Fatalf("killclient response=%+v", killClientResp)
	}
	var afterKillClients xmlListClients
	doAdminXML("/admin/listclients?mount=/radio", &afterKillClients)
	if afterKillClients.Source.Listeners != 0 || len(afterKillClients.Source.Listener) != 0 {
		t.Fatalf("expected 0 clients after killclient, got %+v", afterKillClients)
	}

	// Connect a second listener before /admin/killsource to verify killsource disconnects listeners when no fallback is configured.
	lCtx2, lCancel2 := context.WithCancel(context.Background())
	defer lCancel2()
	lReq2, _ := http.NewRequestWithContext(lCtx2, http.MethodGet, ts.URL+"/radio", nil)
	lResp2, err := http.DefaultClient.Do(lReq2)
	if err != nil {
		t.Fatal(err)
	}
	defer lResp2.Body.Close()
	waitForListeners(t, s, "/radio", 1)

	// 5e) /admin/killsource?mount=/radio
	var killSourceResp xmlIceResponse
	doAdminXML("/admin/killsource?mount=/radio", &killSourceResp)
	if killSourceResp.Return != respSample.Return {
		t.Fatalf("killsource response=%+v", killSourceResp)
	}
	if m.Status().Source {
		t.Fatal("expected source to be disconnected after /admin/killsource")
	}
	waitForListeners(t, s, "/radio", 0)
	select {
	case <-sourceDone:
	case <-time.After(time.Second):
		t.Fatal("source PUT handler did not exit after /admin/killsource")
	}
}

// 6) Serve /<mount>.m3u and /<mount>.xspf playlists, including public_url / hostname and trusted_proxies.
func TestMountM3UAndXSPFPlaylists(t *testing.T) {
	const sampleXSPF = `<?xml version="1.0" encoding="UTF-8"?>
<playlist xmlns="http://xspf.org/ns/0/" version="1">
  <title>Kite Test</title>
  <trackList>
    <track>
      <location>http://localhost:8000/radio</location>
      <title>Artist - Song</title>
      <annotation>Station Desc</annotation>
      <info>https://radio.example</info>
    </track>
  </trackList>
</playlist>`

	var wantXSPF xspfPlaylist
	if err := xml.Unmarshal([]byte(sampleXSPF), &wantXSPF); err != nil {
		t.Fatal(err)
	}

	s, ts := publicServer(t)
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)
	m.SetSourceStreamMetadata(stream.SourceStreamMetadata{
		Description: "Station Desc",
		URL:         "https://radio.example",
	})
	m.SetMetadata(stream.Metadata{Title: "Artist - Song"})

	// Test /radio.m3u
	m3uResp, err := http.Get(ts.URL + "/radio.m3u")
	if err != nil {
		t.Fatal(err)
	}
	m3uBody, _ := io.ReadAll(m3uResp.Body)
	_ = m3uResp.Body.Close()
	if m3uResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /radio.m3u status=%d", m3uResp.StatusCode)
	}
	if ct := m3uResp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "audio/x-mpegurl") {
		t.Fatalf("/radio.m3u Content-Type=%q", ct)
	}
	if !strings.Contains(string(m3uBody), "#EXTM3U") || !strings.Contains(string(m3uBody), ts.URL+"/radio\n") {
		t.Fatalf("/radio.m3u body=%q", string(m3uBody))
	}

	// Test /radio.xspf (song= is not split; creator is omitted unless artist was set explicitly)
	xspfResp, err := http.Get(ts.URL + "/radio.xspf")
	if err != nil {
		t.Fatal(err)
	}
	defer xspfResp.Body.Close()
	if xspfResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /radio.xspf status=%d", xspfResp.StatusCode)
	}
	if ct := xspfResp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/xspf+xml") {
		t.Fatalf("/radio.xspf Content-Type=%q", ct)
	}
	var gotXSPF xspfPlaylist
	if err := xml.NewDecoder(xspfResp.Body).Decode(&gotXSPF); err != nil {
		t.Fatalf("/radio.xspf XML decode failed: %v", err)
	}
	if gotXSPF.Version != wantXSPF.Version ||
		gotXSPF.Title != wantXSPF.Title ||
		gotXSPF.Creator != "" ||
		len(gotXSPF.TrackList.Track) != 1 ||
		gotXSPF.TrackList.Track[0].Location != ts.URL+"/radio" ||
		gotXSPF.TrackList.Track[0].Creator != "" ||
		gotXSPF.TrackList.Track[0].Title != wantXSPF.TrackList.Track[0].Title ||
		gotXSPF.TrackList.Track[0].Annotation != wantXSPF.TrackList.Track[0].Annotation ||
		gotXSPF.TrackList.Track[0].Info != wantXSPF.TrackList.Track[0].Info {
		t.Fatalf("/radio.xspf parsed=%+v, want=%+v", gotXSPF, wantXSPF)
	}

	// Test URL priority: public_url > hostname > trusted X-Forwarded-Host > Host.
	cfg := s.Config()
	cfg.Server.Hostname = "stream.configured.example:8000"
	s.applyConfig(cfg)

	reqUntrusted, _ := http.NewRequest(http.MethodGet, ts.URL+"/radio.m3u", nil)
	reqUntrusted.Header.Set("X-Forwarded-Proto", "https")
	reqUntrusted.Header.Set("X-Forwarded-Host", "evil.example.com")
	respUntrusted, err := http.DefaultClient.Do(reqUntrusted)
	if err != nil {
		t.Fatal(err)
	}
	bodyUntrusted, _ := io.ReadAll(respUntrusted.Body)
	_ = respUntrusted.Body.Close()
	if !strings.Contains(string(bodyUntrusted), "http://stream.configured.example:8000/radio\n") {
		t.Fatalf("expected configured hostname when proxy is untrusted, got %q", string(bodyUntrusted))
	}

	// Behind a trusted proxy: hostname still wins over X-Forwarded-Host, while X-Forwarded-Proto sets scheme.
	cfg.Server.TrustedProxies = []string{"127.0.0.1/32", "::1/128"}
	s.applyConfig(cfg)
	reqHostnameOverXFH, _ := http.NewRequest(http.MethodGet, ts.URL+"/radio.m3u", nil)
	reqHostnameOverXFH.Header.Set("X-Forwarded-Proto", "https")
	reqHostnameOverXFH.Header.Set("X-Forwarded-Host", "edge.radio.example")
	respHostnameOverXFH, err := http.DefaultClient.Do(reqHostnameOverXFH)
	if err != nil {
		t.Fatal(err)
	}
	bodyHostnameOverXFH, _ := io.ReadAll(respHostnameOverXFH.Body)
	_ = respHostnameOverXFH.Body.Close()
	if !strings.Contains(string(bodyHostnameOverXFH), "https://stream.configured.example:8000/radio\n") {
		t.Fatalf("expected hostname to take precedence over trusted X-Forwarded-Host, got %q", string(bodyHostnameOverXFH))
	}

	// public_url wins over both hostname and trusted X-Forwarded-Host.
	cfg.Server.PublicURL = "https://public.canonical.example:8443"
	s.applyConfig(cfg)
	reqPublicURL, _ := http.NewRequest(http.MethodGet, ts.URL+"/radio.m3u", nil)
	reqPublicURL.Header.Set("X-Forwarded-Proto", "http")
	reqPublicURL.Header.Set("X-Forwarded-Host", "edge.radio.example")
	respPublicURL, err := http.DefaultClient.Do(reqPublicURL)
	if err != nil {
		t.Fatal(err)
	}
	bodyPublicURL, _ := io.ReadAll(respPublicURL.Body)
	_ = respPublicURL.Body.Close()
	if !strings.Contains(string(bodyPublicURL), "https://public.canonical.example:8443/radio\n") {
		t.Fatalf("expected public_url to take precedence over hostname and X-Forwarded-Host, got %q", string(bodyPublicURL))
	}

	// When public_url and hostname are unset, trusted X-Forwarded-Host wins over Host.
	cfg.Server.PublicURL = ""
	cfg.Server.Hostname = ""
	s.applyConfig(cfg)
	reqTrusted, _ := http.NewRequest(http.MethodGet, ts.URL+"/radio.m3u", nil)
	reqTrusted.Header.Set("X-Forwarded-Proto", "https")
	reqTrusted.Header.Set("X-Forwarded-Host", "edge.radio.example")
	respTrusted, err := http.DefaultClient.Do(reqTrusted)
	if err != nil {
		t.Fatal(err)
	}
	bodyTrusted, _ := io.ReadAll(respTrusted.Body)
	_ = respTrusted.Body.Close()
	if !strings.Contains(string(bodyTrusted), "https://edge.radio.example/radio\n") {
		t.Fatalf("expected X-Forwarded-Proto/Host when behind trusted proxy, got %q", string(bodyTrusted))
	}

	// When X-Forwarded-Host is trusted and X-Forwarded-Proto is missing, keep the scheme of the actual connection (e.g. https on TLS).
	tlsReq := newTestRequest(http.MethodGet, "https://internal.local/radio.m3u", nil)
	tlsReq.RemoteAddr = "127.0.0.1:43210"
	tlsReq.Header.Set("X-Forwarded-Host", "edge.radio.example")
	if gotBase := s.requestBaseURL(tlsReq); gotBase != "https://edge.radio.example" {
		t.Fatalf("expected TLS connection scheme to be preserved when X-Forwarded-Proto is missing, got %q", gotBase)
	}

	// Unknown mount playlists return 404.
	for _, missing := range []string{"/missing.m3u", "/missing.xspf"} {
		r404, err := http.Get(ts.URL + missing)
		if err != nil {
			t.Fatal(err)
		}
		_ = r404.Body.Close()
		if r404.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s status=%d, want 404", missing, r404.StatusCode)
		}
	}
}

// 7) Auth separation (Icecast admin password vs Bearer admin API token), per-IP lockout,
// pre-bcrypt rate limiting, and redacted logging.
func TestAuthSeparationAndLockout(t *testing.T) {
	var logBuf safeBuffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	hash, err := bcrypt.GenerateFromPassword([]byte("source-secret"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	s, ts := publicServer(t, func(cfg *config.Config) {
		cfg.Mounts[0].Source = config.SourceCredential{
			Username:       "source",
			PasswordBcrypt: string(hash),
		}
	})
	s.log = logger

	nowMu := sync.Mutex{}
	fakeNow := time.Now()
	limiter := s.getAuthLimiter()
	limiter.baseDelay = 5 * time.Second
	limiter.nowFn = func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return fakeNow
	}
	advanceTime := func(d time.Duration) {
		nowMu.Lock()
		fakeNow = fakeNow.Add(d)
		nowMu.Unlock()
	}

	// 7a) Bearer admin API token ("admin") MUST NOT be accepted on /admin/stats.
	reqBearerAsBasic, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
	reqBearerAsBasic.SetBasicAuth("admin", "admin")
	resp, err := http.DefaultClient.Do(reqBearerAsBasic)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Bearer admin token must be rejected on /admin/stats, got status=%d", resp.StatusCode)
	}
	limiter.recordSuccess("127.0.0.1", "icecast_admin")

	// 7b) 5 failed attempts on /admin/stats return 401; 6th attempt returns 429 even with valid password.
	for i := 1; i <= 5; i++ {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
		req.SetBasicAuth("accidental-secret-in-username", "wrong-secret-pass")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Body.Close()
		if r.StatusCode != http.StatusUnauthorized {
			t.Fatalf("attempt %d status=%d, want 401", i, r.StatusCode)
		}
	}

	// 6th attempt with the CORRECT password while locked out must still return 429 Too Many Requests.
	reqLocked, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
	reqLocked.SetBasicAuth("admin", "icecast-admin")
	respLocked, err := http.DefaultClient.Do(reqLocked)
	if err != nil {
		t.Fatal(err)
	}
	_ = respLocked.Body.Close()
	if respLocked.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("locked out attempt status=%d, want 429", respLocked.StatusCode)
	}
	if respLocked.Header.Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 429 lockout response")
	}

	// Advance clock past the 5s initial lockout window; valid password now succeeds.
	advanceTime(6 * time.Second)
	reqOK, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
	reqOK.SetBasicAuth("admin", "icecast-admin")
	respOK, err := http.DefaultClient.Do(reqOK)
	if err != nil {
		t.Fatal(err)
	}
	_ = respOK.Body.Close()
	if respOK.StatusCode != http.StatusOK {
		t.Fatalf("after lockout expiry status=%d, want 200", respOK.StatusCode)
	}

	// 7c) Source auth (with bcrypt) also locks out after 5 failures and logs redacted credentials.
	for i := 1; i <= 5; i++ {
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/radio", bytes.NewReader(testMP3Frame()))
		req.SetBasicAuth("source", "bad-source-pass")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Body.Close()
		if r.StatusCode != http.StatusUnauthorized {
			t.Fatalf("source attempt %d status=%d, want 401", i, r.StatusCode)
		}
	}
	reqSourceLocked, _ := http.NewRequest(http.MethodPut, ts.URL+"/radio", bytes.NewReader(testMP3Frame()))
	reqSourceLocked.SetBasicAuth("source", "source-secret")
	respSourceLocked, err := http.DefaultClient.Do(reqSourceLocked)
	if err != nil {
		t.Fatal(err)
	}
	_ = respSourceLocked.Body.Close()
	if respSourceLocked.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("locked out source attempt status=%d, want 429", respSourceLocked.StatusCode)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] in auth failure logs, got: %s", logs)
	}
	if !strings.Contains(logs, `"user":"[unknown]"`) {
		t.Fatalf("expected unknown username to be logged as [unknown], got: %s", logs)
	}
	if !strings.Contains(logs, `"retry_after":"5s"`) {
		t.Fatalf("expected retry_after to be logged as duration string \"5s\", got: %s", logs)
	}
	if strings.Contains(logs, "wrong-secret-pass") || strings.Contains(logs, "bad-source-pass") || strings.Contains(logs, "accidental-secret-in-username") {
		t.Fatalf("auth failure logs leaked plaintext credentials: %s", logs)
	}
}

// 7d) X-Forwarded-For right-to-left resolution (spoofed leftmost entry), per-(IP, scope) reset
// isolation, no-credential requests not counted, Bearer prefix requirement, and TTL + LRU eviction.
func TestAuthLimiterXFFRightToLeftAndPerScopeAndEviction(t *testing.T) {
	s, ts := publicServer(t, func(cfg *config.Config) {
		cfg.Server.TrustedProxies = []string{"127.0.0.1/32", "::1/128", "10.0.0.254/32"}
	})

	// 1. Unauthenticated requests (no Authorization header) must NOT increment failure counters.
	for i := 0; i < 15; i++ {
		resp, err := http.Get(ts.URL + "/admin/stats")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthenticated request %d status=%d, want 401", i, resp.StatusCode)
		}
	}
	if gotLen := s.getAuthLimiter().len(); gotLen != 0 {
		t.Fatalf("expected 0 limiter entries after unauthenticated requests, got %d", gotLen)
	}

	// 2. XFF right-to-left resolution: rotating a spoofed leftmost XFF entry
	// ("172.16.0.N, 10.0.0.1, 10.0.0.254") through trusted proxies (127.0.0.1 and 10.0.0.254)
	// must resolve to the first untrusted IP from the right (10.0.0.1) and lock it out after 5 failures.
	for i := 1; i <= 5; i++ {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
		req.Header.Set("X-Forwarded-For", "172.16.0."+itoa(i)+", 10.0.0.1, 10.0.0.254")
		req.SetBasicAuth("admin", "wrong-pass")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("spoofed XFF attempt %d status=%d, want 401", i, resp.StatusCode)
		}
	}
	// 6th attempt from 10.0.0.1 with a fresh spoofed leftmost IP must return 429 Too Many Requests.
	reqSpoof6, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
	reqSpoof6.Header.Set("X-Forwarded-For", "172.16.0.99, 10.0.0.1, 10.0.0.254")
	reqSpoof6.SetBasicAuth("admin", "icecast-admin")
	respSpoof6, err := http.DefaultClient.Do(reqSpoof6)
	if err != nil {
		t.Fatal(err)
	}
	_ = respSpoof6.Body.Close()
	if respSpoof6.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 lockout for 10.0.0.1 despite spoofed leftmost XFF, got %d", respSpoof6.StatusCode)
	}
	// Another untrusted client IP (10.0.0.2) is unaffected.
	reqOtherIP, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
	reqOtherIP.Header.Set("X-Forwarded-For", "198.51.100.7, 10.0.0.2")
	reqOtherIP.SetBasicAuth("admin", "icecast-admin")
	respOtherIP, err := http.DefaultClient.Do(reqOtherIP)
	if err != nil {
		t.Fatal(err)
	}
	_ = respOtherIP.Body.Close()
	if respOtherIP.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for unaffected client IP 10.0.0.2, got %d", respOtherIP.StatusCode)
	}
	// Verify parseSourceStreamMetadata also records the right-to-left untrusted IP as SourceIP.
	srcReq := newTestRequest(http.MethodPut, "http://radio.example/radio", nil)
	srcReq.RemoteAddr = "127.0.0.1:54321"
	srcReq.Header.Set("X-Forwarded-For", "172.16.0.88, 10.0.0.1, 10.0.0.254")
	if gotIP := s.parseSourceStreamMetadata(srcReq).SourceIP; gotIP != "10.0.0.1" {
		t.Fatalf("SourceIP=%q, want 10.0.0.1", gotIP)
	}

	// 3. Per-(IP, scope) isolation: interleaving 4 wrong icecast_admin guesses with 1 successful
	// source auth on the same IP (10.0.0.50) must NOT reset the icecast_admin failure counter.
	for i := 1; i <= 4; i++ {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
		req.Header.Set("X-Forwarded-For", "10.0.0.50")
		req.SetBasicAuth("admin", "wrong")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Body.Close()
		if r.StatusCode != http.StatusUnauthorized {
			t.Fatalf("admin guess %d status=%d, want 401", i, r.StatusCode)
		}
	}
	// Successful source metadata update from the same IP 10.0.0.50:
	reqSrcOK, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/metadata?mount=/radio&mode=updinfo&song=Interleaved", nil)
	reqSrcOK.Header.Set("X-Forwarded-For", "10.0.0.50")
	reqSrcOK.SetBasicAuth("source", "source")
	respSrcOK, err := http.DefaultClient.Do(reqSrcOK)
	if err != nil {
		t.Fatal(err)
	}
	_ = respSrcOK.Body.Close()
	if respSrcOK.StatusCode != http.StatusOK {
		t.Fatalf("source metadata update status=%d, want 200", respSrcOK.StatusCode)
	}
	// 5th wrong admin guess from 10.0.0.50 reaches threshold, and 6th attempt returns 429!
	reqAdmin5, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
	reqAdmin5.Header.Set("X-Forwarded-For", "10.0.0.50")
	reqAdmin5.SetBasicAuth("admin", "wrong")
	respAdmin5, err := http.DefaultClient.Do(reqAdmin5)
	if err != nil {
		t.Fatal(err)
	}
	_ = respAdmin5.Body.Close()
	if respAdmin5.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin guess 5 status=%d, want 401", respAdmin5.StatusCode)
	}
	reqAdmin6, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
	reqAdmin6.Header.Set("X-Forwarded-For", "10.0.0.50")
	reqAdmin6.SetBasicAuth("admin", "icecast-admin")
	respAdmin6, err := http.DefaultClient.Do(reqAdmin6)
	if err != nil {
		t.Fatal(err)
	}
	_ = respAdmin6.Body.Close()
	if respAdmin6.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on 6th admin attempt after interleaved source success, got %d", respAdmin6.StatusCode)
	}

	// 4. Admin API requires "Bearer " prefix (rejects raw token without prefix), and accepts case-insensitive "bearer "/"BEARER " per RFC 7235.
	adminMux := http.NewServeMux()
	s.registerAdmin(adminMux)
	rawTokenReq := newTestRequest(http.MethodGet, "http://admin/api/v1/mounts", nil)
	rawTokenReq.Header.Set("Authorization", "admin")
	rawTokenRec := newTestRecorder()
	adminMux.ServeHTTP(rawTokenRec, rawTokenReq)
	if rawTokenRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when Bearer prefix is missing on /api/v1/mounts, got %d", rawTokenRec.Code)
	}
	for _, scheme := range []string{"bearer admin", "BEARER admin", "Bearer admin"} {
		caseReq := newTestRequest(http.MethodGet, "http://admin/api/v1/mounts", nil)
		caseReq.Header.Set("Authorization", scheme)
		caseRec := newTestRecorder()
		adminMux.ServeHTTP(caseRec, caseReq)
		if caseRec.Code != http.StatusOK {
			t.Fatalf("Authorization %q status=%d, want 200", scheme, caseRec.Code)
		}
	}

	// 5. Eviction preference:
	// - Evict zero-failure unlocked entries first.
	// - Next evict unlocked failing entries (never evicting a locked-out IP during an unlocked IP flood).
	// - When every entry is locked out, evict the one whose lockout expires soonest.
	lim := newAuthLimiter()
	lim.ttl = 30 * time.Minute
	lim.maxEntries = 5
	now := time.Now()
	lim.nowFn = func() time.Time { return now }

	// Lock out 198.51.100.1 (5 failures -> locked out).
	for i := 0; i < 5; i++ {
		lim.recordFailure("198.51.100.1", "icecast_admin")
	}
	if locked, _, _ := lim.checkLocked("198.51.100.1", "icecast_admin"); !locked {
		t.Fatal("expected 198.51.100.1 to be locked out")
	}

	// Add 1 zero-failure entry (198.51.100.2 failed once then succeeded) and 3 single-failure unlocked entries.
	lim.recordFailure("198.51.100.2", "source")
	lim.recordSuccess("198.51.100.2", "source") // now 0 failures, unlocked (bucketIdle)
	lim.recordFailure("198.51.100.3", "source")
	lim.recordFailure("198.51.100.4", "source")
	lim.recordFailure("198.51.100.5", "source")
	if got := lim.len(); got != 5 {
		t.Fatalf("limiter len=%d, want 5", got)
	}

	// Inserting a 6th IP must evict the zero-failure unlocked entry (198.51.100.2) first, even though 198.51.100.1 was inserted earlier.
	lim.recordFailure("198.51.100.6", "source")
	if _, exists := lim.ips["198.51.100.2"]; exists {
		t.Fatal("expected zero-failure entry 198.51.100.2 to be evicted first")
	}
	if locked, _, _ := lim.checkLocked("198.51.100.1", "icecast_admin"); !locked {
		t.Fatal("expected locked-out 198.51.100.1 to survive eviction")
	}

	// Flooding 50 more single-failure IPs past maxEntries=5 must only evict unlocked failing entries,
	// keeping locked-out 198.51.100.1 locked out!
	for i := 10; i < 60; i++ {
		lim.recordFailure("192.0.2."+itoa(i), "source")
	}
	if got := lim.len(); got != 5 {
		t.Fatalf("expected cap of 5 entries after flood, got %d", got)
	}
	if locked, _, _ := lim.checkLocked("198.51.100.1", "icecast_admin"); !locked {
		t.Fatal("locked-out IP 198.51.100.1 was evicted by single-failure IP flood")
	}

	// Now lock out all 5 slots with different lockout durations, then add a 6th locked IP:
	// the one whose lockout expires soonest (198.51.100.1, 1s lockout) must be evicted in favor of longer lockouts (2s, 4s, 8s, 16s).
	for idx, ip := range []string{"203.0.113.1", "203.0.113.2", "203.0.113.3", "203.0.113.4"} {
		failures := 6 + idx // 6->2s, 7->4s, 8->8s, 9->16s
		for f := 0; f < failures; f++ {
			lim.recordFailure(ip, "icecast_admin")
		}
	}
	// All 5 entries in lim are now locked: 198.51.100.1 (1s) and 203.0.113.1..4 (2s..16s).
	// Inserting a new IP must evict 198.51.100.1 (soonest lockout expiry: 1s) while preserving 203.0.113.1..4.
	lim.recordFailure("203.0.113.99", "icecast_admin")
	if locked, _, _ := lim.checkLocked("198.51.100.1", "icecast_admin"); locked {
		t.Fatal("expected soonest-expiring locked entry 198.51.100.1 to be evicted when all slots are locked")
	}
	for _, ip := range []string{"203.0.113.1", "203.0.113.2", "203.0.113.3", "203.0.113.4"} {
		if locked, _, _ := lim.checkLocked(ip, "icecast_admin"); !locked {
			t.Fatalf("expected longer-locked entry %s to remain locked", ip)
		}
	}

	// Advance clock past TTL (31 minutes) and verify all idle entries are evicted.
	now = now.Add(31 * time.Minute)
	if got := lim.len(); got != 0 {
		t.Fatalf("expected all idle entries to be TTL-evicted after 31m, got %d", got)
	}

	// 6. IPv6 /64 prefix grouping in limiter while keeping full IPv6 address in logs and source_ip.
	var v6LogBuf safeBuffer
	s.log = slog.New(slog.NewJSONHandler(&v6LogBuf, nil))
	for i := 1; i <= 5; i++ {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
		req.Header.Set("X-Forwarded-For", "2001:db8:abcd:42::"+itoa(i))
		req.SetBasicAuth("admin", "wrong-pass")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Body.Close()
		if r.StatusCode != http.StatusUnauthorized {
			t.Fatalf("IPv6 attempt %d status=%d, want 401", i, r.StatusCode)
		}
	}
	// A 6th attempt from a brand-new host address in the SAME /64 (2001:db8:abcd:42::beef) must be locked out (429).
	reqV6Locked, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
	reqV6Locked.Header.Set("X-Forwarded-For", "2001:db8:abcd:42::beef")
	reqV6Locked.SetBasicAuth("admin", "icecast-admin")
	respV6Locked, err := http.DefaultClient.Do(reqV6Locked)
	if err != nil {
		t.Fatal(err)
	}
	_ = respV6Locked.Body.Close()
	if respV6Locked.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 for rotated host in locked IPv6 /64, got %d", respV6Locked.StatusCode)
	}
	// Verify a different /64 (2001:db8:abcd:43::1) is NOT locked out.
	reqV6Other, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/stats", nil)
	reqV6Other.Header.Set("X-Forwarded-For", "2001:db8:abcd:43::1")
	reqV6Other.SetBasicAuth("admin", "icecast-admin")
	respV6Other, err := http.DefaultClient.Do(reqV6Other)
	if err != nil {
		t.Fatal(err)
	}
	_ = respV6Other.Body.Close()
	if respV6Other.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for different IPv6 /64, got %d", respV6Other.StatusCode)
	}
	// Verify logs and source_ip keep the full IPv6 address (not the /64 prefix).
	if !strings.Contains(v6LogBuf.String(), `"ip":"2001:db8:abcd:42::beef"`) {
		t.Fatalf("expected full IPv6 address in auth logs, got: %s", v6LogBuf.String())
	}
	v6SrcReq := newTestRequest(http.MethodPut, "http://radio.example/radio", nil)
	v6SrcReq.RemoteAddr = "127.0.0.1:54321"
	v6SrcReq.Header.Set("X-Forwarded-For", "2001:db8:abcd:42::cafe")
	if gotIP := s.parseSourceStreamMetadata(v6SrcReq).SourceIP; gotIP != "2001:db8:abcd:42::cafe" {
		t.Fatalf("IPv6 SourceIP=%q, want full address 2001:db8:abcd:42::cafe", gotIP)
	}

	// 7. totalFailures decay over 15m idle intervals and reset when the failing scope succeeds.
	decayLim := newAuthLimiter()
	decayNow := time.Now()
	decayLim.nowFn = func() time.Time { return decayNow }

	// Reach globalLockoutThreshold (15 failures) on "source" scope.
	for i := 0; i < 15; i++ {
		decayLim.recordFailure("198.51.100.77", "source")
	}
	if locked, _, _ := decayLim.checkLocked("198.51.100.77", "icecast_admin"); !locked {
		t.Fatal("expected global ceiling to lock all scopes after 15 failures")
	}
	// Advance past lockout, then succeed on "source": clearing the "source" scope counter must reset totalFailures
	// so a single subsequent failure does NOT re-trigger global lockout.
	decayNow = decayNow.Add(16 * time.Minute)
	decayLim.recordSuccess("198.51.100.77", "source")
	decayLim.recordFailure("198.51.100.77", "source")
	if locked, _, _ := decayLim.checkLocked("198.51.100.77", "icecast_admin"); locked {
		t.Fatal("expected single failure after scope success not to trigger global lockout")
	}

	// Also verify time-based halving decay (every 15m of no failures halves failure counters) without any success call.
	for i := 0; i < 16; i++ {
		decayLim.recordFailure("198.51.100.88", "source")
	}
	// After 30 minutes (2 x 15m periods), 16 totalFailures halves twice to 4 (< 5 threshold and < 15 global threshold).
	decayNow = decayNow.Add(30 * time.Minute)
	if locked, _, failures := decayLim.checkLocked("198.51.100.88", "source"); locked || failures != 4 {
		t.Fatalf("after 2 decay periods (30m): locked=%v failures=%d, want locked=false failures=4", locked, failures)
	}
}

// 8) Write coalescing at the live edge: frames arriving within write_interval are batched, not written per-frame.
func TestWriteCoalescingAtLiveEdge(t *testing.T) {
	cfg := config.Default()
	cfg.Server.HTTPAddress = "127.0.0.1:8000"
	cfg.Defaults.WriteInterval = config.Duration(80 * time.Millisecond)
	cfg.Mounts = []config.Mount{
		{
			Path:        "/radio",
			Profile:     "mp3",
			ContentType: "audio/mpeg",
			Source:      config.SourceCredential{Username: "source", PasswordBcrypt: "$2a$10$abcdefghijklmnopqrstuv"},
			BurstBytes:  417 * 10,
		},
	}
	if err := cfg.Normalize(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	h, err := stream.NewHub(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	m, _ := h.Get("/radio")
	if err := m.AcquireSource(); err != nil {
		t.Fatal(err)
	}
	defer m.ReleaseSource()

	sub := m.Subscribe("http")
	defer sub.Close("done")

	cw := &countingWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- stream.Copy(ctx, cw, sub)
	}()

	// Write 8 live MP3 frames (8 * 417 = 3,336 bytes < 4,096 threshold) spaced 5ms apart (40ms total < 80ms write_interval).
	frame := testMP3Frame()
	for i := 0; i < 8; i++ {
		if err := m.Write(frame); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Wait for the 80ms write_interval flush to deliver all 3,336 bytes.
	deadline := time.Now().Add(time.Second)
	for cw.TotalBytes() < 8*len(frame) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if cw.TotalBytes() != 8*len(frame) {
		t.Fatalf("total bytes=%d, want %d", cw.TotalBytes(), 8*len(frame))
	}
	// All 8 frames arrived within a single 80ms write_interval and under 4KB, so they must be coalesced into 1-2 writes, not 8.
	if writes := cw.WriteCount(); writes > 2 {
		t.Fatalf("live-edge frames were not coalesced: got %d Write calls for 8 frames, want <= 2", writes)
	}
}

// 9) ICY zero-length metadata block when title hasn't changed, and JSON config public:false via Admin API.
func TestICYZeroLengthUnchangedBlockAndJSONConfigPublicFalse(t *testing.T) {
	s, ts := publicServer(t, func(cfg *config.Config) {
		def := config.Default()
		cfg.Version = def.Version
		cfg.Server = def.Server
		cfg.TLS = def.TLS
	})
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)
	m.SetMetadata(stream.Metadata{Title: "Same Track"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/radio", nil)
	req.Header.Set("Icy-MetaData", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	waitForListeners(t, s, "/radio", 1)

	// metaint is 256 bytes. Write 2 * 256 = 512 bytes with unchanged title.
	payload := bytes.Repeat([]byte{0xAA}, 512)
	if err := m.Write(payload); err != nil {
		t.Fatal(err)
	}
	firstBlock := stream.FormatICYMetadataBlock("Same Track")
	// Expect: 256 audio bytes + firstBlock (non-empty) + 256 audio bytes + 0x00 (1-byte zero-length block).
	wantLen := 256 + len(firstBlock) + 256 + 1
	buf := readBodyBytes(t, resp.Body, wantLen, 2*time.Second)
	if buf[256] == 0 {
		t.Fatal("first metadata block should contain the initial title, got length 0")
	}
	secondLenByteOffset := 256 + len(firstBlock) + 256
	if buf[secondLenByteOffset] != 0 {
		t.Fatalf("second metadata block with unchanged title should be 0x00, got length byte %d", buf[secondLenByteOffset])
	}

	// Verify JSON config via PUT /api/v1/config can force public:false (setting PublicSet=true).
	adminMux := http.NewServeMux()
	s.registerAdmin(adminMux)
	jsonBody := `{
		"version": 1,
		"server": {"https_address": ":8443", "http3_address": ":8443", "public_https_port": 8443},
		"admin": {"address": "127.0.0.1:9090", "token_env": "ADMIN_TOKEN", "icecast_admin_password_env": "ICECAST_ADMIN_PASSWORD"},
		"tls": {"mode": "development"},
		"mounts": [
			{
				"path": "/radio",
				"profile": "mp3",
				"source": {"username": "source", "password_env": "SOURCE_PASSWORD"},
				"metadata": {"name": "JSON Private", "bitrate": 128000, "public": false}
			}
		]
	}`
	// Align static fields with current config so Commit does not require restart.
	var cfgDoc config.Config
	if err := json.Unmarshal([]byte(jsonBody), &cfgDoc); err != nil {
		t.Fatal(err)
	}
	if !cfgDoc.Mounts[0].Metadata.PublicSet || cfgDoc.Mounts[0].Metadata.Public {
		t.Fatalf("JSON Unmarshal did not set PublicSet=true with Public=false: %+v", cfgDoc.Mounts[0].Metadata)
	}
	cur := *s.Config()
	cur.Mounts[0].Metadata = cfgDoc.Mounts[0].Metadata
	rawJSON, err := json.Marshal(cur)
	if err != nil {
		t.Fatal(err)
	}
	putReq := newTestRequest(http.MethodPut, "http://admin/api/v1/config", bytes.NewReader(rawJSON))
	putReq.Header.Set("Authorization", "Bearer admin")
	putRec := newTestRecorder()
	adminMux.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PUT /api/v1/config status=%d body=%s", putRec.Code, putRec.Body.String())
	}

	mAfter, _ := s.hub.Get("/radio")
	mAfter.SetSourceStreamMetadata(stream.SourceStreamMetadata{Public: true, PublicSet: true})
	if mAfter.StreamMetadata().Public {
		t.Fatal("expected JSON config public:false to override source ice-public:1")
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type countingWriter struct {
	mu     sync.Mutex
	writes int
	bytes  int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes++
	w.bytes += len(p)
	return len(p), nil
}

func (w *countingWriter) WriteCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes
}

func (w *countingWriter) TotalBytes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytes
}

func newTestRequest(method, target string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, target, body)
	if err != nil {
		panic(err)
	}
	return req
}

func itoa(v int) string {
	return strings.TrimSpace(strings.Join([]string{
		func() string {
			var b [20]byte
			i := len(b)
			if v == 0 {
				return "0"
			}
			for v > 0 {
				i--
				b[i] = byte('0' + v%10)
				v /= 10
			}
			return string(b[i:])
		}(),
	}, ""))
}

type testRecorder struct {
	Code   int
	header http.Header
	Body   bytes.Buffer
}

func newTestRecorder() *testRecorder {
	return &testRecorder{Code: http.StatusOK, header: make(http.Header)}
}
func (r *testRecorder) Header() http.Header         { return r.header }
func (r *testRecorder) WriteHeader(code int)        { r.Code = code }
func (r *testRecorder) Write(p []byte) (int, error) { return r.Body.Write(p) }
