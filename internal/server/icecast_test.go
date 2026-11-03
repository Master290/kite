package server

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Master290/kite/internal/config"
	"github.com/Master290/kite/internal/stream"
)

func testMP3Frame() []byte {
	frame := make([]byte, 417)
	copy(frame, []byte{0xff, 0xfb, 0x90, 0x64})
	return frame
}

// 1) Busy mount returns 403 Forbidden (not 409 Conflict).
func TestBusyMountReturns403Forbidden(t *testing.T) {
	s, ts := publicServer(t)
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
}

// 2) Read ice-name, ice-genre, ice-description, ice-url, ice-bitrate, ice-public
// from the source request and use them as stream metadata unless overridden in config.
func TestSourceIceHeadersAndConfigOverrides(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "admin")
	t.Setenv("SOURCE_PASSWORD", "source")

	yamlCfg := `
version: 1
server:
  http_address: "127.0.0.1:8000"
admin:
  address: "127.0.0.1:9090"
  token_env: ADMIN_TOKEN
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
      bitrate: 192
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
	for _, path := range []string{"/dynamic", "/overridden"} {
		req, err := http.NewRequest(http.MethodPut, ts.URL+path, bytes.NewReader(frame))
		if err != nil {
			t.Fatal(err)
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
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT %s status=%d", path, resp.StatusCode)
		}
	}

	// Verify /dynamic uses the source's ice-* headers.
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

	// Verify /overridden keeps config values instead of source headers.
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
		t.Fatalf("overridden icy-br=%q", got)
	}
	if got := headOvr.Header.Get("icy-pub"); got != "0" {
		t.Fatalf("overridden icy-pub=%q, want 0 (config public: false)", got)
	}

	mDyn, _ := s.hub.Get("/dynamic")
	if meta := mDyn.StreamMetadata(); !meta.Public || meta.Bitrate != 128 || meta.Name != "Source Stream Name" {
		t.Fatalf("dynamic StreamMetadata=%+v", meta)
	}
	mOvr, _ := s.hub.Get("/overridden")
	if meta := mOvr.StreamMetadata(); meta.Public || meta.Bitrate != 192 || meta.Name != "Config Name" {
		t.Fatalf("overridden StreamMetadata=%+v", meta)
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

// 4) /status-json.xsl matches Icecast 2.4 schema (object for 1 mount, array for multiple mounts),
// while /api/v1 remains unchanged.
func TestStatusJSONMatchesIcecast24Schema(t *testing.T) {
	// Sample Icecast 2.4 single-mount JSON response for structural comparison.
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
				"title": "One More Time",
				"artist": "Daft Punk"
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
	m, _ := s.hub.Get("/radio")
	acquireTestSource(t, m)
	m.SetSourceStreamMetadata(stream.SourceStreamMetadata{
		Description: "Main Station",
		Genre:       "Electronic",
		Bitrate:     128,
		AudioInfo:   "ice-samplerate=44100;ice-bitrate=128;ice-channels=2",
	})
	m.SetMetadata(stream.Metadata{Title: "Daft Punk - One More Time"})

	// Connect one listener to populate listeners and listener_peak.
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
	gotStats, ok := gotMap["icestats"].(map[string]any)
	if !ok {
		t.Fatalf("missing icestats object: %+v", gotMap)
	}
	for k := range sampleStats {
		if _, exists := gotStats[k]; !exists {
			t.Fatalf("icestats missing required key %q in %+v", k, gotStats)
		}
	}
	// With 1 visible mount, source MUST be a JSON object, not an array.
	gotSource, ok := gotStats["source"].(map[string]any)
	if !ok {
		t.Fatalf("expected source to be a JSON object when 1 mount exists, got %T (%+v)", gotStats["source"], gotStats["source"])
	}
	for k := range sampleSource {
		if _, exists := gotSource[k]; !exists {
			t.Fatalf("source object missing required Icecast 2.4 key %q in %+v", k, gotSource)
		}
	}
	if gotSource["server_name"] != "Kite Test" ||
		gotSource["server_description"] != "Main Station" ||
		gotSource["server_type"] != "audio/mpeg" ||
		gotSource["genre"] != "Electronic" ||
		gotSource["bitrate"] != float64(128) ||
		gotSource["artist"] != "Daft Punk" ||
		gotSource["title"] != "One More Time" ||
		gotSource["listeners"] != float64(1) ||
		gotSource["listener_peak"] != float64(1) ||
		gotSource["audio_info"] != "ice-samplerate=44100;ice-bitrate=128;ice-channels=2" {
		t.Fatalf("unexpected source fields: %+v", gotSource)
	}

	// Now add a second visible mount and verify source becomes a JSON array.
	cfg := s.Config()
	cfg.Mounts = append(cfg.Mounts, config.Mount{
		Path:        "/jazz",
		Profile:     "mp3",
		ContentType: "audio/mpeg",
		Source:      cfg.Mounts[0].Source,
		Metadata:    config.Metadata{Name: "Jazz Channel", Genre: "Jazz", Bitrate: 160, Public: true},
	})
	s.applyConfig(cfg)

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
		t.Fatalf("expected source to be a 2-element array when 2 mounts exist, got %T (%+v)", multiStats["source"], multiStats["source"])
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
// /admin/killclient?mount=&id=, /admin/killsource?mount= with admin Basic auth.
func TestIcecastAdminXMLEndpoints(t *testing.T) {
	// Sample Icecast 2.4 XML responses to compare structure against.
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
  <sources>2</sources>
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
    <title>Track</title>
    <artist>Band</artist>
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

	// Ensure sample Icecast XML documents unmarshal into the same schemas.
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

	// Verify all 5 endpoints require admin Basic auth.
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
		req.SetBasicAuth("admin", "admin")
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

	// 5a) /admin/stats
	var gotStats xmlIceStats
	doAdminXML("/admin/stats", &gotStats)
	if gotStats.ServerID != statsSample.ServerID || gotStats.Listeners != 1 || len(gotStats.Source) == 0 {
		t.Fatalf("/admin/stats unexpected: %+v", gotStats)
	}
	var radioStats *xmlStatsSource
	for i := range gotStats.Source {
		if gotStats.Source[i].Mount == "/radio" {
			radioStats = &gotStats.Source[i]
			break
		}
	}
	if radioStats == nil || radioStats.Genre != "Rock" || radioStats.Bitrate != 128 || radioStats.Artist != "Band" || radioStats.Title != "Track" || radioStats.Listeners != 1 {
		t.Fatalf("/admin/stats /radio=%+v", radioStats)
	}

	// 5b) /admin/listmounts
	var gotMounts xmlListMounts
	doAdminXML("/admin/listmounts", &gotMounts)
	if len(gotMounts.Source) == 0 || gotMounts.Source[0].ContentType != "audio/mpeg" {
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

	// 5e) /admin/killsource?mount=/radio
	var killSourceResp xmlIceResponse
	doAdminXML("/admin/killsource?mount=/radio", &killSourceResp)
	if killSourceResp.Return != respSample.Return {
		t.Fatalf("killsource response=%+v", killSourceResp)
	}
	if m.Status().Source {
		t.Fatal("expected source to be disconnected after /admin/killsource")
	}
	select {
	case <-sourceDone:
	case <-time.After(time.Second):
		t.Fatal("source PUT handler did not exit after /admin/killsource")
	}
}

// 6) Serve /<mount>.m3u and /<mount>.xspf playlists.
func TestMountM3UAndXSPFPlaylists(t *testing.T) {
	const sampleXSPF = `<?xml version="1.0" encoding="UTF-8"?>
<playlist xmlns="http://xspf.org/ns/0/" version="1">
  <title>Kite Test</title>
  <creator>Artist</creator>
  <trackList>
    <track>
      <location>http://localhost:8000/radio</location>
      <creator>Artist</creator>
      <title>Song</title>
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

	// Test /radio.xspf
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
		len(gotXSPF.TrackList.Track) != 1 ||
		gotXSPF.TrackList.Track[0].Location != ts.URL+"/radio" ||
		gotXSPF.TrackList.Track[0].Creator != wantXSPF.TrackList.Track[0].Creator ||
		gotXSPF.TrackList.Track[0].Title != wantXSPF.TrackList.Track[0].Title ||
		gotXSPF.TrackList.Track[0].Annotation != wantXSPF.TrackList.Track[0].Annotation ||
		gotXSPF.TrackList.Track[0].Info != wantXSPF.TrackList.Track[0].Info {
		t.Fatalf("/radio.xspf parsed=%+v, want=%+v", gotXSPF, wantXSPF)
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
