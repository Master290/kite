package server

import (
	"crypto/subtle"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Master290/kite/internal/stream"
	"golang.org/x/crypto/bcrypt"
)

const (
	icecastTimeFormat    = "Mon, 02 Jan 2006 15:04:05 -0700"
	icecastISO8601Format = "2006-01-02T15:04:05-0700"
	defaultIcecastAdmin  = "icemaster@localhost"
	defaultIcecastLoc    = "Earth"
	defaultIcecastID     = "Icecast 2.4.4"
)

func (s *Server) registerIcecastAdmin(mux *http.ServeMux) {
	mux.HandleFunc("/admin/stats", s.adminBasicAuth(s.handleAdminStats))
	mux.HandleFunc("/admin/stats.xml", s.adminBasicAuth(s.handleAdminStats))
	mux.HandleFunc("/admin/listmounts", s.adminBasicAuth(s.handleAdminListMounts))
	mux.HandleFunc("/admin/listclients", s.adminBasicAuth(s.handleAdminListClients))
	mux.HandleFunc("/admin/killclient", s.adminBasicAuth(s.handleAdminKillClient))
	mux.HandleFunc("/admin/killsource", s.adminBasicAuth(s.handleAdminKillSource))
}

func (s *Server) adminBasicAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := s.adminToken()
		if token == "" {
			http.Error(w, "admin token is not configured", http.StatusServiceUnavailable)
			return
		}
		if !s.checkAdminBasicAuth(r, token) {
			w.Header().Set("WWW-Authenticate", `Basic realm="Icecast2 Server"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) checkAdminBasicAuth(r *http.Request, token string) bool {
	if token == "" {
		token = s.adminToken()
	}
	if token == "" {
		return false
	}
	user, pass, ok := r.BasicAuth()
	if !ok || user != "admin" {
		return false
	}
	if strings.HasPrefix(token, "$2") {
		return bcrypt.CompareHashAndPassword([]byte(token), []byte(pass)) == nil
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(pass)) == 1
}

func parseSourceStreamMetadata(r *http.Request) stream.SourceStreamMetadata {
	h := r.Header
	audioInfo := strings.TrimSpace(h.Get("ice-audio-info"))
	meta := stream.SourceStreamMetadata{
		Name:        strings.TrimSpace(headerFirst(h, "ice-name", "icy-name")),
		Description: strings.TrimSpace(headerFirst(h, "ice-description", "icy-description")),
		Genre:       strings.TrimSpace(headerFirst(h, "ice-genre", "icy-genre")),
		URL:         strings.TrimSpace(headerFirst(h, "ice-url", "icy-url")),
		AudioInfo:   audioInfo,
		SourceIP:    requestClientIP(r),
		ContentType: strings.TrimSpace(h.Get("Content-Type")),
	}
	if rawBr := strings.TrimSpace(headerFirst(h, "ice-bitrate", "icy-br", "icy-bitrate")); rawBr != "" {
		if br, err := strconv.Atoi(rawBr); err == nil && br > 0 {
			meta.Bitrate = br
		}
	}
	if meta.Bitrate == 0 && audioInfo != "" {
		meta.Bitrate = parseBitrateFromAudioInfo(audioInfo)
	}
	if rawPub := strings.TrimSpace(headerFirst(h, "ice-public", "icy-pub", "icy-public")); rawPub != "" {
		meta.PublicSet = true
		meta.Public = rawPub == "1" || strings.EqualFold(rawPub, "true") || strings.EqualFold(rawPub, "yes")
	}
	return meta
}

func headerFirst(h http.Header, keys ...string) string {
	for _, k := range keys {
		if v := h.Get(k); v != "" {
			return v
		}
	}
	return ""
}

func parseBitrateFromAudioInfo(info string) int {
	for _, part := range strings.Split(info, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		k = strings.ToLower(strings.TrimSpace(k))
		v = strings.TrimSpace(v)
		if k == "ice-bitrate" || k == "bitrate" {
			if br, err := strconv.Atoi(v); err == nil && br > 0 {
				return br
			}
		}
	}
	return 0
}

func requestClientIP(r *http.Request) string {
	if r == nil || r.RemoteAddr == "" {
		return "127.0.0.1"
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil && host != "" {
		return host
	}
	return r.RemoteAddr
}

func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r != nil && r.TLS != nil {
		scheme = "https"
	}
	host := "localhost"
	if r != nil && r.Host != "" {
		host = r.Host
	}
	return scheme + "://" + host
}

func requestHostOnly(r *http.Request, cfgHosts []string) string {
	if r != nil && r.Host != "" {
		if h, _, err := net.SplitHostPort(r.Host); err == nil && h != "" {
			return h
		}
		return r.Host
	}
	if len(cfgHosts) > 0 && cfgHosts[0] != "" {
		return cfgHosts[0]
	}
	return "localhost"
}

func splitArtistTitle(raw string) (artist, title string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	if a, t, ok := strings.Cut(raw, " - "); ok {
		return strings.TrimSpace(a), strings.TrimSpace(t)
	}
	return "", raw
}

func boolToZeroOne(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

type icecastJSONStats struct {
	Admin              string `json:"admin"`
	Host               string `json:"host"`
	Location           string `json:"location"`
	ServerID           string `json:"server_id"`
	ServerStart        string `json:"server_start"`
	ServerStartISO8601 string `json:"server_start_iso8601"`
	Source             any    `json:"source"`
}

type icecastJSONSource struct {
	ListenURL          string `json:"listenurl"`
	ServerName         string `json:"server_name"`
	ServerDescription  string `json:"server_description"`
	ServerType         string `json:"server_type"`
	ServerURL          string `json:"server_url,omitempty"`
	Genre              string `json:"genre"`
	Bitrate            int    `json:"bitrate"`
	Title              string `json:"title"`
	Artist             string `json:"artist"`
	Listeners          int    `json:"listeners"`
	ListenerPeak       int    `json:"listener_peak"`
	StreamStart        string `json:"stream_start"`
	StreamStartISO8601 string `json:"stream_start_iso8601"`
	AudioInfo          string `json:"audio_info"`
	Path               string `json:"path,omitempty"`
}

func (s *Server) buildJSONSource(r *http.Request, m *stream.Mount) icecastJSONSource {
	st := m.Status()
	meta := m.StreamMetadata()
	artist, title := splitArtistTitle(st.Metadata.Title)
	streamStart := m.StreamStart()
	return icecastJSONSource{
		ListenURL:          requestBaseURL(r) + st.Path,
		ServerName:         meta.Name,
		ServerDescription:  meta.Description,
		ServerType:         st.ContentType,
		ServerURL:          meta.URL,
		Genre:              meta.Genre,
		Bitrate:            meta.Bitrate,
		Title:              title,
		Artist:             artist,
		Listeners:          st.Listeners,
		ListenerPeak:       m.ListenerPeak(),
		StreamStart:        streamStart.Format(icecastTimeFormat),
		StreamStartISO8601: streamStart.Format(icecastISO8601Format),
		AudioInfo:          m.SourceAudioInfo(),
		Path:               st.Path,
	}
}

func (s *Server) serverAdminEmail() string {
	if cfg := s.Config(); cfg != nil && cfg.TLS.Email != "" {
		return cfg.TLS.Email
	}
	return defaultIcecastAdmin
}

func (s *Server) startedTime() time.Time {
	if !s.startedAt.IsZero() {
		return s.startedAt
	}
	return time.Now()
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	cfg := s.Config()
	items := make([]icecastJSONSource, 0)
	for _, m := range s.hub.Mounts() {
		mCfg := m.Config()
		if mCfg.Hidden {
			continue
		}
		items = append(items, s.buildJSONSource(r, m))
	}
	var sourceField any = items
	if len(items) == 1 {
		sourceField = items[0]
	}
	start := s.startedTime()
	var hosts []string
	if cfg != nil {
		hosts = cfg.TLS.Hosts
	}
	stats := icecastJSONStats{
		Admin:              s.serverAdminEmail(),
		Host:               requestHostOnly(r, hosts),
		Location:           defaultIcecastLoc,
		ServerID:           defaultIcecastID,
		ServerStart:        start.Format(icecastTimeFormat),
		ServerStartISO8601: start.Format(icecastISO8601Format),
		Source:             sourceField,
	}
	writeJSON(w, http.StatusOK, map[string]any{"icestats": stats})
}

type xmlIceStats struct {
	XMLName                 xml.Name         `xml:"icestats"`
	Admin                   string           `xml:"admin,omitempty"`
	Clients                 int              `xml:"clients,omitempty"`
	Connections             int              `xml:"connections,omitempty"`
	FileConnections         int              `xml:"file_connections"`
	Host                    string           `xml:"host,omitempty"`
	ListenerConnections     int              `xml:"listener_connections"`
	Listeners               int              `xml:"listeners"`
	Location                string           `xml:"location,omitempty"`
	ServerID                string           `xml:"server_id,omitempty"`
	ServerStart             string           `xml:"server_start,omitempty"`
	ServerStartISO8601      string           `xml:"server_start_iso8601,omitempty"`
	SourceClientConnections int              `xml:"source_client_connections"`
	SourceRelayConnections  int              `xml:"source_relay_connections"`
	SourceTotalConnections  int              `xml:"source_total_connections"`
	Sources                 int              `xml:"sources"`
	Stats                   int              `xml:"stats"`
	StatsConnections        int              `xml:"stats_connections"`
	Source                  []xmlStatsSource `xml:"source"`
}

type xmlStatsSource struct {
	Mount              string `xml:"mount,attr"`
	AudioInfo          string `xml:"audio_info"`
	Artist             string `xml:"artist"`
	Bitrate            int    `xml:"bitrate"`
	Connected          int64  `xml:"connected"`
	Genre              string `xml:"genre"`
	ListenerPeak       int    `xml:"listener_peak"`
	Listeners          int    `xml:"listeners"`
	ListenURL          string `xml:"listenurl"`
	MaxListeners       string `xml:"max_listeners"`
	Public             int    `xml:"public"`
	ServerDescription  string `xml:"server_description"`
	ServerName         string `xml:"server_name"`
	ServerType         string `xml:"server_type"`
	ServerURL          string `xml:"server_url"`
	SlowListeners      int    `xml:"slow_listeners"`
	SourceIP           string `xml:"source_ip"`
	StreamStart        string `xml:"stream_start"`
	StreamStartISO8601 string `xml:"stream_start_iso8601"`
	Title              string `xml:"title"`
	TotalBytesRead     uint64 `xml:"total_bytes_read"`
	TotalBytesSent     uint64 `xml:"total_bytes_sent"`
}

func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	cfg := s.Config()
	var hosts []string
	if cfg != nil {
		hosts = cfg.TLS.Hosts
	}
	mounts := s.hub.Mounts()
	sources := make([]xmlStatsSource, 0, len(mounts))
	totalListeners := 0
	directSources := 0
	relaySources := 0
	now := time.Now()
	for _, m := range mounts {
		st := m.Status()
		meta := m.StreamMetadata()
		artist, title := splitArtistTitle(st.Metadata.Title)
		streamStart := m.StreamStart()
		connectedSec := int64(now.Sub(streamStart).Seconds())
		if connectedSec < 0 {
			connectedSec = 0
		}
		pub := 0
		if meta.Public {
			pub = 1
		}
		totalListeners += st.Listeners
		if st.Source {
			if st.IsRelay {
				relaySources++
			} else {
				directSources++
			}
		}
		sources = append(sources, xmlStatsSource{
			Mount:              st.Path,
			AudioInfo:          m.SourceAudioInfo(),
			Artist:             artist,
			Bitrate:            meta.Bitrate,
			Connected:          connectedSec,
			Genre:              meta.Genre,
			ListenerPeak:       m.ListenerPeak(),
			Listeners:          st.Listeners,
			ListenURL:          requestBaseURL(r) + st.Path,
			MaxListeners:       "unlimited",
			Public:             pub,
			ServerDescription:  meta.Description,
			ServerName:         meta.Name,
			ServerType:         st.ContentType,
			ServerURL:          meta.URL,
			SlowListeners:      0,
			SourceIP:           m.SourceIP(),
			StreamStart:        streamStart.Format(icecastTimeFormat),
			StreamStartISO8601: streamStart.Format(icecastISO8601Format),
			Title:              title,
			TotalBytesRead:     st.BytesIn,
			TotalBytesSent:     st.BytesOut,
		})
	}
	start := s.startedTime()
	activeSources := directSources + relaySources
	doc := xmlIceStats{
		Admin:                   s.serverAdminEmail(),
		Clients:                 totalListeners + activeSources,
		Connections:             totalListeners + activeSources,
		Host:                    requestHostOnly(r, hosts),
		ListenerConnections:     totalListeners,
		Listeners:               totalListeners,
		Location:                defaultIcecastLoc,
		ServerID:                defaultIcecastID,
		ServerStart:             start.Format(icecastTimeFormat),
		ServerStartISO8601:      start.Format(icecastISO8601Format),
		SourceClientConnections: directSources,
		SourceRelayConnections:  relaySources,
		SourceTotalConnections:  activeSources,
		Sources:                 len(sources),
		Source:                  sources,
	}
	writeXML(w, http.StatusOK, doc)
}

type xmlListMounts struct {
	XMLName xml.Name         `xml:"icestats"`
	Source  []xmlMountRecord `xml:"source"`
}

type xmlMountRecord struct {
	Mount             string `xml:"mount,attr"`
	Listeners         int    `xml:"listeners"`
	Connected         int64  `xml:"Connected"`
	ConnectedLower    int64  `xml:"connected"`
	ContentType       string `xml:"content-type"`
	ServerType        string `xml:"server_type"`
	ServerName        string `xml:"server_name"`
	ServerDescription string `xml:"server_description"`
	Genre             string `xml:"genre"`
	Bitrate           int    `xml:"bitrate"`
	Public            int    `xml:"public"`
	ListenURL         string `xml:"listenurl"`
	Title             string `xml:"title"`
	Artist            string `xml:"artist"`
}

func (s *Server) handleAdminListMounts(w http.ResponseWriter, r *http.Request) {
	mounts := s.hub.Mounts()
	out := make([]xmlMountRecord, 0, len(mounts))
	now := time.Now()
	for _, m := range mounts {
		st := m.Status()
		meta := m.StreamMetadata()
		artist, title := splitArtistTitle(st.Metadata.Title)
		connectedSec := int64(now.Sub(m.StreamStart()).Seconds())
		if connectedSec < 0 {
			connectedSec = 0
		}
		pub := 0
		if meta.Public {
			pub = 1
		}
		out = append(out, xmlMountRecord{
			Mount:             st.Path,
			Listeners:         st.Listeners,
			Connected:         connectedSec,
			ConnectedLower:    connectedSec,
			ContentType:       st.ContentType,
			ServerType:        st.ContentType,
			ServerName:        meta.Name,
			ServerDescription: meta.Description,
			Genre:             meta.Genre,
			Bitrate:           meta.Bitrate,
			Public:            pub,
			ListenURL:         requestBaseURL(r) + st.Path,
			Title:             title,
			Artist:            artist,
		})
	}
	writeXML(w, http.StatusOK, xmlListMounts{Source: out})
}

type xmlListClients struct {
	XMLName xml.Name             `xml:"icestats"`
	Source  xmlListClientsSource `xml:"source"`
}

type xmlListClientsSource struct {
	Mount     string              `xml:"mount,attr"`
	Listeners int                 `xml:"listeners"`
	Listener  []xmlClientListener `xml:"listener"`
}

type xmlClientListener struct {
	IDAttr    uint64 `xml:"id,attr"`
	ID        uint64 `xml:"ID"`
	IP        string `xml:"IP"`
	UserAgent string `xml:"UserAgent"`
	Connected int64  `xml:"Connected"`
	Lag       int    `xml:"lag"`
}

func (s *Server) handleAdminListClients(w http.ResponseWriter, r *http.Request) {
	mountPath := r.URL.Query().Get("mount")
	if mountPath == "" {
		writeXML(w, http.StatusBadRequest, xmlIceResponse{Message: "Missing mount parameter", Return: 0})
		return
	}
	m, ok := s.hub.Get(mountPath)
	if !ok {
		writeXML(w, http.StatusNotFound, xmlIceResponse{Message: "Mountpoint will not be available", Return: 0})
		return
	}
	clients := m.ListClients()
	now := time.Now()
	listeners := make([]xmlClientListener, 0, len(clients))
	for _, c := range clients {
		connectedSec := int64(now.Sub(c.ConnectedAt).Seconds())
		if connectedSec < 0 {
			connectedSec = 0
		}
		listeners = append(listeners, xmlClientListener{
			IDAttr:    c.ID,
			ID:        c.ID,
			IP:        c.IP,
			UserAgent: c.UserAgent,
			Connected: connectedSec,
			Lag:       c.LagBytes,
		})
	}
	doc := xmlListClients{
		Source: xmlListClientsSource{
			Mount:     mountPath,
			Listeners: len(listeners),
			Listener:  listeners,
		},
	}
	writeXML(w, http.StatusOK, doc)
}

type xmlIceResponse struct {
	XMLName xml.Name `xml:"iceresponse"`
	Message string   `xml:"message"`
	Return  int      `xml:"return"`
}

func (s *Server) handleAdminKillClient(w http.ResponseWriter, r *http.Request) {
	mountPath := r.URL.Query().Get("mount")
	idStr := r.URL.Query().Get("id")
	if mountPath == "" || idStr == "" {
		writeXML(w, http.StatusBadRequest, xmlIceResponse{Message: "Missing mount or id parameter", Return: 0})
		return
	}
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		writeXML(w, http.StatusBadRequest, xmlIceResponse{Message: "Invalid client id", Return: 0})
		return
	}
	m, ok := s.hub.Get(mountPath)
	if !ok {
		writeXML(w, http.StatusNotFound, xmlIceResponse{Message: "Mountpoint will not be available", Return: 0})
		return
	}
	if !m.KillClient(id) {
		writeXML(w, http.StatusNotFound, xmlIceResponse{Message: "Client not found", Return: 0})
		return
	}
	writeXML(w, http.StatusOK, xmlIceResponse{
		Message: fmt.Sprintf("Client %d removed", id),
		Return:  1,
	})
}

func (s *Server) handleAdminKillSource(w http.ResponseWriter, r *http.Request) {
	mountPath := r.URL.Query().Get("mount")
	if mountPath == "" {
		writeXML(w, http.StatusBadRequest, xmlIceResponse{Message: "Missing mount parameter", Return: 0})
		return
	}
	m, ok := s.hub.Get(mountPath)
	if !ok {
		writeXML(w, http.StatusNotFound, xmlIceResponse{Message: "Mountpoint will not be available", Return: 0})
		return
	}
	if !m.DisconnectSource() {
		writeXML(w, http.StatusNotFound, xmlIceResponse{Message: "Source is not available", Return: 0})
		return
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for m.Status().Source && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	writeXML(w, http.StatusOK, xmlIceResponse{
		Message: "Source removed",
		Return:  1,
	})
}

func writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(v)
	_, _ = w.Write([]byte("\n"))
}

type xspfPlaylist struct {
	XMLName   xml.Name      `xml:"playlist"`
	Version   string        `xml:"version,attr"`
	XMLNS     string        `xml:"xmlns,attr"`
	Title     string        `xml:"title"`
	Creator   string        `xml:"creator"`
	TrackList xspfTrackList `xml:"trackList"`
}

type xspfTrackList struct {
	Track []xspfTrack `xml:"track"`
}

type xspfTrack struct {
	Location   string `xml:"location"`
	Creator    string `xml:"creator"`
	Title      string `xml:"title"`
	Annotation string `xml:"annotation"`
	Info       string `xml:"info,omitempty"`
}

func (s *Server) routeMountPlaylist(w http.ResponseWriter, r *http.Request, path string) bool {
	if strings.HasSuffix(path, ".m3u") {
		mountPath := strings.TrimSuffix(path, ".m3u")
		if m, ok := s.hub.Get(mountPath); ok {
			s.handleMountM3U(w, r, m)
			return true
		}
		return false
	}
	if strings.HasSuffix(path, ".xspf") {
		mountPath := strings.TrimSuffix(path, ".xspf")
		if m, ok := s.hub.Get(mountPath); ok {
			s.handleMountXSPF(w, r, m)
			return true
		}
		return false
	}
	return false
}

func (s *Server) handleMountM3U(w http.ResponseWriter, r *http.Request, m *stream.Mount) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	meta := m.StreamMetadata()
	name := meta.Name
	if name == "" {
		name = "Kite"
	}
	listenURL := requestBaseURL(r) + m.Config().Path
	setCORS(w, m.Config().CORSOrigins, r.Header.Get("Origin"))
	w.Header().Set("Content-Type", "audio/x-mpegurl; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXTINF:-1,%s\n%s\n", name, listenURL)
}

func (s *Server) handleMountXSPF(w http.ResponseWriter, r *http.Request, m *stream.Mount) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	meta := m.StreamMetadata()
	st := m.Status()
	artist, trackTitle := splitArtistTitle(st.Metadata.Title)
	if trackTitle == "" {
		trackTitle = meta.Name
	}
	listenURL := requestBaseURL(r) + st.Path
	doc := xspfPlaylist{
		Version: "1",
		XMLNS:   "http://xspf.org/ns/0/",
		Title:   meta.Name,
		Creator: artist,
		TrackList: xspfTrackList{
			Track: []xspfTrack{
				{
					Location:   listenURL,
					Creator:    artist,
					Title:      trackTitle,
					Annotation: meta.Description,
					Info:       meta.URL,
				},
			},
		},
	}
	setCORS(w, m.Config().CORSOrigins, r.Header.Get("Origin"))
	w.Header().Set("Content-Type", "application/xspf+xml; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(doc)
	_, _ = w.Write([]byte("\n"))
}
