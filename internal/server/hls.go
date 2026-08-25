package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Master290/kite/internal/stream"
)

// hlsPlaylist serves the live media playlist for a mount at `<mount>.m3u8`.
func (s *Server) handleHLSPlaylist(w http.ResponseWriter, r *http.Request, m *stream.Mount) {
	if !s.hlsAllowed(m) {
		http.NotFound(w, r)
		return
	}
	infos, target, ok := m.HLSSnapshot()
	if !ok {
		http.Error(w, "hls packaging unavailable", http.StatusServiceUnavailable)
		return
	}
	if len(infos) == 0 && r.Context().Err() == nil {
		// Nothing packaged yet: ask the player to retry instead of ending.
		w.Header().Set("Retry-After", "2")
		http.Error(w, "no segments yet", http.StatusServiceUnavailable)
		return
	}
	setCORS(w, m.Config().CORSOrigins, r.Header.Get("Origin"))
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-cache, no-store")

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	fmt.Fprintf(&b, "#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:%d\n", target)
	if len(infos) > 0 {
		fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n", infos[0].Seq)
	}
	for _, info := range infos {
		if info.Discontinuity {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%s.hls/%d.ts\n", info.Duration, m.Config().Path, info.Seq)
	}
	_, _ = w.Write([]byte(b.String()))
}

// hlsSegment serves one immutable TS segment at `<mount>.hls/<seq>.ts`.
func (s *Server) handleHLSSegment(w http.ResponseWriter, r *http.Request, m *stream.Mount, seq uint64) {
	if !s.hlsAllowed(m) {
		http.NotFound(w, r)
		return
	}
	data, ok := m.HLSSegment(seq)
	if !ok {
		http.NotFound(w, r)
		return
	}
	setCORS(w, m.Config().CORSOrigins, r.Header.Get("Origin"))
	w.Header().Set("Content-Type", "video/mp2t")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(data)
}

func (s *Server) hlsAllowed(m *stream.Mount) bool {
	return s.Config().Server.HLS() && stream.HLSSupportedProfile(m.Profile())
}

// routeHLS intercepts `.m3u8` and `.hls/<seq>.ts` paths. It reports whether
// the request was handled; false means fall through to mount routing.
func (s *Server) routeHLS(w http.ResponseWriter, r *http.Request, path string) bool {
	if strings.HasSuffix(path, ".m3u8") {
		mountPath := strings.TrimSuffix(path, ".m3u8")
		if m, ok := s.hub.Get(mountPath); ok {
			s.handleHLSPlaylist(w, r, m)
			return true
		}
		return false
	}
	idx := strings.LastIndex(path, ".hls/")
	if idx < 0 || !strings.HasSuffix(path, ".ts") {
		return false
	}
	mountPath := path[:idx]
	seq, err := strconv.ParseUint(strings.TrimSuffix(path[idx+len(".hls/"):], ".ts"), 10, 64)
	if err != nil {
		return false
	}
	if m, ok := s.hub.Get(mountPath); ok {
		s.handleHLSSegment(w, r, m, seq)
		return true
	}
	return false
}
