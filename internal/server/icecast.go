package server

import (
	"crypto/subtle"
	"encoding/xml"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Master290/kite/internal/config"
	"github.com/Master290/kite/internal/stream"
	"golang.org/x/crypto/bcrypt"
)

const (
	icecastTimeFormat    = "Mon, 02 Jan 2006 15:04:05 -0700"
	icecastISO8601Format = "2006-01-02T15:04:05-0700"
	defaultIcecastAdmin  = "icemaster@localhost"
	defaultIcecastLoc    = "Earth"
	defaultIcecastID     = "Icecast 2.4.4"

	authLockoutThreshold   = 5
	globalLockoutThreshold = 15
	defaultAuthBaseDelay   = 1 * time.Second
	maxAuthLockoutDelay    = 15 * time.Minute
	defaultAuthDecayPeriod = 15 * time.Minute
	// defaultAuthEntryTTL is maxAuthLockoutDelay + defaultAuthDecayPeriod (15m + 15m = 30m)
	// so entries survive the maximum lockout window plus one post-lockout decay window.
	defaultAuthEntryTTL       = maxAuthLockoutDelay + defaultAuthDecayPeriod
	defaultAuthMaxEntries     = 100000
	defaultAuthIPv6PrefixBits = 64

	bucketIdle    = 0
	bucketFailing = 1
	bucketLocked  = 2
)

type authLimiter struct {
	mu             sync.Mutex
	ips            map[string]*ipAuthState
	heads          [3]*ipAuthState
	tails          [3]*ipAuthState
	baseDelay      time.Duration
	maxDelay       time.Duration
	decayInterval  time.Duration
	ttl            time.Duration
	maxEntries     int
	ipv6PrefixBits int
	nowFn          func() time.Time
}

type scopeAuthState struct {
	failures    int
	lockedUntil time.Time
}

type ipAuthState struct {
	ip                string
	icecastAdmin      scopeAuthState
	source            scopeAuthState
	adminAPI          scopeAuthState
	totalFailures     int
	globalLockedUntil time.Time
	lastFailure       time.Time
	lastLockout       time.Duration
	lastSeen          time.Time
	bcryptWindow      time.Time
	bcryptCount       int
	bucket            int
	inList            bool
	prev              *ipAuthState
	next              *ipAuthState
}

func (st *ipAuthState) scopeState(scope string) *scopeAuthState {
	switch scope {
	case "icecast_admin":
		return &st.icecastAdmin
	case "admin_api":
		return &st.adminAPI
	default:
		return &st.source
	}
}

func (st *ipAuthState) latestLockedUntil() time.Time {
	latest := st.globalLockedUntil
	for _, t := range []time.Time{st.icecastAdmin.lockedUntil, st.source.lockedUntil, st.adminAPI.lockedUntil} {
		if t.After(latest) {
			latest = t
		}
	}
	return latest
}

func (st *ipAuthState) activeScopeFailures() int {
	return st.icecastAdmin.failures + st.source.failures + st.adminAPI.failures
}

func (st *ipAuthState) targetBucket(now time.Time) int {
	if now.Before(st.latestLockedUntil()) {
		return bucketLocked
	}
	if st.activeScopeFailures() > 0 || st.totalFailures > 0 {
		return bucketFailing
	}
	return bucketIdle
}

func normalizeIPKey(ipStr string, ipv6PrefixBits int) string {
	ip := net.ParseIP(strings.TrimSpace(ipStr))
	if ip == nil {
		return ipStr
	}
	if ip4 := ip.To4(); ip4 != nil {
		return ip4.String()
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return ipStr
	}
	bits := ipv6PrefixBits
	if bits <= 0 {
		bits = defaultAuthIPv6PrefixBits
	}
	if bits >= 128 {
		return ip16.String()
	}
	masked := ip16.Mask(net.CIDRMask(bits, 128))
	return fmt.Sprintf("%s/%d", masked.String(), bits)
}

func newAuthLimiter() *authLimiter {
	return &authLimiter{
		ips:            make(map[string]*ipAuthState),
		baseDelay:      defaultAuthBaseDelay,
		maxDelay:       maxAuthLockoutDelay,
		decayInterval:  defaultAuthDecayPeriod,
		ttl:            defaultAuthEntryTTL,
		maxEntries:     defaultAuthMaxEntries,
		ipv6PrefixBits: defaultAuthIPv6PrefixBits,
		nowFn:          time.Now,
	}
}

func (l *authLimiter) now() time.Time {
	if l != nil && l.nowFn != nil {
		return l.nowFn()
	}
	return time.Now()
}

func (l *authLimiter) setIPv6PrefixBits(bits int) {
	if bits < 1 || bits > 128 {
		return
	}
	l.mu.Lock()
	l.ipv6PrefixBits = bits
	l.mu.Unlock()
}

func (l *authLimiter) key(ip string) string {
	return normalizeIPKey(ip, l.ipv6PrefixBits)
}

func (l *authLimiter) removeNode(st *ipAuthState) {
	if !st.inList {
		return
	}
	b := st.bucket
	if st.prev != nil {
		st.prev.next = st.next
	} else if l.heads[b] == st {
		l.heads[b] = st.next
	}
	if st.next != nil {
		st.next.prev = st.prev
	} else if l.tails[b] == st {
		l.tails[b] = st.prev
	}
	st.prev = nil
	st.next = nil
	st.inList = false
}

func (l *authLimiter) pushFront(st *ipAuthState, b int) {
	st.bucket = b
	st.prev = nil
	st.next = l.heads[b]
	if l.heads[b] != nil {
		l.heads[b].prev = st
	}
	l.heads[b] = st
	if l.tails[b] == nil {
		l.tails[b] = st
	}
	st.inList = true
}

func (l *authLimiter) touch(st *ipAuthState, now time.Time) {
	st.lastSeen = now
	b := st.targetBucket(now)
	if st.inList && st.bucket == b && l.heads[b] == st {
		return
	}
	l.removeNode(st)
	l.pushFront(st, b)
}

func (l *authLimiter) evictNode(st *ipAuthState) {
	l.removeNode(st)
	delete(l.ips, st.ip)
}

func (l *authLimiter) decayLocked(st *ipAuthState, now time.Time) {
	if st.lastFailure.IsZero() {
		return
	}
	period := l.decayInterval
	if period <= 0 {
		period = defaultAuthDecayPeriod
	}
	elapsed := now.Sub(st.lastFailure)
	if elapsed < period {
		return
	}
	steps := int(elapsed / period)
	if steps >= 30 {
		st.totalFailures = 0
		st.icecastAdmin.failures = 0
		st.source.failures = 0
		st.adminAPI.failures = 0
	} else {
		st.totalFailures >>= steps
		st.icecastAdmin.failures >>= steps
		st.source.failures >>= steps
		st.adminAPI.failures >>= steps
	}
	if st.totalFailures < globalLockoutThreshold {
		st.globalLockedUntil = time.Time{}
	}
	st.lastFailure = st.lastFailure.Add(time.Duration(steps) * period)
}

func (l *authLimiter) isExpired(st *ipAuthState, now time.Time) bool {
	if now.Before(st.latestLockedUntil()) {
		return false
	}
	ttl := l.ttl
	if ttl <= 0 {
		ttl = defaultAuthEntryTTL
	}
	if st.lastLockout > ttl {
		ttl = st.lastLockout
	}
	return now.Sub(st.lastSeen) > ttl
}

func (l *authLimiter) evictExpiredLocked(now time.Time) {
	ttl := l.ttl
	if ttl <= 0 {
		ttl = defaultAuthEntryTTL
	}
	for b := 0; b < len(l.tails); b++ {
		for cur := l.tails[b]; cur != nil; {
			prev := cur.prev
			if now.Sub(cur.lastSeen) <= ttl {
				break
			}
			if l.isExpired(cur, now) {
				l.evictNode(cur)
			}
			cur = prev
		}
	}
}

func (l *authLimiter) evictOneVictimLocked(now time.Time) {
	// Refresh bucket classification of entries in bucketLocked or bucketFailing whose lockouts/failures decayed.
	for cur := l.tails[bucketLocked]; cur != nil; {
		prev := cur.prev
		l.decayLocked(cur, now)
		if tb := cur.targetBucket(now); tb != bucketLocked {
			l.removeNode(cur)
			l.pushFront(cur, tb)
		}
		cur = prev
	}
	for cur := l.tails[bucketFailing]; cur != nil; {
		prev := cur.prev
		l.decayLocked(cur, now)
		if tb := cur.targetBucket(now); tb != bucketFailing {
			l.removeNode(cur)
			l.pushFront(cur, tb)
		}
		cur = prev
	}

	// 1. Prefer evicting entries with zero failures and no active lockout (bucketIdle, LRU first).
	if l.tails[bucketIdle] != nil {
		l.evictNode(l.tails[bucketIdle])
		return
	}
	// 2. Next, evict entries with no active lockout (bucketFailing, LRU first).
	if l.tails[bucketFailing] != nil {
		l.evictNode(l.tails[bucketFailing])
		return
	}
	// 3. Every entry is actively locked out: evict the one whose lockout expires soonest.
	var soonest *ipAuthState
	var soonestTime time.Time
	for cur := l.tails[bucketLocked]; cur != nil; cur = cur.prev {
		until := cur.latestLockedUntil()
		if soonest == nil || until.Before(soonestTime) {
			soonest = cur
			soonestTime = until
		}
	}
	if soonest != nil {
		l.evictNode(soonest)
	}
}

func (l *authLimiter) state(key string, now time.Time) *ipAuthState {
	if l.ips == nil {
		l.ips = make(map[string]*ipAuthState)
	}
	st, ok := l.ips[key]
	if ok {
		l.decayLocked(st, now)
		l.touch(st, now)
		return st
	}
	maxEntries := l.maxEntries
	if maxEntries <= 0 {
		maxEntries = defaultAuthMaxEntries
	}
	for len(l.ips) >= maxEntries && len(l.ips) > 0 {
		l.evictOneVictimLocked(now)
	}
	st = &ipAuthState{ip: key, lastSeen: now}
	l.ips[key] = st
	l.pushFront(st, bucketIdle)
	return st
}

func (l *authLimiter) lockoutDelay(overThreshold int) time.Duration {
	base := l.baseDelay
	if base <= 0 {
		base = defaultAuthBaseDelay
	}
	maxDelay := l.maxDelay
	if maxDelay <= 0 {
		maxDelay = maxAuthLockoutDelay
	}
	shift := overThreshold
	if shift < 0 {
		shift = 0
	}
	if shift > 12 {
		shift = 12
	}
	delay := base << shift
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}

func (l *authLimiter) checkLocked(ip, scope string) (bool, time.Duration, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.evictExpiredLocked(now)
	key := l.key(ip)
	st, ok := l.ips[key]
	if !ok {
		return false, 0, 0
	}
	l.decayLocked(st, now)
	sc := st.scopeState(scope)
	var lockedUntil time.Time
	failures := sc.failures
	if now.Before(sc.lockedUntil) {
		lockedUntil = sc.lockedUntil
	}
	if now.Before(st.globalLockedUntil) && st.globalLockedUntil.After(lockedUntil) {
		lockedUntil = st.globalLockedUntil
		if st.totalFailures > failures {
			failures = st.totalFailures
		}
	}
	l.touch(st, now)
	if !lockedUntil.IsZero() {
		return true, lockedUntil.Sub(now), failures
	}
	return false, 0, failures
}

// allowBcrypt rate-limits bcrypt comparisons per IP before running the expensive
// hash check to prevent CPU exhaustion under concurrent brute-force floods.
func (l *authLimiter) allowBcrypt(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.evictExpiredLocked(now)
	key := l.key(ip)
	st := l.state(key, now)
	if now.Before(st.latestLockedUntil()) {
		return false
	}
	if st.bcryptWindow.IsZero() || now.Sub(st.bcryptWindow) >= time.Second {
		st.bcryptWindow = now
		st.bcryptCount = 0
	}
	if st.bcryptCount >= authLockoutThreshold {
		return false
	}
	st.bcryptCount++
	return true
}

func (l *authLimiter) recordFailure(ip, scope string) (int, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.evictExpiredLocked(now)
	key := l.key(ip)
	st := l.state(key, now)
	sc := st.scopeState(scope)
	sc.failures++
	st.totalFailures++
	st.lastFailure = now

	var scopeDelay time.Duration
	if sc.failures >= authLockoutThreshold {
		scopeDelay = l.lockoutDelay(sc.failures - authLockoutThreshold)
		sc.lockedUntil = now.Add(scopeDelay)
	}

	var globalDelay time.Duration
	if st.totalFailures >= globalLockoutThreshold {
		globalDelay = l.lockoutDelay(st.totalFailures - globalLockoutThreshold)
		st.globalLockedUntil = now.Add(globalDelay)
	}

	delay := scopeDelay
	if globalDelay > delay {
		delay = globalDelay
	}
	if delay > st.lastLockout {
		st.lastLockout = delay
	}
	l.touch(st, now)
	return sc.failures, delay
}

func (l *authLimiter) recordSuccess(ip, scope string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.evictExpiredLocked(now)
	key := l.key(ip)
	st, ok := l.ips[key]
	if !ok {
		return
	}
	l.decayLocked(st, now)
	sc := st.scopeState(scope)
	sc.failures = 0
	sc.lockedUntil = time.Time{}
	st.totalFailures = st.activeScopeFailures()
	if st.totalFailures < globalLockoutThreshold {
		st.globalLockedUntil = time.Time{}
	}
	l.touch(st, now)
}

func (l *authLimiter) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.evictExpiredLocked(l.now())
	return len(l.ips)
}

func (s *Server) getAuthLimiter() *authLimiter {
	s.authMu.Lock()
	if s.auth == nil {
		s.auth = newAuthLimiter()
	}
	lim := s.auth
	s.authMu.Unlock()
	if cfg := s.Config(); cfg != nil && cfg.Server.IPv6PrefixBits > 0 {
		lim.setIPv6PrefixBits(cfg.Server.IPv6PrefixBits)
	}
	return lim
}

func writeAuthLockout(w http.ResponseWriter, retryAfter time.Duration) {
	secs := int(math.Ceil(retryAfter.Seconds()))
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	http.Error(w, "too many failed authentication attempts", http.StatusTooManyRequests)
}

func (s *Server) registerIcecastAdmin(mux *http.ServeMux) {
	mux.HandleFunc("/admin/stats", s.adminBasicAuth(s.handleAdminStats))
	mux.HandleFunc("/admin/stats.xml", s.adminBasicAuth(s.handleAdminStats))
	mux.HandleFunc("/admin/listmounts", s.adminBasicAuth(s.handleAdminListMounts))
	mux.HandleFunc("/admin/listclients", s.adminBasicAuth(s.handleAdminListClients))
	mux.HandleFunc("/admin/killclient", s.adminBasicAuth(s.handleAdminKillClient))
	mux.HandleFunc("/admin/killsource", s.adminBasicAuth(s.handleAdminKillSource))
}

func (s *Server) icecastAdminUser() string {
	cfg := s.Config()
	if cfg != nil {
		if cfg.Admin.IcecastAdminUser != "" {
			return cfg.Admin.IcecastAdminUser
		}
		if cfg.Server.IcecastAdminUser != "" {
			return cfg.Server.IcecastAdminUser
		}
	}
	return "admin"
}

func (s *Server) icecastAdminPassword() string {
	cfg := s.Config()
	if cfg == nil {
		return ""
	}
	if cfg.Admin.IcecastAdminPassword != "" {
		return cfg.Admin.IcecastAdminPassword
	}
	if cfg.Server.IcecastAdminPassword != "" {
		return cfg.Server.IcecastAdminPassword
	}
	if cfg.Admin.IcecastAdminPasswordEnv != "" {
		if b, err := config.ResolveSecret(cfg.Admin.IcecastAdminPasswordEnv, ""); err == nil {
			return string(b)
		}
		return ""
	}
	if cfg.Admin.IcecastAdminPasswordFile != "" {
		if b, err := config.ResolveSecret("", cfg.Admin.IcecastAdminPasswordFile); err == nil {
			return string(b)
		}
		return ""
	}
	return os.Getenv("KITE_ICECAST_ADMIN_PASSWORD")
}

func (s *Server) adminBasicAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		expectedPass := s.icecastAdminPassword()
		if expectedPass == "" {
			http.Error(w, "icecast admin password is not configured", http.StatusServiceUnavailable)
			return
		}
		ok, locked, retryAfter := s.authenticateBasic(r, "icecast_admin", s.icecastAdminUser(), expectedPass)
		if locked {
			writeAuthLockout(w, retryAfter)
			return
		}
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="Icecast2 Server"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) safeLogUsername(user, expectedUser string) string {
	if user == "" {
		return "[unknown]"
	}
	if expectedUser != "" && user == expectedUser {
		return user
	}
	if user == s.icecastAdminUser() {
		return user
	}
	if cfg := s.Config(); cfg != nil {
		for _, m := range cfg.Mounts {
			if m.Source.Username != "" && user == m.Source.Username {
				return user
			}
		}
	}
	return "[unknown]"
}

func (s *Server) authenticateBasic(r *http.Request, scope, expectedUser, expectedPass string) (ok bool, locked bool, retryAfter time.Duration) {
	ip := s.requestClientIP(r)
	limiter := s.getAuthLimiter()
	user, pass, hasBasic := r.BasicAuth()
	logUser := s.safeLogUsername(user, expectedUser)
	if isLocked, wait, failures := limiter.checkLocked(ip, scope); isLocked {
		s.log.Warn("authentication rejected: IP locked out", "scope", scope, "ip", ip, "user", logUser, "password", "[REDACTED]", "failures", failures, "retry_after", wait.Round(time.Millisecond).String())
		return false, true, wait
	}
	if strings.TrimSpace(r.Header.Get("Authorization")) == "" {
		return false, false, 0
	}
	if expectedPass == "" || !hasBasic || user != expectedUser {
		failures, lockDelay := limiter.recordFailure(ip, scope)
		s.log.Warn("authentication failed", "scope", scope, "ip", ip, "user", logUser, "password", "[REDACTED]", "failures", failures, "lockout", lockDelay.Round(time.Millisecond).String(), "retry_after", lockDelay.Round(time.Millisecond).String())
		return false, false, 0
	}
	if strings.HasPrefix(expectedPass, "$2") {
		if !limiter.allowBcrypt(ip) {
			failures, lockDelay := limiter.recordFailure(ip, scope)
			if lockDelay == 0 {
				lockDelay = time.Second
			}
			s.log.Warn("authentication rate-limited before bcrypt", "scope", scope, "ip", ip, "user", logUser, "password", "[REDACTED]", "failures", failures, "retry_after", lockDelay.Round(time.Millisecond).String())
			return false, true, lockDelay
		}
		if bcrypt.CompareHashAndPassword([]byte(expectedPass), []byte(pass)) != nil {
			failures, lockDelay := limiter.recordFailure(ip, scope)
			s.log.Warn("authentication failed", "scope", scope, "ip", ip, "user", logUser, "password", "[REDACTED]", "failures", failures, "lockout", lockDelay.Round(time.Millisecond).String(), "retry_after", lockDelay.Round(time.Millisecond).String())
			return false, false, 0
		}
		limiter.recordSuccess(ip, scope)
		return true, false, 0
	}
	if subtle.ConstantTimeCompare([]byte(expectedPass), []byte(pass)) != 1 {
		failures, lockDelay := limiter.recordFailure(ip, scope)
		s.log.Warn("authentication failed", "scope", scope, "ip", ip, "user", logUser, "password", "[REDACTED]", "failures", failures, "lockout", lockDelay.Round(time.Millisecond).String(), "retry_after", lockDelay.Round(time.Millisecond).String())
		return false, false, 0
	}
	limiter.recordSuccess(ip, scope)
	return true, false, 0
}

func (s *Server) parseSourceStreamMetadata(r *http.Request) stream.SourceStreamMetadata {
	h := r.Header
	audioInfo := strings.TrimSpace(h.Get("ice-audio-info"))
	meta := stream.SourceStreamMetadata{
		Name:        strings.TrimSpace(headerFirst(h, "ice-name", "icy-name")),
		Description: strings.TrimSpace(headerFirst(h, "ice-description", "icy-description")),
		Genre:       strings.TrimSpace(headerFirst(h, "ice-genre", "icy-genre")),
		URL:         strings.TrimSpace(headerFirst(h, "ice-url", "icy-url")),
		AudioInfo:   audioInfo,
		SourceIP:    s.requestClientIP(r),
		ContentType: strings.TrimSpace(h.Get("Content-Type")),
	}
	if rawBr := strings.TrimSpace(headerFirst(h, "ice-bitrate", "icy-br", "icy-bitrate")); rawBr != "" {
		if br, err := strconv.Atoi(rawBr); err == nil && br > 0 {
			meta.Bitrate = config.NormalizeBitrateKbps(br)
		}
	}
	if meta.Bitrate == 0 && audioInfo != "" {
		meta.Bitrate = config.NormalizeBitrateKbps(parseBitrateFromAudioInfo(audioInfo))
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

func remoteAddrIP(remoteAddr string) string {
	if remoteAddr == "" {
		return "127.0.0.1"
	}
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil && host != "" {
		return host
	}
	return remoteAddr
}

func (s *Server) isTrustedIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	cfg := s.Config()
	if cfg == nil {
		return false
	}
	for _, list := range [][]string{cfg.Server.TrustedProxies, cfg.Server.TrustedProxyNetworks} {
		for _, entry := range list {
			if _, cidr, err := net.ParseCIDR(entry); err == nil {
				if cidr.Contains(ip) {
					return true
				}
				continue
			}
			if trustedIP := net.ParseIP(entry); trustedIP != nil && trustedIP.Equal(ip) {
				return true
			}
		}
	}
	return false
}

func (s *Server) isTrustedProxy(remoteAddr string) bool {
	return s.isTrustedIP(net.ParseIP(remoteAddrIP(remoteAddr)))
}

func (s *Server) requestClientIP(r *http.Request) string {
	if r == nil {
		return "127.0.0.1"
	}
	remoteIPStr := remoteAddrIP(r.RemoteAddr)
	if !s.isTrustedIP(net.ParseIP(remoteIPStr)) {
		return remoteIPStr
	}
	if xffHeaders := r.Header.Values("X-Forwarded-For"); len(xffHeaders) > 0 {
		for hIdx := len(xffHeaders) - 1; hIdx >= 0; hIdx-- {
			parts := strings.Split(xffHeaders[hIdx], ",")
			for i := len(parts) - 1; i >= 0; i-- {
				candidate := strings.TrimSpace(parts[i])
				ip := net.ParseIP(candidate)
				if ip == nil {
					continue
				}
				if s.isTrustedIP(ip) {
					continue
				}
				return ip.String()
			}
		}
		return remoteIPStr
	}
	if xrip := strings.TrimSpace(r.Header.Get("X-Real-IP")); xrip != "" {
		if ip := net.ParseIP(xrip); ip != nil && !s.isTrustedIP(ip) {
			return ip.String()
		}
	}
	return remoteIPStr
}

func firstForwardedValue(v string) string {
	if idx := strings.IndexByte(v, ','); idx >= 0 {
		v = v[:idx]
	}
	return strings.TrimSpace(v)
}

func (s *Server) requestBaseURL(r *http.Request) string {
	cfg := s.Config()
	if cfg != nil && cfg.Server.PublicURL != "" {
		return strings.TrimRight(cfg.Server.PublicURL, "/")
	}

	trusted := r != nil && s.isTrustedProxy(r.RemoteAddr)
	scheme := "http"
	if r != nil && (r.TLS != nil || (r.URL != nil && strings.EqualFold(r.URL.Scheme, "https"))) {
		scheme = "https"
	}
	if trusted {
		if xfProto := strings.ToLower(firstForwardedValue(r.Header.Get("X-Forwarded-Proto"))); xfProto == "https" || xfProto == "http" {
			scheme = xfProto
		}
	}

	if cfg != nil && cfg.Server.Hostname != "" {
		return scheme + "://" + cfg.Server.Hostname
	}

	if trusted {
		if xfHost := firstForwardedValue(r.Header.Get("X-Forwarded-Host")); xfHost != "" {
			return scheme + "://" + xfHost
		}
	}

	host := "localhost"
	if r != nil && r.Host != "" {
		host = r.Host
	}
	return scheme + "://" + host
}

func (s *Server) requestHostOnly(r *http.Request) string {
	base := s.requestBaseURL(r)
	if u, err := url.Parse(base); err == nil && u.Host != "" {
		if h, _, err := net.SplitHostPort(u.Host); err == nil && h != "" {
			return h
		}
		return u.Host
	}
	if cfg := s.Config(); cfg != nil && len(cfg.TLS.Hosts) > 0 && cfg.TLS.Hosts[0] != "" {
		return cfg.TLS.Hosts[0]
	}
	return "localhost"
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
	Source             any    `json:"source,omitempty"`
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
	Artist             string `json:"artist,omitempty"`
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
	streamStart := m.StreamStart()
	var startStr, startISO string
	if !streamStart.IsZero() {
		startStr = streamStart.Format(icecastTimeFormat)
		startISO = streamStart.Format(icecastISO8601Format)
	}
	return icecastJSONSource{
		ListenURL:          s.requestBaseURL(r) + st.Path,
		ServerName:         meta.Name,
		ServerDescription:  meta.Description,
		ServerType:         st.ContentType,
		ServerURL:          meta.URL,
		Genre:              meta.Genre,
		Bitrate:            meta.Bitrate,
		Title:              st.Metadata.DisplayTitle(),
		Artist:             st.Metadata.Artist,
		Listeners:          st.Listeners,
		ListenerPeak:       m.ListenerPeak(),
		StreamStart:        startStr,
		StreamStartISO8601: startISO,
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
	items := make([]icecastJSONSource, 0)
	for _, m := range s.hub.Mounts() {
		mCfg := m.Config()
		if mCfg.Hidden {
			continue
		}
		st := m.Status()
		if !st.Source {
			continue
		}
		items = append(items, s.buildJSONSource(r, m))
	}
	var sourceField any
	switch len(items) {
	case 0:
		sourceField = nil
	case 1:
		sourceField = items[0]
	default:
		sourceField = items
	}
	start := s.startedTime()
	stats := icecastJSONStats{
		Admin:              s.serverAdminEmail(),
		Host:               s.requestHostOnly(r),
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
	Artist             string `xml:"artist,omitempty"`
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
	SourceIP           string `xml:"source_ip,omitempty"`
	StreamStart        string `xml:"stream_start,omitempty"`
	StreamStartISO8601 string `xml:"stream_start_iso8601,omitempty"`
	Title              string `xml:"title"`
	TotalBytesRead     uint64 `xml:"total_bytes_read"`
	TotalBytesSent     uint64 `xml:"total_bytes_sent"`
}

func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	mounts := s.hub.Mounts()
	sources := make([]xmlStatsSource, 0, len(mounts))
	totalListeners := 0
	directSources := 0
	relaySources := 0
	now := time.Now()
	for _, m := range mounts {
		st := m.Status()
		meta := m.StreamMetadata()
		streamStart := m.StreamStart()
		var connectedSec int64
		var streamStartStr, streamStartISO string
		if st.Source && !streamStart.IsZero() {
			connectedSec = int64(now.Sub(streamStart).Seconds())
			if connectedSec < 0 {
				connectedSec = 0
			}
			streamStartStr = streamStart.Format(icecastTimeFormat)
			streamStartISO = streamStart.Format(icecastISO8601Format)
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
			Artist:             st.Metadata.Artist,
			Bitrate:            meta.Bitrate,
			Connected:          connectedSec,
			Genre:              meta.Genre,
			ListenerPeak:       m.ListenerPeak(),
			Listeners:          st.Listeners,
			ListenURL:          s.requestBaseURL(r) + st.Path,
			MaxListeners:       "unlimited",
			Public:             pub,
			ServerDescription:  meta.Description,
			ServerName:         meta.Name,
			ServerType:         st.ContentType,
			ServerURL:          meta.URL,
			SlowListeners:      0,
			SourceIP:           m.SourceIP(),
			StreamStart:        streamStartStr,
			StreamStartISO8601: streamStartISO,
			Title:              st.Metadata.DisplayTitle(),
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
		Host:                    s.requestHostOnly(r),
		ListenerConnections:     totalListeners,
		Listeners:               totalListeners,
		Location:                defaultIcecastLoc,
		ServerID:                defaultIcecastID,
		ServerStart:             start.Format(icecastTimeFormat),
		ServerStartISO8601:      start.Format(icecastISO8601Format),
		SourceClientConnections: directSources,
		SourceRelayConnections:  relaySources,
		SourceTotalConnections:  activeSources,
		Sources:                 activeSources,
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
	Artist            string `xml:"artist,omitempty"`
}

func (s *Server) handleAdminListMounts(w http.ResponseWriter, r *http.Request) {
	mounts := s.hub.Mounts()
	out := make([]xmlMountRecord, 0, len(mounts))
	now := time.Now()
	for _, m := range mounts {
		st := m.Status()
		if !st.Source {
			continue
		}
		meta := m.StreamMetadata()
		streamStart := m.StreamStart()
		var connectedSec int64
		if !streamStart.IsZero() {
			connectedSec = int64(now.Sub(streamStart).Seconds())
			if connectedSec < 0 {
				connectedSec = 0
			}
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
			ListenURL:         s.requestBaseURL(r) + st.Path,
			Title:             st.Metadata.DisplayTitle(),
			Artist:            st.Metadata.Artist,
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
	mCfg := m.Config()
	if len(mCfg.Fallback) == 0 && mCfg.Relay == nil {
		m.CloseListeners("source_killed")
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
	Creator   string        `xml:"creator,omitempty"`
	TrackList xspfTrackList `xml:"trackList"`
}

type xspfTrackList struct {
	Track []xspfTrack `xml:"track"`
}

type xspfTrack struct {
	Location   string `xml:"location"`
	Creator    string `xml:"creator,omitempty"`
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
	listenURL := s.requestBaseURL(r) + m.Config().Path
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
	trackTitle := st.Metadata.DisplayTitle()
	if trackTitle == "" {
		trackTitle = meta.Name
	}
	artist := st.Metadata.Artist
	listenURL := s.requestBaseURL(r) + st.Path
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
