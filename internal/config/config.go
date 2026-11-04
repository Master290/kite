package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const CurrentVersion = 1

type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	v, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		*d = Duration(v)
		return nil
	}
	var ns int64
	if err := json.Unmarshal(data, &ns); err == nil {
		*d = Duration(ns)
		return nil
	}
	return fmt.Errorf("invalid duration %s", string(data))
}

type Config struct {
	Version  int      `yaml:"version" json:"version"`
	Server   Server   `yaml:"server" json:"server"`
	Admin    Admin    `yaml:"admin" json:"admin"`
	TLS      TLS      `yaml:"tls" json:"tls"`
	Defaults Defaults `yaml:"defaults" json:"defaults"`
	Mounts   []Mount  `yaml:"mounts" json:"mounts"`
}

type Server struct {
	HTTPAddress          string   `yaml:"http_address,omitempty" json:"http_address,omitempty"`
	HTTPSAddress         string   `yaml:"https_address,omitempty" json:"https_address,omitempty"`
	HTTP3Address         string   `yaml:"http3_address,omitempty" json:"http3_address,omitempty"`
	PublicHTTPSPort      int      `yaml:"public_https_port,omitempty" json:"public_https_port,omitempty"`
	PublicURL            string   `yaml:"public_url,omitempty" json:"public_url,omitempty"`
	Hostname             string   `yaml:"hostname,omitempty" json:"hostname,omitempty"`
	ReadHeaderTimeout    Duration `yaml:"read_header_timeout" json:"read_header_timeout"`
	IdleTimeout          Duration `yaml:"idle_timeout" json:"idle_timeout"`
	ShutdownTimeout      Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
	ListenerWriteTimeout Duration `yaml:"listener_write_timeout,omitempty" json:"listener_write_timeout,omitempty"`
	MaxHeaderBytes       int      `yaml:"max_header_bytes" json:"max_header_bytes"`
	HLSEnabled           *bool    `yaml:"hls_enabled,omitempty" json:"hls_enabled,omitempty"`
	StatusPageEnabled    *bool    `yaml:"status_page_enabled,omitempty" json:"status_page_enabled,omitempty"`
	TrustedProxies       []string `yaml:"trusted_proxies,omitempty" json:"trusted_proxies,omitempty"`
	TrustedProxyNetworks []string `yaml:"trusted_proxy_networks,omitempty" json:"trusted_proxy_networks,omitempty"`
	IPv6PrefixBits       int      `yaml:"ipv6_prefix_bits,omitempty" json:"ipv6_prefix_bits,omitempty"`
	IcecastAdminUser     string   `yaml:"icecast_admin_user,omitempty" json:"icecast_admin_user,omitempty"`
	IcecastAdminPassword string   `yaml:"icecast_admin_password,omitempty" json:"icecast_admin_password,omitempty"`
}

// HLS reports whether `.m3u8` playlists and TS segments are served. It
// defaults to true and can be toggled live through the admin config API.
func (s Server) HLS() bool { return s.HLSEnabled == nil || *s.HLSEnabled }

// StatusPage reports whether the built-in status and player page is served.
// It defaults to true and can be toggled live through the admin config API.
func (s Server) StatusPage() bool { return s.StatusPageEnabled == nil || *s.StatusPageEnabled }

type Admin struct {
	Address                  string `yaml:"address" json:"address"`
	TokenEnv                 string `yaml:"token_env,omitempty" json:"token_env,omitempty"`
	TokenFile                string `yaml:"token_file,omitempty" json:"token_file,omitempty"`
	IcecastAdminUser         string `yaml:"icecast_admin_user,omitempty" json:"icecast_admin_user,omitempty"`
	IcecastAdminPassword     string `yaml:"icecast_admin_password,omitempty" json:"icecast_admin_password,omitempty"`
	IcecastAdminPasswordEnv  string `yaml:"icecast_admin_password_env,omitempty" json:"icecast_admin_password_env,omitempty"`
	IcecastAdminPasswordFile string `yaml:"icecast_admin_password_file,omitempty" json:"icecast_admin_password_file,omitempty"`
}

type TLS struct {
	Mode                 string   `yaml:"mode" json:"mode"`
	Hosts                []string `yaml:"hosts,omitempty" json:"hosts,omitempty"`
	Email                string   `yaml:"email,omitempty" json:"email,omitempty"`
	CacheDirectory       string   `yaml:"cache_directory,omitempty" json:"cache_directory,omitempty"`
	CertificateFile      string   `yaml:"certificate_file,omitempty" json:"certificate_file,omitempty"`
	PrivateKeyFile       string   `yaml:"private_key_file,omitempty" json:"private_key_file,omitempty"`
	HTTPChallengeAddress string   `yaml:"http_challenge_address,omitempty" json:"http_challenge_address,omitempty"`
	ACMEDirectoryURL     string   `yaml:"acme_directory_url,omitempty" json:"acme_directory_url,omitempty"`
}

type Defaults struct {
	SourceTimeout        Duration `yaml:"source_timeout" json:"source_timeout"`
	FailbackDelay        Duration `yaml:"failback_delay" json:"failback_delay"`
	BufferDuration       Duration `yaml:"buffer_duration" json:"buffer_duration"`
	BurstBytes           int      `yaml:"burst_bytes,omitempty" json:"burst_bytes,omitempty"`
	WriteInterval        Duration `yaml:"write_interval" json:"write_interval"`
	ListenerWriteTimeout Duration `yaml:"listener_write_timeout" json:"listener_write_timeout"`
	RelayIdleTimeout     Duration `yaml:"relay_idle_timeout" json:"relay_idle_timeout"`
	RelayMaxBackoff      Duration `yaml:"relay_max_backoff" json:"relay_max_backoff"`
	ICYMetaInterval      int      `yaml:"icy_meta_interval" json:"icy_meta_interval"`
	MaxSourceBitrate     int      `yaml:"max_source_bitrate" json:"max_source_bitrate"`
}

type Mount struct {
	Path                 string           `yaml:"path" json:"path"`
	Hidden               bool             `yaml:"hidden,omitempty" json:"hidden,omitempty"`
	Profile              string           `yaml:"profile" json:"profile"`
	ContentType          string           `yaml:"content_type,omitempty" json:"content_type,omitempty"`
	Source               SourceCredential `yaml:"source" json:"source"`
	Relay                *RelayConfig     `yaml:"relay,omitempty" json:"relay,omitempty"`
	Metadata             Metadata         `yaml:"metadata,omitempty" json:"metadata,omitempty"`
	Fallback             []Fallback       `yaml:"fallback,omitempty" json:"fallback,omitempty"`
	SourceTimeout        Duration         `yaml:"source_timeout,omitempty" json:"source_timeout,omitempty"`
	FailbackDelay        Duration         `yaml:"failback_delay,omitempty" json:"failback_delay,omitempty"`
	BufferDuration       Duration         `yaml:"buffer_duration,omitempty" json:"buffer_duration,omitempty"`
	BurstBytes           int              `yaml:"burst_bytes,omitempty" json:"burst_bytes,omitempty"`
	ListenerWriteTimeout Duration         `yaml:"listener_write_timeout,omitempty" json:"listener_write_timeout,omitempty"`
	RelayIdleTimeout     Duration         `yaml:"relay_idle_timeout,omitempty" json:"relay_idle_timeout,omitempty"`
	RelayMaxBackoff      Duration         `yaml:"relay_max_backoff,omitempty" json:"relay_max_backoff,omitempty"`
	ICYMetaInterval      int              `yaml:"icy_meta_interval,omitempty" json:"icy_meta_interval,omitempty"`
	CORSOrigins          []string         `yaml:"cors_origins,omitempty" json:"cors_origins,omitempty"`
}

type RelayConfig struct {
	URL              string   `yaml:"url" json:"url"`
	Username         string   `yaml:"username,omitempty" json:"username,omitempty"`
	Password         string   `yaml:"password,omitempty" json:"password,omitempty"`
	PasswordEnv      string   `yaml:"password_env,omitempty" json:"password_env,omitempty"`
	PasswordFile     string   `yaml:"password_file,omitempty" json:"password_file,omitempty"`
	RetryDelay       Duration `yaml:"retry_delay,omitempty" json:"retry_delay,omitempty"`
	RelayIdleTimeout Duration `yaml:"relay_idle_timeout,omitempty" json:"relay_idle_timeout,omitempty"`
	RelayMaxBackoff  Duration `yaml:"relay_max_backoff,omitempty" json:"relay_max_backoff,omitempty"`
}

type SourceCredential struct {
	Username       string `yaml:"username,omitempty" json:"username,omitempty"`
	PasswordBcrypt string `yaml:"password_bcrypt,omitempty" json:"password_bcrypt,omitempty"`
	PasswordEnv    string `yaml:"password_env,omitempty" json:"password_env,omitempty"`
	PasswordFile   string `yaml:"password_file,omitempty" json:"password_file,omitempty"`
}

type Metadata struct {
	Name        string `yaml:"name,omitempty" json:"name,omitempty"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	Genre       string `yaml:"genre,omitempty" json:"genre,omitempty"`
	URL         string `yaml:"url,omitempty" json:"url,omitempty"`
	Bitrate     int    `yaml:"bitrate,omitempty" json:"bitrate,omitempty"`
	Public      bool   `yaml:"public,omitempty" json:"public,omitempty"`
	PublicSet   bool   `yaml:"-" json:"-"`
}

func (m *Metadata) UnmarshalYAML(node *yaml.Node) error {
	type rawMetadata Metadata
	var raw rawMetadata
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			switch node.Content[i].Value {
			case "name", "description", "genre", "url", "bitrate":
			case "public":
				raw.PublicSet = true
			default:
				return fmt.Errorf("unknown metadata field %q", node.Content[i].Value)
			}
		}
	}
	if err := node.Decode(&raw); err != nil {
		return err
	}
	*m = Metadata(raw)
	return nil
}

func (m Metadata) MarshalYAML() (any, error) {
	node := &yaml.Node{Kind: yaml.MappingNode}
	addStr := func(k, v string) {
		if v != "" {
			node.Content = append(node.Content,
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: k},
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v},
			)
		}
	}
	addStr("name", m.Name)
	addStr("description", m.Description)
	addStr("genre", m.Genre)
	addStr("url", m.URL)
	if m.Bitrate != 0 {
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "bitrate"},
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.Itoa(m.Bitrate)},
		)
	}
	if m.Public || m.PublicSet {
		val := "false"
		if m.Public {
			val = "true"
		}
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "public"},
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: val},
		)
	}
	return node, nil
}

func (m *Metadata) UnmarshalJSON(data []byte) error {
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &rawMap); err != nil {
		return err
	}
	for k := range rawMap {
		switch k {
		case "name", "description", "genre", "url", "bitrate", "public":
		default:
			return fmt.Errorf("unknown metadata field %q", k)
		}
	}
	type rawMetadata Metadata
	var raw rawMetadata
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if _, ok := rawMap["public"]; ok {
		raw.PublicSet = true
	}
	*m = Metadata(raw)
	return nil
}

func (m Metadata) MarshalJSON() ([]byte, error) {
	out := make(map[string]any, 6)
	if m.Name != "" {
		out["name"] = m.Name
	}
	if m.Description != "" {
		out["description"] = m.Description
	}
	if m.Genre != "" {
		out["genre"] = m.Genre
	}
	if m.URL != "" {
		out["url"] = m.URL
	}
	if m.Bitrate != 0 {
		out["bitrate"] = m.Bitrate
	}
	if m.Public || m.PublicSet {
		out["public"] = m.Public
	}
	return json.Marshal(out)
}

// NormalizeBitrateKbps converts a bitrate value to kbps. Values >= 1000 are
// treated as bps (e.g. 128000 -> 128) for backward compatibility.
func NormalizeBitrateKbps(v int) int {
	if v <= 0 {
		return 0
	}
	if v >= 1000 {
		return v / 1000
	}
	return v
}

// NormalizeBitrateBps converts a bitrate value to bps. Values < 1000 are
// treated as kbps (e.g. 128 -> 128000).
func NormalizeBitrateBps(v int) int {
	if v <= 0 {
		return 0
	}
	if v < 1000 {
		return v * 1000
	}
	return v
}

type Fallback struct {
	Mount    string `yaml:"mount,omitempty" json:"mount,omitempty"`
	File     string `yaml:"file,omitempty" json:"file,omitempty"`
	Folder   string `yaml:"folder,omitempty" json:"folder,omitempty"`
	Playlist string `yaml:"playlist,omitempty" json:"playlist,omitempty"`
	Shuffle  bool   `yaml:"shuffle,omitempty" json:"shuffle,omitempty"`
	Title    string `yaml:"title,omitempty" json:"title,omitempty"`
}

func Default() Config {
	return Config{
		Version: CurrentVersion,
		Server: Server{
			HTTPSAddress:      ":8443",
			HTTP3Address:      ":8443",
			PublicHTTPSPort:   8443,
			ReadHeaderTimeout: Duration(10 * time.Second),
			IdleTimeout:       Duration(30 * time.Second),
			ShutdownTimeout:   Duration(15 * time.Second),
			MaxHeaderBytes:    32 << 10,
		},
		Admin: Admin{Address: "127.0.0.1:9090", TokenEnv: "KITE_ADMIN_TOKEN"},
		TLS:   TLS{Mode: "development"},
		Defaults: Defaults{
			SourceTimeout:        Duration(3 * time.Second),
			FailbackDelay:        Duration(5 * time.Second),
			BufferDuration:       Duration(2 * time.Second),
			BurstBytes:           65536,
			WriteInterval:        Duration(100 * time.Millisecond),
			ListenerWriteTimeout: Duration(10 * time.Second),
			RelayIdleTimeout:     Duration(10 * time.Second),
			RelayMaxBackoff:      Duration(10 * time.Second),
			ICYMetaInterval:      16000,
			MaxSourceBitrate:     512000,
		},
	}
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b, filepath.Dir(path))
}

func Parse(b []byte, baseDir string) (*Config, error) {
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.Normalize(baseDir); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func Marshal(cfg *Config) ([]byte, error) {
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("encode config: %w", err)
	}
	return b, nil
}

func (c *Config) Normalize(baseDir string) error {
	if c.Version == 0 {
		c.Version = CurrentVersion
	}
	if c.Version != CurrentVersion {
		return fmt.Errorf("unsupported config version %d", c.Version)
	}
	if c.Server.ReadHeaderTimeout == 0 {
		c.Server.ReadHeaderTimeout = Duration(10 * time.Second)
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = Duration(30 * time.Second)
	}
	if c.Server.ShutdownTimeout == 0 {
		c.Server.ShutdownTimeout = Duration(15 * time.Second)
	}
	if c.Server.MaxHeaderBytes == 0 {
		c.Server.MaxHeaderBytes = 32 << 10
	}
	if c.Server.HLSEnabled == nil {
		hlsEnabled := true
		c.Server.HLSEnabled = &hlsEnabled
	}
	if c.Admin.Address == "" {
		c.Admin.Address = "127.0.0.1:9090"
	}
	if c.Admin.TokenFile != "" {
		c.Admin.TokenFile = absolute(baseDir, c.Admin.TokenFile)
	}
	if c.Admin.IcecastAdminUser == "" && c.Server.IcecastAdminUser != "" {
		c.Admin.IcecastAdminUser = c.Server.IcecastAdminUser
	}
	if c.Admin.IcecastAdminUser == "" {
		c.Admin.IcecastAdminUser = "admin"
	}
	if c.Admin.IcecastAdminPassword == "" && c.Server.IcecastAdminPassword != "" {
		c.Admin.IcecastAdminPassword = c.Server.IcecastAdminPassword
	}
	if c.Admin.IcecastAdminPasswordFile != "" {
		c.Admin.IcecastAdminPasswordFile = absolute(baseDir, c.Admin.IcecastAdminPasswordFile)
	}
	if c.Server.PublicURL != "" {
		u, err := url.Parse(c.Server.PublicURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("invalid server.public_url %q", c.Server.PublicURL)
		}
		c.Server.PublicURL = strings.TrimRight(c.Server.PublicURL, "/")
	}
	for _, entry := range append(append([]string(nil), c.Server.TrustedProxies...), c.Server.TrustedProxyNetworks...) {
		if _, _, err := net.ParseCIDR(entry); err != nil {
			if net.ParseIP(entry) == nil {
				return fmt.Errorf("invalid trusted proxy %q", entry)
			}
		}
	}
	if c.Server.IPv6PrefixBits == 0 {
		c.Server.IPv6PrefixBits = 64
	}
	if c.Server.IPv6PrefixBits < 1 || c.Server.IPv6PrefixBits > 128 {
		return errors.New("ipv6_prefix_bits must be between 1 and 128")
	}
	if c.Defaults.SourceTimeout == 0 {
		c.Defaults.SourceTimeout = Duration(3 * time.Second)
	}
	if c.Defaults.FailbackDelay == 0 {
		c.Defaults.FailbackDelay = Duration(5 * time.Second)
	}
	if c.Defaults.BufferDuration == 0 {
		c.Defaults.BufferDuration = Duration(2 * time.Second)
	}
	if c.Defaults.BurstBytes == 0 {
		c.Defaults.BurstBytes = 65536
	}
	if c.Defaults.BurstBytes < 0 {
		return errors.New("burst_bytes must be non-negative")
	}
	if c.Defaults.WriteInterval == 0 {
		c.Defaults.WriteInterval = Duration(100 * time.Millisecond)
	}
	if c.Defaults.ListenerWriteTimeout == 0 {
		if c.Server.ListenerWriteTimeout > 0 {
			c.Defaults.ListenerWriteTimeout = c.Server.ListenerWriteTimeout
		} else {
			c.Defaults.ListenerWriteTimeout = Duration(10 * time.Second)
		}
	}
	if c.Defaults.ListenerWriteTimeout < 0 || c.Server.ListenerWriteTimeout < 0 {
		return errors.New("listener_write_timeout must be positive")
	}
	if c.Defaults.RelayIdleTimeout == 0 {
		c.Defaults.RelayIdleTimeout = Duration(10 * time.Second)
	}
	if c.Defaults.RelayIdleTimeout < 0 {
		return errors.New("relay_idle_timeout must be positive")
	}
	if c.Defaults.RelayMaxBackoff == 0 {
		c.Defaults.RelayMaxBackoff = Duration(10 * time.Second)
	}
	if c.Defaults.RelayMaxBackoff < 0 {
		return errors.New("relay_max_backoff must be positive")
	}
	if c.Defaults.ICYMetaInterval == 0 {
		c.Defaults.ICYMetaInterval = 16000
	}
	if c.Defaults.MaxSourceBitrate == 0 {
		c.Defaults.MaxSourceBitrate = 512000
	}
	if c.Server.HTTPAddress == "" && c.Server.HTTPSAddress == "" {
		return errors.New("at least one of server.http_address or server.https_address is required")
	}
	for _, address := range []string{c.Server.HTTPAddress, c.Server.HTTPSAddress, c.Server.HTTP3Address, c.Admin.Address} {
		if address != "" {
			if _, _, err := net.SplitHostPort(address); err != nil {
				return fmt.Errorf("invalid listen address %q: %w", address, err)
			}
		}
	}
	if c.Server.HTTPSAddress != "" {
		switch c.TLS.Mode {
		case "development":
		case "files":
			if c.TLS.CertificateFile == "" || c.TLS.PrivateKeyFile == "" {
				return errors.New("tls files mode requires certificate_file and private_key_file")
			}
			c.TLS.CertificateFile = absolute(baseDir, c.TLS.CertificateFile)
			c.TLS.PrivateKeyFile = absolute(baseDir, c.TLS.PrivateKeyFile)
		case "acme":
			if len(c.TLS.Hosts) == 0 || c.TLS.Email == "" {
				return errors.New("tls acme mode requires hosts and email")
			}
			if c.TLS.CacheDirectory == "" {
				c.TLS.CacheDirectory = absolute(baseDir, "acme-cache")
			} else {
				c.TLS.CacheDirectory = absolute(baseDir, c.TLS.CacheDirectory)
			}
		default:
			return fmt.Errorf("unsupported tls mode %q", c.TLS.Mode)
		}
	}
	if c.Server.HTTP3Address != "" && c.Server.HTTPSAddress == "" {
		return errors.New("http3 requires server.https_address")
	}
	seen := make(map[string]int, len(c.Mounts))
	for i := range c.Mounts {
		m := &c.Mounts[i]
		if err := normalizeMount(m, c.Defaults, baseDir); err != nil {
			return fmt.Errorf("mount %d: %w", i, err)
		}
		if _, ok := seen[m.Path]; ok {
			return fmt.Errorf("duplicate mount path %q", m.Path)
		}
		seen[m.Path] = i
	}
	for i := range c.Mounts {
		for _, fb := range c.Mounts[i].Fallback {
			if fb.Mount != "" {
				j, ok := seen[fb.Mount]
				if !ok {
					return fmt.Errorf("mount %q references unknown fallback mount %q", c.Mounts[i].Path, fb.Mount)
				}
				if c.Mounts[j].Profile != c.Mounts[i].Profile {
					return fmt.Errorf("mount %q fallback %q has incompatible profile", c.Mounts[i].Path, fb.Mount)
				}
			}
		}
	}
	if err := detectFallbackCycles(c.Mounts, seen); err != nil {
		return err
	}
	return nil
}

func normalizeMount(m *Mount, d Defaults, baseDir string) error {
	if m.Path == "" || !strings.HasPrefix(m.Path, "/") || strings.ContainsAny(m.Path, "?#") {
		return fmt.Errorf("invalid path %q", m.Path)
	}
	if strings.HasPrefix(m.Path, "/_kite/") || m.Path == "/status-json.xsl" || strings.HasPrefix(m.Path, "/admin/") {
		return fmt.Errorf("path %q is reserved", m.Path)
	}
	switch m.Profile {
	case "mp3":
		if m.ContentType == "" {
			m.ContentType = "audio/mpeg"
		}
	case "aac-adts":
		if m.ContentType == "" {
			m.ContentType = "audio/aac"
		}
	case "ogg-opus":
		if m.ContentType == "" {
			m.ContentType = "audio/ogg; codecs=opus"
		}
	case "opaque":
		if m.ContentType == "" {
			return errors.New("opaque profile requires content_type")
		}
		if len(m.Fallback) > 0 {
			return errors.New("opaque profile cannot use fallback")
		}
	default:
		return fmt.Errorf("unsupported profile %q", m.Profile)
	}
	if m.Relay != nil {
		u, err := url.Parse(m.Relay.URL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("invalid relay url %q", m.Relay.URL)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("relay url scheme must be http or https, got %q", u.Scheme)
		}
		if m.Relay.PasswordFile != "" {
			m.Relay.PasswordFile = absolute(baseDir, m.Relay.PasswordFile)
		}
		if m.Relay.RetryDelay == 0 {
			m.Relay.RetryDelay = Duration(3 * time.Second)
		}
	}
	credentials := 0
	for _, value := range []string{m.Source.PasswordBcrypt, m.Source.PasswordEnv, m.Source.PasswordFile} {
		if value != "" {
			credentials++
		}
	}
	if m.Relay == nil {
		if m.Source.Username == "" {
			m.Source.Username = "source"
		}
		if credentials != 1 {
			return errors.New("source requires exactly one of password_bcrypt, password_env, or password_file")
		}
	} else if credentials > 0 {
		if credentials != 1 {
			return errors.New("source requires exactly one of password_bcrypt, password_env, or password_file")
		}
		if m.Source.Username == "" {
			m.Source.Username = "source"
		}
	}
	if m.Source.PasswordFile != "" {
		m.Source.PasswordFile = absolute(baseDir, m.Source.PasswordFile)
	}
	if m.SourceTimeout == 0 {
		m.SourceTimeout = d.SourceTimeout
	}
	if m.FailbackDelay == 0 {
		m.FailbackDelay = d.FailbackDelay
	}
	if m.BufferDuration == 0 {
		m.BufferDuration = d.BufferDuration
	}
	if m.BurstBytes == 0 {
		m.BurstBytes = d.BurstBytes
	}
	if m.BurstBytes < 0 {
		return errors.New("burst_bytes must be non-negative")
	}
	if m.ListenerWriteTimeout == 0 {
		m.ListenerWriteTimeout = d.ListenerWriteTimeout
	}
	if m.ListenerWriteTimeout <= 0 {
		return errors.New("listener_write_timeout must be positive")
	}
	if m.RelayIdleTimeout == 0 {
		m.RelayIdleTimeout = d.RelayIdleTimeout
	}
	if m.RelayIdleTimeout <= 0 {
		return errors.New("relay_idle_timeout must be positive")
	}
	if m.RelayMaxBackoff == 0 {
		m.RelayMaxBackoff = d.RelayMaxBackoff
	}
	if m.RelayMaxBackoff <= 0 {
		return errors.New("relay_max_backoff must be positive")
	}
	if m.Relay != nil {
		if m.Relay.RelayIdleTimeout == 0 {
			m.Relay.RelayIdleTimeout = m.RelayIdleTimeout
		}
		if m.Relay.RelayIdleTimeout <= 0 {
			return errors.New("relay_idle_timeout must be positive")
		}
		if m.Relay.RelayMaxBackoff == 0 {
			m.Relay.RelayMaxBackoff = m.RelayMaxBackoff
		}
		if m.Relay.RelayMaxBackoff <= 0 {
			return errors.New("relay_max_backoff must be positive")
		}
	}
	if m.ICYMetaInterval == 0 {
		m.ICYMetaInterval = d.ICYMetaInterval
	}
	if m.SourceTimeout.Duration() < 250*time.Millisecond {
		return errors.New("source_timeout must be at least 250ms")
	}
	if m.BufferDuration.Duration() < 250*time.Millisecond {
		return errors.New("buffer_duration must be at least 250ms")
	}
	if m.ICYMetaInterval < 256 || m.ICYMetaInterval > 1<<20 {
		return errors.New("icy_meta_interval must be between 256 and 1048576")
	}
	for i := range m.Fallback {
		fb := &m.Fallback[i]
		count := 0
		if fb.Mount != "" {
			count++
		}
		if fb.File != "" {
			count++
		}
		if fb.Folder != "" {
			count++
		}
		if fb.Playlist != "" {
			count++
		}
		if count != 1 {
			return errors.New("fallback requires exactly one of mount, file, folder, or playlist")
		}
		if fb.Mount != "" && !strings.HasPrefix(fb.Mount, "/") {
			return fmt.Errorf("invalid fallback mount %q", fb.Mount)
		}
		if fb.File != "" {
			fb.File = absolute(baseDir, fb.File)
		}
		if fb.Folder != "" {
			fb.Folder = absolute(baseDir, fb.Folder)
		}
		if fb.Playlist != "" {
			fb.Playlist = absolute(baseDir, fb.Playlist)
		}
	}
	for _, origin := range m.CORSOrigins {
		if origin == "*" {
			continue
		}
		u, err := url.Parse(origin)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("invalid CORS origin %q", origin)
		}
	}
	return nil
}

func detectFallbackCycles(mounts []Mount, indexes map[string]int) error {
	state := make([]uint8, len(mounts))
	var visit func(int) error
	visit = func(i int) error {
		if state[i] == 1 {
			return fmt.Errorf("fallback cycle includes mount %q", mounts[i].Path)
		}
		if state[i] == 2 {
			return nil
		}
		state[i] = 1
		for _, fb := range mounts[i].Fallback {
			if fb.Mount != "" {
				if err := visit(indexes[fb.Mount]); err != nil {
					return err
				}
			}
		}
		state[i] = 2
		return nil
	}
	for i := range mounts {
		if err := visit(i); err != nil {
			return err
		}
	}
	return nil
}

func absolute(baseDir, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Clean(filepath.Join(baseDir, path))
}

func (c *Config) Mount(path string) (Mount, bool) {
	for _, m := range c.Mounts {
		if m.Path == path {
			return m, true
		}
	}
	return Mount{}, false
}

func (c *Config) Paths() []string {
	paths := make([]string, 0, len(c.Mounts))
	for _, m := range c.Mounts {
		paths = append(paths, m.Path)
	}
	sort.Strings(paths)
	return paths
}

func (c *Config) StaticKey() string {
	return strings.Join([]string{
		c.Server.HTTPAddress, c.Server.HTTPSAddress, c.Server.HTTP3Address, fmt.Sprint(c.Server.PublicHTTPSPort),
		c.Server.ReadHeaderTimeout.Duration().String(), c.Server.IdleTimeout.Duration().String(), c.Server.ShutdownTimeout.Duration().String(), fmt.Sprint(c.Server.MaxHeaderBytes), strings.Join(c.Server.TrustedProxyNetworks, ","),
		c.Admin.Address, c.TLS.Mode, strings.Join(c.TLS.Hosts, ","), c.TLS.Email, c.TLS.CacheDirectory, c.TLS.CertificateFile, c.TLS.PrivateKeyFile, c.TLS.HTTPChallengeAddress, c.TLS.ACMEDirectoryURL,
	}, "\x00")
}

func ResolveSecret(envName, fileName string) ([]byte, error) {
	if envName != "" {
		value, ok := os.LookupEnv(envName)
		if !ok {
			return nil, fmt.Errorf("environment variable %s is not set", envName)
		}
		return []byte(value), nil
	}
	b, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(b), nil
}
