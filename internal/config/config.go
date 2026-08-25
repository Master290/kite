package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
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
	ReadHeaderTimeout    Duration `yaml:"read_header_timeout" json:"read_header_timeout"`
	IdleTimeout          Duration `yaml:"idle_timeout" json:"idle_timeout"`
	ShutdownTimeout      Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
	MaxHeaderBytes       int      `yaml:"max_header_bytes" json:"max_header_bytes"`
	HLSEnabled           *bool    `yaml:"hls_enabled,omitempty" json:"hls_enabled,omitempty"`
	TrustedProxyNetworks []string `yaml:"trusted_proxy_networks,omitempty" json:"trusted_proxy_networks,omitempty"`
}

// HLS reports whether `.m3u8` playlists and TS segments are served. It
// defaults to true and can be toggled live through the admin config API.
func (s Server) HLS() bool { return s.HLSEnabled == nil || *s.HLSEnabled }

type Admin struct {
	Address   string `yaml:"address" json:"address"`
	TokenEnv  string `yaml:"token_env,omitempty" json:"token_env,omitempty"`
	TokenFile string `yaml:"token_file,omitempty" json:"token_file,omitempty"`
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
	SourceTimeout    Duration `yaml:"source_timeout" json:"source_timeout"`
	FailbackDelay    Duration `yaml:"failback_delay" json:"failback_delay"`
	BufferDuration   Duration `yaml:"buffer_duration" json:"buffer_duration"`
	WriteInterval    Duration `yaml:"write_interval" json:"write_interval"`
	ICYMetaInterval  int      `yaml:"icy_meta_interval" json:"icy_meta_interval"`
	MaxSourceBitrate int      `yaml:"max_source_bitrate" json:"max_source_bitrate"`
}

type Mount struct {
	Path            string           `yaml:"path" json:"path"`
	Hidden          bool             `yaml:"hidden,omitempty" json:"hidden,omitempty"`
	Profile         string           `yaml:"profile" json:"profile"`
	ContentType     string           `yaml:"content_type,omitempty" json:"content_type,omitempty"`
	Source          SourceCredential `yaml:"source" json:"source"`
	Metadata        Metadata         `yaml:"metadata,omitempty" json:"metadata,omitempty"`
	Fallback        []Fallback       `yaml:"fallback,omitempty" json:"fallback,omitempty"`
	SourceTimeout   Duration         `yaml:"source_timeout,omitempty" json:"source_timeout,omitempty"`
	FailbackDelay   Duration         `yaml:"failback_delay,omitempty" json:"failback_delay,omitempty"`
	BufferDuration  Duration         `yaml:"buffer_duration,omitempty" json:"buffer_duration,omitempty"`
	ICYMetaInterval int              `yaml:"icy_meta_interval,omitempty" json:"icy_meta_interval,omitempty"`
	CORSOrigins     []string         `yaml:"cors_origins,omitempty" json:"cors_origins,omitempty"`
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
}

type Fallback struct {
	Mount string `yaml:"mount,omitempty" json:"mount,omitempty"`
	File  string `yaml:"file,omitempty" json:"file,omitempty"`
	Title string `yaml:"title,omitempty" json:"title,omitempty"`
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
			SourceTimeout:    Duration(3 * time.Second),
			FailbackDelay:    Duration(5 * time.Second),
			BufferDuration:   Duration(2 * time.Second),
			WriteInterval:    Duration(100 * time.Millisecond),
			ICYMetaInterval:  16000,
			MaxSourceBitrate: 512000,
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
	if c.Defaults.SourceTimeout == 0 {
		c.Defaults.SourceTimeout = Duration(3 * time.Second)
	}
	if c.Defaults.FailbackDelay == 0 {
		c.Defaults.FailbackDelay = Duration(5 * time.Second)
	}
	if c.Defaults.BufferDuration == 0 {
		c.Defaults.BufferDuration = Duration(2 * time.Second)
	}
	if c.Defaults.WriteInterval == 0 {
		c.Defaults.WriteInterval = Duration(100 * time.Millisecond)
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
	if m.Source.Username == "" {
		m.Source.Username = "source"
	}
	credentials := 0
	for _, value := range []string{m.Source.PasswordBcrypt, m.Source.PasswordEnv, m.Source.PasswordFile} {
		if value != "" {
			credentials++
		}
	}
	if credentials != 1 {
		return errors.New("source requires exactly one of password_bcrypt, password_env, or password_file")
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
		if (fb.Mount == "") == (fb.File == "") {
			return errors.New("fallback requires exactly one of mount or file")
		}
		if fb.Mount != "" && !strings.HasPrefix(fb.Mount, "/") {
			return fmt.Errorf("invalid fallback mount %q", fb.Mount)
		}
		if fb.File != "" {
			fb.File = absolute(baseDir, fb.File)
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
