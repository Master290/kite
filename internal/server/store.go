package server

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Master290/kite/internal/config"
)

type ConfigStore struct {
	mu       sync.Mutex
	current  atomic.Pointer[config.Config]
	path     string
	baseDir  string
	revision atomic.Uint64
	etag     atomic.Value
}

func NewConfigStore(path string, cfg *config.Config) *ConfigStore {
	s := &ConfigStore{path: path, baseDir: filepath.Dir(path)}
	s.current.Store(cfg)
	s.revision.Store(1)
	s.etag.Store(hashConfig(cfg))
	return s
}

func (s *ConfigStore) Current() *config.Config { return s.current.Load() }
func (s *ConfigStore) Revision() uint64        { return s.revision.Load() }
func (s *ConfigStore) ETag() string            { value, _ := s.etag.Load().(string); return value }

func (s *ConfigStore) Parse(body []byte) (*config.Config, error) {
	return config.Parse(body, s.baseDir)
}

func (s *ConfigStore) Commit(next *config.Config, apply func(*config.Config) error) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if next.StaticKey() != s.current.Load().StaticKey() {
		return s.revision.Load(), ErrRestartRequired
	}
	b, err := config.Marshal(next)
	if err != nil {
		return s.revision.Load(), err
	}
	if err := atomicWrite(s.path, b, 0o600); err != nil {
		return s.revision.Load(), err
	}
	if err := apply(next); err != nil {
		return s.revision.Load(), fmt.Errorf("apply config: %w", err)
	}
	s.current.Store(next)
	rev := s.revision.Add(1)
	s.etag.Store(hashConfig(next))
	return rev, nil
}

func hashConfig(cfg *config.Config) string {
	b, _ := config.Marshal(cfg)
	sum := sha256.Sum256(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".kite-config-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
