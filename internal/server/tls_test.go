package server

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Master290/kite/internal/config"
)

func TestMergeProtocolsPreservesACMEALPN(t *testing.T) {
	got := mergeProtocols([]string{"h3", "h2", "http/1.1"}, []string{"acme-tls/1", "http/1.1"})
	want := []string{"h3", "h2", "http/1.1", "acme-tls/1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("protocols=%v, want %v", got, want)
	}
}

func TestFileProviderDetectsKeyReplacement(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	writeCertificatePair(t, certPath, keyPath)

	provider, err := newCertificateProvider(config.TLS{
		Mode:            "files",
		CertificateFile: certPath,
		PrivateKeyFile:  keyPath,
	}, NewMetrics())
	if err != nil {
		t.Fatal(err)
	}

	_, replacementKey := certificatePEM(t)
	if err := os.WriteFile(keyPath, replacementKey, 0o600); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(keyPath, future, future); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.loadFiles(); err == nil {
		t.Fatal("mismatched replacement key was not reloaded")
	}
}

func writeCertificatePair(t *testing.T, certPath, keyPath string) {
	t.Helper()
	cert, key := certificatePEM(t)
	if err := os.WriteFile(certPath, cert, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatal(err)
	}
}

func certificatePEM(t *testing.T) ([]byte, []byte) {
	t.Helper()
	pair, err := developmentCertificate([]string{"localhost"})
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pair.Certificate[0]})
	key := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return cert, key
}
