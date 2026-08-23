package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Master290/kite/internal/config"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

type certificateProvider struct {
	mu       sync.Mutex
	mode     string
	cert     *tls.Certificate
	certMod  time.Time
	certFile string
	keyFile  string
	manager  *autocert.Manager
	metrics  *Metrics
}

func newCertificateProvider(cfg config.TLS, metrics *Metrics) (*certificateProvider, error) {
	p := &certificateProvider{mode: cfg.Mode, certFile: cfg.CertificateFile, keyFile: cfg.PrivateKeyFile, metrics: metrics}
	switch cfg.Mode {
	case "development":
		cert, err := developmentCertificate(cfg.Hosts)
		if err != nil {
			return nil, err
		}
		p.cert = cert
		p.recordExpiry(cert)
	case "files":
		if _, err := p.loadFiles(); err != nil {
			return nil, err
		}
	case "acme":
		client := &acme.Client{}
		if cfg.ACMEDirectoryURL != "" {
			client.DirectoryURL = cfg.ACMEDirectoryURL
		}
		p.manager = &autocert.Manager{Prompt: autocert.AcceptTOS, Email: cfg.Email, Cache: autocert.DirCache(cfg.CacheDirectory), HostPolicy: autocert.HostWhitelist(cfg.Hosts...), Client: client}
	default:
		return nil, fmt.Errorf("unsupported TLS mode %q", cfg.Mode)
	}
	return p, nil
}

func (p *certificateProvider) TLSConfig() *tls.Config {
	c := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"h3", "h2", "http/1.1"}}
	if p.manager != nil {
		base := p.manager.TLSConfig()
		base.MinVersion = tls.VersionTLS12
		base.NextProtos = c.NextProtos
		return base
	}
	c.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		if p.mode == "files" {
			return p.loadFiles()
		}
		return p.cert, nil
	}
	return c
}

func (p *certificateProvider) loadFiles() (*tls.Certificate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	info, err := os.Stat(p.certFile)
	if err != nil {
		return nil, err
	}
	if p.cert != nil && !info.ModTime().After(p.certMod) {
		return p.cert, nil
	}
	cert, err := tls.LoadX509KeyPair(p.certFile, p.keyFile)
	if err != nil {
		return nil, err
	}
	p.cert, p.certMod = &cert, info.ModTime()
	p.recordExpiry(&cert)
	return &cert, nil
}

func (p *certificateProvider) recordExpiry(cert *tls.Certificate) {
	if len(cert.Certificate) == 0 {
		return
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err == nil {
		p.metrics.SetCertificateExpiry(leaf.NotAfter.Unix())
	}
}

func developmentCertificate(hosts []string) (*tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, err
	}
	tmpl := x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Kite development"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(30 * 24 * time.Hour), KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	if len(hosts) == 0 {
		hosts = []string{"localhost", "127.0.0.1", "::1"}
	}
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, host)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &cert, nil
}
