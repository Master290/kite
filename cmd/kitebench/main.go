package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go/http3"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kitebench:", err)
		os.Exit(1)
	}
}

func run() error {
	url := flag.String("url", "https://localhost:8443/radio", "stream URL")
	listeners := flag.Int("listeners", 100, "concurrent listeners")
	duration := flag.Duration("duration", 30*time.Second, "test duration")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification")
	useHTTP3 := flag.Bool("http3", false, "use HTTP/3")
	flag.Parse()
	if *listeners < 1 {
		return fmt.Errorf("listeners must be positive")
	}
	tlsConfig := &tls.Config{InsecureSkipVerify: *insecure}
	var transport http.RoundTripper
	var closer io.Closer
	if *useHTTP3 {
		tr := &http3.Transport{TLSClientConfig: tlsConfig}
		transport = tr
		closer = tr
	} else {
		transport = &http.Transport{TLSClientConfig: tlsConfig, MaxIdleConns: *listeners, MaxConnsPerHost: *listeners, ForceAttemptHTTP2: true}
	}
	if closer != nil {
		defer closer.Close()
	}
	client := &http.Client{Transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	var connected, failed, bytesRead atomic.Uint64
	start := time.Now()
	var wg sync.WaitGroup
	ready := make(chan struct{})
	for i := 0; i < *listeners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, *url, nil)
			if err != nil {
				failed.Add(1)
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				if ctx.Err() == nil {
					failed.Add(1)
				}
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				failed.Add(1)
				return
			}
			connected.Add(1)
			buf := make([]byte, 32<<10)
			for {
				n, err := resp.Body.Read(buf)
				bytesRead.Add(uint64(n))
				if err != nil {
					if ctx.Err() == nil {
						failed.Add(1)
					}
					return
				}
			}
		}()
	}
	close(ready)
	wg.Wait()
	elapsed := time.Since(start)
	mbps := float64(bytesRead.Load()*8) / elapsed.Seconds() / 1_000_000
	failures := failed.Load()
	missing := uint64(*listeners) - connected.Load()
	if missing > failures {
		failures = missing
	}
	fmt.Printf("listeners=%d connected=%d failed=%d bytes=%d elapsed=%s throughput=%.2fMbps\n", *listeners, connected.Load(), failures, bytesRead.Load(), elapsed.Round(time.Millisecond), mbps)
	if failures > 0 {
		return fmt.Errorf("%d listener(s) failed", failures)
	}
	return nil
}
