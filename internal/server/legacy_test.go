package server

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestLegacyConnRewritesICEProtocol(t *testing.T) {
	client, rawServer := net.Pipe()
	defer client.Close()
	defer rawServer.Close()
	conn := &legacyConn{Conn: rawServer, reader: bufio.NewReaderSize(rawServer, 1024)}
	go func() { _, _ = io.WriteString(client, "SOURCE /radio ICE/1.0\r\nAuthorization: Basic abc\r\n\r\n") }()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 128)
	n, err := conn.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(b[:n]), "SOURCE /radio HTTP/1.0\r\n") {
		t.Fatalf("rewritten request=%q", b[:n])
	}
}
