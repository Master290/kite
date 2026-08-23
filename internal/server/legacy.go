package server

import (
	"bufio"
	"bytes"
	"net"
	"strings"
	"sync"
)

type legacyListener struct{ net.Listener }

func (l legacyListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &legacyConn{Conn: c, reader: bufio.NewReaderSize(c, 64<<10)}, nil
}

type legacyConn struct {
	net.Conn
	reader *bufio.Reader
	once   sync.Once
	prefix *bytes.Reader
}

func (c *legacyConn) Read(p []byte) (int, error) {
	c.once.Do(func() {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			c.prefix = bytes.NewReader([]byte(line))
			return
		}
		trimmed := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if strings.HasPrefix(trimmed, "SOURCE ") && strings.HasSuffix(trimmed, " ICE/1.0") {
			trimmed = strings.TrimSuffix(trimmed, " ICE/1.0") + " HTTP/1.0"
			line = trimmed + "\r\n"
		}
		c.prefix = bytes.NewReader([]byte(line))
	})
	if c.prefix != nil && c.prefix.Len() > 0 {
		return c.prefix.Read(p)
	}
	return c.reader.Read(p)
}
