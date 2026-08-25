// Package web embeds static assets served by Kite itself.
package web

import _ "embed"

// DemoHTML is the built-in player page served at /demo.
//
//go:embed demo/index.html
var DemoHTML []byte
