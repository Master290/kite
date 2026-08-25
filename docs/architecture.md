# Architecture

Kite separates a public data plane from a loopback control plane.

```text
Icecast encoder
      │ PUT /mount
      ▼
media framer ──► mount runtime ──► bounded listener queues
                       │               ├─ HTTP/1.1 + ICY
backup mount/file ─────┘               ├─ HTTP/2
                                       ├─ HTTP/3
metadata ─────────────────────────────►├─ SSE
                                       └─ WebSocket

YAML ◄── atomic rename ◄── Admin API ──► immutable config snapshot
Prometheus ◄──────────────────────────── runtime observer
```

Each incoming media frame or Ogg page is allocated once in a byte-bounded ring owned by the mount. Listeners keep only a sequence cursor. When the cursor falls behind the oldest retained sequence, Kite closes that listener as slow instead of growing memory or latency.

Only one source may own a mount. The runtime monitors the timestamp of the last valid frame/page, chooses the first available fallback, and returns to the primary after its stability delay. Backup mounts use the same subscription mechanism as listeners, while local files are parsed and bitrate-paced.

Configuration is parsed with strict field checking and validated as a complete graph. Runtime changes prepare a new mount map, persist canonical YAML with `fsync` and rename, then replace the active snapshot. Listener and TLS bind changes are deliberately restart-only.

Labels in Prometheus metrics are limited to configured mount, transport, reason, and fallback target. Client IPs and arbitrary metadata never become labels.

The passthrough framers in `internal/stream/framer.go` parse untrusted encoder bytes, so they are covered by native Go fuzz targets (`internal/stream/fuzz_test.go`) asserting that no input can panic, hang, or emit more bytes than it consumed. CI runs each target for a bounded time on every push; failures are minimized into `testdata/fuzz` corpora.
