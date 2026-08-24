# Kite

Kite is a lightweight Icecast-compatible streaming server written in Go. It accepts live audio from existing source clients and serves the same stream over HTTP/1.1, HTTP/2, HTTP/3, WebSocket, and SSE without transcoding.

## Start here

- [Quick start](quick-start.md) to run Kite locally.
- [BUTT setup](butt.md) to connect a live MP3 source.
- [Production deployment](deployment.md) for ACME TLS and Docker Compose.
- [Configuration reference](configuration.md) for the YAML model.

## What Kite provides

- Icecast Source ingest through HTTP `PUT`, `POST`, and legacy `SOURCE ... ICE/1.0`.
- Passthrough MP3, AAC/ADTS, and Ogg/Opus streaming.
- Bounded per-mount buffering with slow-listener protection.
- Primary, backup-mount, and looping file fallback.
- ICY metadata, SSE events, WebSocket audio, and Prometheus metrics.
- Development TLS, file-based certificates, and built-in ACME.
- Atomic dynamic configuration with revision ETags.

Kite is a single-node streaming server. It does not currently provide transcoding, HLS/DASH, AutoDJ scheduling, clustering, listener authentication, or full Shoutcast DSP compatibility.

## Project links

- [GitHub repository](https://github.com/Master290/kite)
- [Releases](https://github.com/Master290/kite/releases)
- [Issue tracker](https://github.com/Master290/kite/issues)

