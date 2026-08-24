# Kite

Kite is a lightweight Icecast-compatible streaming server written in Go. It accepts live audio from existing Icecast source clients and serves the same stream over HTTP/1.1, HTTP/2, HTTP/3, WebSocket, and SSE without transcoding.

Read the documentation at https://master290.github.io/kite/.

The project is an early implementation. Its current focus is a dependable single-node data plane: bounded latency, bounded memory, hot source fallback, dynamic configuration, native TLS, and observability.

## Features

- Icecast source ingest through HTTP `PUT`, `POST`, and legacy `SOURCE ... ICE/1.0`.
- Passthrough MP3, AAC/ADTS, and Ogg/Opus with frame/page-aware fan-out.
- HTTP/1.1, HTTP/2, and HTTP/3 listener delivery with `Alt-Svc` discovery.
- ICY response headers and interleaved `StreamTitle` metadata.
- SSE metadata/source events with `Last-Event-ID` replay.
- Binary audio plus JSON control events over WebSocket.
- Primary → backup mount → looping local file fallback chains.
- Development TLS, hot-reloaded certificate files, or built-in ACME.
- Atomic YAML configuration updates with revision ETags.
- Prometheus metrics on a separate control-plane listener.
- Shared byte-bounded ring per mount; listeners keep cursors instead of private audio queues.
- TypeScript browser SDK and a small demo player.

Kite does not transcode audio. Browser playback depends on codec support in the browser. HLS/DASH, AutoDJ scheduling, listener authentication, clustering, and Shoutcast DSP ingest are not part of the current release.

## Quick start

Kite requires Go 1.26.6 or newer, or a container runtime.

```bash
cp kite.example.yaml kite.yaml
export KITE_SOURCE_PASSWORD=change-me
export KITE_ADMIN_TOKEN=change-admin-token
go run ./cmd/kite serve -config kite.yaml
```

The example uses a self-signed development certificate:

- stream: `https://localhost:8443/radio`
- source: `https://localhost:8443/radio`
- HTTP/3: UDP `8443`
- admin API and metrics: `http://127.0.0.1:9090`

Send an MP3 stream with FFmpeg:

```bash
ffmpeg -re -i input.wav -codec:a libmp3lame -b:a 128k -content_type audio/mpeg \
  -tls 1 -tls_verify 0 -f mp3 icecast://source:change-me@localhost:8443/radio
```

For the self-signed development certificate, use a source client option that disables certificate verification. For production, use ACME or configured certificate files.

Production deployment templates are provided in [`kite.production.example.yaml`](kite.production.example.yaml), [`.env.example`](.env.example), and [`compose.production.yaml`](compose.production.yaml). Copy the first two files to `kite.production.yaml` and `.env`, replace the example hostname, email, and secrets, then run `docker compose -f compose.production.yaml up -d --build`. DNS for the ACME hostname must already point to the host, and TCP port 80 plus TCP/UDP port 443 must be reachable from the Internet. The Compose file exposes the admin API only at `127.0.0.1:9090` on the host.

See the [production deployment guide](docs/deployment.md) for DNS, firewall, verification, BUTT, update, and backup instructions.

Prometheus alert rules and portable Docker volume backup/restore helpers are included under [`deploy/prometheus`](deploy/prometheus) and [`scripts`](scripts).

Listen with curl:

```bash
curl -k https://localhost:8443/radio --output radio.mp3
```

Update metadata:

```bash
curl -k -u source:change-me \
  "https://localhost:8443/admin/metadata?mount=/radio&mode=updinfo&song=Artist%20-%20Title"
```

## Configuration

See [kite.example.yaml](kite.example.yaml) and [configuration reference](docs/configuration.md). Validate a file before starting:

```bash
go run ./cmd/kite validate -config kite.yaml
```

Generate a bcrypt source credential:

```bash
go run ./cmd/kite hash-password 'a-long-password'
```

Plaintext port `8000` is disabled in the example. To support legacy encoders, set `server.http_address: ":8000"`; Basic Auth credentials are then sent without transport encryption unless a trusted TLS proxy is used.

## Web transports

The normal browser path is a native `<audio>` element pointed at the mount plus SSE for metadata. The SDK packages this behavior:

```ts
import { KitePlayer } from "@kite-stream/player";

const player = new KitePlayer({
  baseURL: "https://radio.example",
  mount: "/radio",
  audio: document.querySelector("audio")!,
});

player.addEventListener("metadata", (event) => console.log(event.detail));
await player.play();
```

`connectKiteSocket()` is available for custom binary consumers. WebSocket audio is raw frames/pages for the configured mount profile, not a transcoded media format.

## Operations

The control plane binds to loopback by default. Send `Authorization: Bearer $KITE_ADMIN_TOKEN` to `/api/v1/*`.

- `GET /api/v1/config` — redacted active configuration and ETag.
- `PUT /api/v1/config` — validate, atomically persist, and activate YAML/JSON-compatible YAML; use `If-Match`.
- `POST /api/v1/config/validate` — validate a proposed YAML document.
- `POST /api/v1/reload` — reload the file on disk.
- `GET /api/v1/mounts` — runtime source/listener state.
- `DELETE /api/v1/source?mount=/radio` — disconnect the active source.
- `GET /metrics` — Prometheus exposition.
- `GET /healthz` — process health.
- `GET /readyz` — listener readiness.

Changes to listener addresses, admin bind address, or TLS mode require a restart and return HTTP `409`. Mounts, credentials, metadata, fallbacks, timeouts, buffers, and CORS rules are applied live.

## Development

```bash
go test ./...
go vet ./...
go build ./cmd/kite
go run ./cmd/kitebench -url https://localhost:8443/radio -listeners 1000 -duration 30s -insecure

cd sdk
npm ci
npm test
npm run build

cd ..
python -m pip install -r requirements-docs.txt
python -m mkdocs serve
```

Linux CI also runs the race detector. See [architecture](docs/architecture.md) for the data flow and safety model.

## License

Apache License 2.0. See [LICENSE](LICENSE).
