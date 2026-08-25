# HTTP API

## Public stream

`GET /<mount>` returns the live stream. The response includes `icy-*` headers and `Content-Type` for the configured profile. `HEAD` returns headers without creating a listener.

| Endpoint | Purpose |
| --- | --- |
| `GET /radio` | Audio stream |
| `GET /radio` with `Icy-MetaData: 1` | Audio with ICY metadata blocks |
| `GET /_kite/v1/events?mount=/radio` | SSE metadata/source events |
| `GET /_kite/v1/ws?mount=/radio` | WebSocket audio and JSON events |
| `GET /_kite/v1/playlist.m3u?mount=/radio` | Absolute M3U playlist |
| `GET /status-json.xsl` | Mount status JSON |
| `GET /demo` | Built-in player page (disable with `server.demo_enabled: false`) |
| `GET /healthz` | Process health on the admin listener |
| `GET /readyz` | Listener readiness on the admin listener |

## Source ingest

Source clients authenticate with HTTP Basic Auth using the configured mount username and password. Kite accepts `PUT /radio`, `POST /radio`, and legacy `SOURCE /radio ICE/1.0` over HTTP/1.1. The media profile must match the mount configuration.

## Admin API

Admin endpoints require `Authorization: Bearer <KITE_ADMIN_TOKEN>`.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/config` | Redacted active config and ETag |
| `PUT` | `/api/v1/config` | Validate, persist, and activate config |
| `POST` | `/api/v1/config/validate` | Validate a proposed config |
| `POST` | `/api/v1/reload` | Reload config from disk |
| `GET` | `/api/v1/mounts` | Runtime mount status |
| `DELETE` | `/api/v1/source?mount=/radio` | Disconnect active source |
| `GET` | `/metrics` | Prometheus exposition |

Use `If-Match` with the config `ETag` to avoid overwriting a newer revision. Listener addresses, admin bind settings, and TLS mode require a restart.

## Runtime profiling

Set the environment variable `KITE_PPROF=1` and restart to expose Go pprof handlers on the admin listener under `/debug/pprof/`. The handlers require the admin bearer token like every other control-plane endpoint, and the admin listener binds to loopback by default.

