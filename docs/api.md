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
| `GET /radio.m3u` | Icecast-compatible M3U playlist |
| `GET /radio.xspf` | Icecast-compatible XSPF playlist |
| `GET /`, `/index.html`, `/status.xsl` | Built-in HTML status and player page |
| `GET /status-json.xsl` | Icecast 2.4-compatible `icestats` JSON |
| `GET /radio.m3u8` | Live HLS playlist for MP3 and AAC mounts |
| `GET /radio.hls/<seq>.ts` | Immutable HLS MPEG-TS segment |
| `GET /healthz` | Process health on the admin listener |
| `GET /readyz` | Listener readiness on the admin listener |

## HLS

Mounts with the `mp3` or `aac-adts` profile are packaged into live HLS streams without transcoding: `/radio.m3u8` serves a sliding-window playlist (six ~4-second MPEG-TS segments) and each segment is served from `<mount>.hls/<sequence>.ts`. Segments are immutable and cacheable; playlists are not. Packaging starts on the first playlist request and stops after 30 seconds without listeners. Source switches and dropped data are marked with `#EXT-X-DISCONTINUITY`. Disable with `server.hls_enabled: false`.

## Source ingest

Source clients authenticate with HTTP Basic Auth using the configured mount username and password. Kite accepts `PUT /radio`, `POST /radio`, and legacy `SOURCE /radio ICE/1.0` over HTTP/1.1. The media profile must match the mount configuration. Connecting to a mount that already has an active direct source returns `403 Forbidden`.

Standard Icecast source headers (`ice-name`, `ice-genre`, `ice-description`, `ice-url`, `ice-bitrate`, `ice-public`, `ice-audio-info`, and `icy-*` equivalents) are read from the source request and used as stream metadata unless explicitly overridden in the mount configuration.

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

### Icecast-compatible XML Admin API

Kite also serves Icecast 2.4-compatible XML endpoints under `/admin/`, authenticated exclusively with HTTP Basic Auth using the separate Icecast admin credentials (`admin.icecast_admin_user`, default `admin`, and `admin.icecast_admin_password` / `admin.icecast_admin_password_env` / `admin.icecast_admin_password_file` / `KITE_ICECAST_ADMIN_PASSWORD`). The Bearer control-plane token (`KITE_ADMIN_TOKEN`) is never accepted on `/admin/*`, and Bearer authentication is never accepted on the public listener. Per-IP failed-authentication lockout applies to both admin and source endpoints (5 consecutive failures trigger exponential backoff up to 15 minutes).

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/admin/stats` | Global and per-mount XML statistics (`<icestats>`) |
| `GET` | `/admin/listmounts` | Connected/configured mounts (`<icestats><source mount="...">`) |
| `GET` | `/admin/listclients?mount=/radio` | Connected listeners for a mount (`<listener id="...">`) |
| `GET`, `POST` | `/admin/killclient?mount=/radio&id=<id>` | Disconnect a specific listener by ID |
| `GET`, `POST` | `/admin/killsource?mount=/radio` | Disconnect the active source on a mount (disconnects listeners if no fallback is configured) |
| `GET`, `POST` | `/admin/metadata?mount=/radio&mode=updinfo&song=...` | Update stream metadata (returns `<iceresponse>`; accepts mount source or Icecast admin Basic Auth) |

Use `If-Match` with the config `ETag` to avoid overwriting a newer revision. Listener addresses, admin bind settings, and TLS mode require a restart.

## Runtime profiling

Set the environment variable `KITE_PPROF=1` and restart to expose Go pprof handlers on the admin listener under `/debug/pprof/`. The handlers require the admin bearer token like every other control-plane endpoint, and the admin listener binds to loopback by default.

