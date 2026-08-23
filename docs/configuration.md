# Configuration reference

Kite uses strict, versioned YAML. Unknown fields and invalid fallback graphs prevent startup or reload.

## Server

`server.http_address` enables plaintext HTTP/1.1 and the legacy ICE request-line adapter. It is empty by default. `server.https_address` serves HTTP/1.1 and HTTP/2. `server.http3_address` serves QUIC on UDP, normally using the same port number. `public_https_port` controls the advertised `Alt-Svc` port.

Changing any listener address at runtime requires a restart.

## TLS

- `development`: creates an ephemeral self-signed certificate on each start.
- `files`: loads `certificate_file` and `private_key_file`; changes are picked up for new TLS handshakes.
- `acme`: obtains certificates for the explicit `hosts` allowlist using the configured email and cache directory. Set `http_challenge_address: ":80"` when HTTP-01 is required.

An alternative ACME directory can be configured with `acme_directory_url`, which is useful for Pebble testing.

## Source credentials

Each mount requires exactly one credential source:

```yaml
source:
  username: source
  password_bcrypt: "$2a$12$..."
```

or `password_env`, or `password_file`. Environment and file values may be plaintext or bcrypt hashes. Files and environment variables are resolved at authentication time, allowing secret rotation without a config reload.

## Profiles

- `mp3` → `audio/mpeg`
- `aac-adts` → `audio/aac`
- `ogg-opus` → `audio/ogg; codecs=opus`
- `opaque` → an explicitly configured `content_type`; fallback is disabled

Kite parses MP3 frames, ADTS frames, and Ogg pages before fan-out. Every mount in a fallback chain must use the same profile.

## Fallback

Fallback candidates are evaluated in order:

```yaml
fallback:
  - mount: /backup
  - file: ./emergency.mp3
    title: Emergency programming
```

Kite switches after `source_timeout` without valid input. It returns to a stable primary after `failback_delay`. Listener connections remain open. The switch occurs at a media frame/page boundary; Kite does not decode, crossfade, or normalize audio.

Kite validates every configured fallback file during startup and before a dynamic configuration commit. A missing file or a file without a valid first frame/page rejects the complete configuration without changing disk or runtime state.

File fallback is looped and paced using `metadata.bitrate`, or 128 kbit/s if it is omitted.

## Buffering

`buffer_duration` determines the bounded per-listener queue. A client that falls beyond the live window is disconnected instead of increasing latency and memory indefinitely. Tune `metadata.bitrate` accurately to make this bound representative.

## CORS

List allowed browser origins per mount. `"*"` is suitable only for public streams. WebSocket requests are rejected when their `Origin` is not allowed.
