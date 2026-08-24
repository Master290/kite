# Quick start

This path starts Kite with a self-signed development certificate and a local MP3 mount.

## Requirements

- Go 1.26.6 or newer, or Docker.
- An MP3 source client such as BUTT or FFmpeg.

## Run locally

```bash
cp kite.example.yaml kite.yaml
export KITE_SOURCE_PASSWORD=change-me
export KITE_ADMIN_TOKEN=change-admin-token
go run ./cmd/kite validate -config kite.yaml
go run ./cmd/kite serve -config kite.yaml
```

The example listens on HTTPS `8443`, HTTP/3 UDP `8443`, and the loopback admin listener `9090`.

## Send audio

```bash
ffmpeg -re -i input.wav -codec:a libmp3lame -b:a 128k \
  -content_type audio/mpeg -tls 1 -tls_verify 0 -f mp3 \
  icecast://source:change-me@localhost:8443/radio
```

For BUTT, use the [BUTT setup guide](butt.md).

## Listen and inspect

```bash
curl -k https://localhost:8443/radio --output radio.mp3
curl http://127.0.0.1:9090/healthz
curl http://127.0.0.1:9090/readyz
curl http://127.0.0.1:9090/metrics
```

The development certificate is intentionally self-signed. Use `-k` only for local testing. Use ACME or configured certificate files for public deployments.

