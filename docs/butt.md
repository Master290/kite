# BUTT

Kite accepts BUTT through the Icecast Source protocol, including the legacy `SOURCE /mount ICE/1.0` request line.

## Local development

With the default example configuration, use:

| BUTT field | Value |
| --- | --- |
| Server type | IceCast |
| Address | `127.0.0.1` |
| Port | `8000` |
| User | `source` |
| Password | `KITE_SOURCE_PASSWORD` |
| Mountpoint | `/radio` |
| SSL/TLS | Disabled |
| Codec | MP3 |

Enable `server.http_address: ":8000"` in `kite.yaml` for this plaintext test path. The HTTPS path also works when BUTT supports TLS certificate verification settings.

## Production

For the ACME Compose profile:

| BUTT field | Value |
| --- | --- |
| Server type | IceCast |
| Address | Your ACME hostname |
| Port | `443` |
| User | `source` |
| Password | `KITE_SOURCE_PASSWORD` |
| Mountpoint | `/radio` |
| SSL/TLS | Enabled |
| Codec | MP3 |

The mount profile must match the source codec. The example mount is `mp3`; do not send AAC or Ogg data to it.

## Metadata

```bash
curl -u source:"$KITE_SOURCE_PASSWORD" \
  "https://radio.example.com/admin/metadata?mount=/radio&mode=updinfo&song=Artist%20-%20Title"
```

Kite exposes ICY metadata to compatible listeners and metadata/source events through SSE and WebSocket.

