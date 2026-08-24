# Troubleshooting

## BUTT cannot connect

Check the mount path, username, password, profile, and port. For local plaintext testing, confirm that `server.http_address: ":8000"` is enabled and TLS is disabled in BUTT. For production, use port `443`, TLS enabled, and the exact ACME hostname.

Inspect logs:

```bash
docker compose -f compose.production.yaml logs --tail=100 kite
```

## Certificate issuance fails

Verify DNS, inbound TCP port 80, and the configured `tls.http_challenge_address`. Ensure no other service owns the challenge port. Keep the ACME cache on the persistent `kite-data` volume.

## Stream returns 404

The mount must be configured exactly, including its leading slash. The default example mount is `/radio`; administrative paths are reserved.

## Listeners disconnect

Kite closes listeners that fall behind the bounded live buffer. Check client bandwidth, `buffer_duration`, and `metadata.bitrate`. A disconnected source may also trigger a configured fallback.

## Metadata is missing

Use `Icy-MetaData: 1` for ICY interleaving, or subscribe to `/_kite/v1/events` for SSE. Confirm the source client is sending updates or call `/admin/metadata` with source credentials.

