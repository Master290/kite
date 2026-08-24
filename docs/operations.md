# Operations

## Health and metrics

The admin listener should stay on loopback or a private network:

```bash
curl http://127.0.0.1:9090/healthz
curl http://127.0.0.1:9090/readyz
curl http://127.0.0.1:9090/metrics
```

Use the alert rules in the repository at [`deploy/prometheus/kite-alerts.yml`](https://github.com/Master290/kite/blob/main/deploy/prometheus/kite-alerts.yml). They cover source disconnects, fallback activation, certificate expiry, and missing metrics.

## Important metrics

| Metric | Meaning |
| --- | --- |
| `kite_source_connected` | Primary source state by mount |
| `kite_listeners` | Current listeners by mount and transport |
| `kite_source_bytes_total` | Bytes received from sources |
| `kite_listener_bytes_total` | Bytes written to listeners |
| `kite_fallback_switches_total` | Fallback transitions |
| `kite_tls_certificate_expiry_timestamp_seconds` | Active certificate expiry time |

## Backups

The ACME account and certificates live in the `kite-data` Docker volume. Use the repository helper:

```bash
./scripts/backup-kite-data.sh backups/kite-data-$(date -u +%Y%m%dT%H%M%SZ).tar.gz
```

Restore only after stopping Kite and confirming the prompt:

```bash
docker compose -f compose.production.yaml down
./scripts/restore-kite-data.sh backups/kite-data-20260824T120000Z.tar.gz
docker compose -f compose.production.yaml up -d
```

Treat backups as secrets. They contain ACME account and certificate material.

## Updates

```bash
docker compose -f compose.production.yaml build --pull
docker compose -f compose.production.yaml up -d
docker compose -f compose.production.yaml ps
```

Check `/readyz`, source connection, listener count, and certificate expiry after each update.

