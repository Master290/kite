# Production deployment

The included Compose profile runs Kite as a non-root user, stores ACME state in a persistent volume, exposes the control plane only on host loopback, and serves HTTP/1.1, HTTP/2, and HTTP/3 on the standard public HTTPS port.

## Prerequisites

- A Linux host with Docker Engine and the Compose plugin.
- An A or AAAA record for the radio hostname pointing at that host.
- Inbound TCP port 80 and inbound TCP and UDP port 443 allowed by the host and provider firewalls.
- No other process bound to public ports 80 or 443.

## Configure

```bash
cp kite.production.example.yaml kite.production.yaml
cp .env.example .env
chmod 600 .env
```

Edit `kite.production.yaml` and replace `radio.example.com` and `admin@example.com`. Edit `.env` and generate independent long random values for `KITE_SOURCE_PASSWORD` and `KITE_ADMIN_TOKEN`. Do not commit either copied file.

Validate and start the service:

```bash
docker compose -f compose.production.yaml run --rm kite validate -config /etc/kite/kite.yaml
docker compose -f compose.production.yaml up -d --build
docker compose -f compose.production.yaml ps
docker compose -f compose.production.yaml logs -f kite
```

The first public TLS connection triggers certificate issuance. The ACME HTTP-01 challenge is served through public TCP port 80. Certificate state survives container replacement in the `kite-data` volume.

## Verify

```bash
curl https://radio.example.com/status-json.xsl
curl http://127.0.0.1:9090/healthz
curl http://127.0.0.1:9090/readyz
curl -H "Authorization: Bearer $KITE_ADMIN_TOKEN" \
  http://127.0.0.1:9090/api/v1/mounts
```

Prometheus can scrape `http://127.0.0.1:9090/metrics` locally. If Prometheus runs in another container, attach both services to a private Docker network rather than publishing the admin listener publicly.

Ready-to-load alert rules are in [`deploy/prometheus/kite-alerts.yml`](../deploy/prometheus/kite-alerts.yml). Change the `job="kite"` selector in `KiteNotReady` if your Prometheus scrape job uses another name. The source-disconnected alert is intentionally a warning because a configured fallback may keep listeners online.

## BUTT

Configure an IceCast server with:

- Address: `radio.example.com`
- Port: `443`
- User: `source`
- Password: the value of `KITE_SOURCE_PASSWORD`
- Mountpoint: `/radio`
- SSL/TLS: enabled
- Codec: MP3 at the bitrate declared for the mount

Listeners use `https://radio.example.com/radio`. UDP 443 enables HTTP/3 where the client supports it; TCP 443 remains available for every normal HTTP client.

## Updates and backup

```bash
docker compose -f compose.production.yaml build --pull
docker compose -f compose.production.yaml up -d
./scripts/backup-kite-data.sh backups/kite-data-$(date -u +%Y%m%dT%H%M%SZ).tar.gz
```

The backup contains ACME account and certificate material and must be protected like any other credential.

To restore, stop the service first and use the explicit confirmation prompt:

```bash
docker compose -f compose.production.yaml down
./scripts/restore-kite-data.sh backups/kite-data-20260824T120000Z.tar.gz
docker compose -f compose.production.yaml up -d
```

Set `KITE_DATA_VOLUME` when the Compose project name is not the default, for example `KITE_DATA_VOLUME=myproject_kite-data`.
