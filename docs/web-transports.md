# Web transports

## Native audio

The simplest browser integration points an `<audio>` element at the mount:

```html
<audio controls src="https://radio.example.com/radio"></audio>
```

Browser codec support depends on the configured profile. Kite does not transcode audio.

## HLS

For maximum reach — iOS Safari, unstable mobile networks, CDN caching — point an HLS player at the live playlist:

```html
<script src="https://cdn.jsdelivr.net/npm/hls.js@1"></script>
<audio id="audio" controls></audio>
<script>
  const audio = document.querySelector("#audio");
  if (audio.canPlayType("application/vnd.apple.mpegurl")) {
    audio.src = "https://radio.example.com/radio.m3u8";
  } else if (window.Hls) {
    new Hls().loadSource("https://radio.example.com/radio.m3u8").attachMedia(audio);
  }
</script>
```

HLS is available for `mp3` and `aac-adts` mounts without transcoding. The window keeps six ~4-second MPEG-TS segments, so end-to-end latency is roughly 10–15 seconds versus under a second for the native `<audio>` path. Source switches appear as `#EXT-X-DISCONTINUITY`.

## SSE

SSE carries metadata and source state without carrying audio bytes:

```text
GET /_kite/v1/events?mount=/radio
```

Events include `source` and `metadata` types. `Last-Event-ID` enables replay from the recent event history.

## WebSocket

WebSocket sends a JSON `hello` message, binary media frames/pages, and JSON metadata/source events. Binary payloads are raw bytes for the configured mount profile, not a browser-transcoded format.

The TypeScript SDK in [`sdk/`](https://github.com/Master290/kite/tree/main/sdk) provides `KitePlayer` and `connectKiteSocket()` helpers.

## HTTP/3

When `server.http3_address` is configured alongside HTTPS, Kite advertises HTTP/3 with `Alt-Svc` and serves QUIC over UDP. Keep TCP and UDP open on the public HTTPS port; TCP remains the fallback for clients without HTTP/3.

