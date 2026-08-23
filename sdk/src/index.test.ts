import { describe, expect, it, vi } from "vitest";
import { connectKiteSocket } from "./index";

describe("connectKiteSocket", () => {
  it("uses a secure websocket URL for HTTPS", () => {
    const ctor = vi.fn(function (this: { binaryType: string }, _url: URL) { this.binaryType = ""; });
    vi.stubGlobal("WebSocket", ctor);
    connectKiteSocket("https://radio.example", "/live", { onAudio() {} });
    expect(String(ctor.mock.calls[0][0])).toBe("wss://radio.example/_kite/v1/ws?mount=%2Flive");
  });
});

