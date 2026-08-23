export interface KiteMetadata {
  title: string;
  url?: string;
}

export interface KiteEvent<T = Record<string, unknown>> {
  id: number;
  type: "metadata" | "source" | string;
  mount: string;
  time: string;
  payload?: T;
}

export interface KitePlayerOptions {
  baseURL: string;
  mount: string;
  audio: HTMLAudioElement;
  reconnectDelayMs?: number;
}

export class KitePlayer extends EventTarget {
  readonly audio: HTMLAudioElement;
  readonly baseURL: URL;
  readonly mount: string;
  private events?: EventSource;
  private stopped = true;
  private reconnectDelayMs: number;

  constructor(options: KitePlayerOptions) {
    super();
    this.audio = options.audio;
    this.baseURL = new URL(options.baseURL, window.location.href);
    this.mount = options.mount.startsWith("/") ? options.mount : `/${options.mount}`;
    this.reconnectDelayMs = options.reconnectDelayMs ?? 1000;
  }

  async play(): Promise<void> {
    this.stopped = false;
    this.connectEvents();
    this.audio.src = new URL(this.mount, this.baseURL).toString();
    await this.audio.play();
  }

  stop(): void {
    this.stopped = true;
    this.events?.close();
    this.events = undefined;
    this.audio.pause();
    this.audio.removeAttribute("src");
    this.audio.load();
  }

  private connectEvents(): void {
    this.events?.close();
    const url = new URL("/_kite/v1/events", this.baseURL);
    url.searchParams.set("mount", this.mount);
    const source = new EventSource(url);
    this.events = source;
    for (const type of ["metadata", "source"]) {
      source.addEventListener(type, (event) => {
        const detail = JSON.parse((event as MessageEvent<string>).data) as KiteEvent;
        this.dispatchEvent(new CustomEvent(type, { detail }));
      });
    }
    source.onerror = () => {
      source.close();
      if (!this.stopped) window.setTimeout(() => this.connectEvents(), this.reconnectDelayMs);
    };
  }
}

export interface KiteSocketHandlers {
  onAudio(data: ArrayBuffer): void;
  onEvent?(event: KiteEvent): void;
  onClose?(event: CloseEvent): void;
}

export function connectKiteSocket(baseURL: string, mount: string, handlers: KiteSocketHandlers): WebSocket {
  const url = new URL("/_kite/v1/ws", baseURL);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("mount", mount.startsWith("/") ? mount : `/${mount}`);
  const socket = new WebSocket(url);
  socket.binaryType = "arraybuffer";
  socket.onmessage = (message) => {
    if (message.data instanceof ArrayBuffer) handlers.onAudio(message.data);
    else handlers.onEvent?.(JSON.parse(String(message.data)) as KiteEvent);
  };
  socket.onclose = (event) => handlers.onClose?.(event);
  return socket;
}

