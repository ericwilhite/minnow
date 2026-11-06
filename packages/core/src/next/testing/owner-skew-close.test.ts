/** P3: close owns and cancels skew discovery's timer and channel, not only its outer race. */
import { expect, it, vi } from "vitest";
import { ProtocolClient } from "../owner/client.js";
import { OwnerConnector } from "../owner/connector.js";
import { FakeLockRegistry } from "./fake-locks.js";

it("close during skew discovery leaves zero timers and zero BroadcastChannels", async () => {
  const NativeChannel = globalThis.BroadcastChannel;
  const live = new Set<BroadcastChannel>();
  let entered!: () => void;
  const discovering = new Promise<void>((resolve) => {
    entered = resolve;
  });
  class TrackedChannel extends NativeChannel {
    constructor(name: string) {
      super(name);
      live.add(this);
      if (live.size === 2) entered(); // announcement listener plus active skew discovery
    }
    override close() {
      live.delete(this);
      super.close();
    }
  }
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  vi.stubGlobal("BroadcastChannel", TrackedChannel);
  const ports: MessagePort[] = [];
  const connector = new OwnerConnector({
    database: "skew-close",
    client: new ProtocolClient(),
    locks: new FakeLockRegistry().context(),
    startOwner: null,
    context: () => ({
      kind: "page",
      visible: true,
      superseded: false,
      platform: { mobile: false },
    }),
    openBroker() {
      const { port1, port2 } = new MessageChannel();
      ports.push(port1, port2);
      port1.onmessage = (event) => {
        const message = event.data as { t: string; nonce: number };
        if (message.t === "connect") port1.postMessage({ t: "unreachable", nonce: message.nonce });
      };
      return port2;
    },
  });
  const start = connector.start().catch(() => undefined);
  try {
    await discovering;
    expect(live.size).toBe(2);
    connector.close();
    // Check synchronously: waiting out discovery must not be what cleans up close.
    expect({ timers: vi.getTimerCount(), channels: live.size }).toEqual({ timers: 0, channels: 0 });
    await start;
  } finally {
    connector.close();
    await vi.runOnlyPendingTimersAsync();
    await start;
    for (const port of ports) port.close();
    for (const channel of live) channel.close();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  }
});
