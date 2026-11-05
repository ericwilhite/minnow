/** The port broker SharedWorker of the next core's owner (owner/broker.ts). */
import { serveBroker } from "../../dist/next/owner/broker.js";

serveBroker(
  self as unknown as { onconnect: ((event: MessageEvent) => void) | null },
  navigator.locks,
);
