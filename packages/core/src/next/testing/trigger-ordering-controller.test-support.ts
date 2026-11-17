/** Synthetic controller-only qualification stays separate from native-provider execution. */
import { fixture } from "./trigger-controller.test-support.js";
import type { TriggerOrderingFactory } from "./trigger-ordering.test-support.js";
/** Real controller/queues/bindings/body protocol; injected scalar source and RI adapters.
 * Replace ONLY this factory with B5b's verified native SQL entry point when it lands. */
export const controllerOrderingFactory: TriggerOrderingFactory = async (h) => {
  const f = await fixture(h.event, false, h.twoBodies, "none", null, false, 2, h);
  return {
    async run() {
      for (let n = 1; n <= h.rows; n++) await f.row(n);
      await f.runtime.finishRoot();
      return [...f.trace];
    },
    close: () => f.close(),
  };
};
