/** Observe real native admissions; cancel the first pending request without
 * replacing any budget, grant, allocator, spill decision, or abort signal.
 */
import type { MemoryGovernor } from "../memory/governor.js";

export function cancelPendingAdmission(governor: MemoryGovernor, stop: AbortController) {
  const admit = governor.admit.bind(governor),
    group = governor.admitGroup.bind(governor),
    baseline = governor.usedBytes,
    reason = new Error("Cancel pending complete-query admission"),
    observed = { waiting: false, heldAboveBaseline: 0, requested: 0 };
  const watch = <T>(run: () => Promise<T>, requested: number): Promise<T> => {
    const pending = run();
    let settled = false;
    void pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    setTimeout(() => {
      if (!settled && !observed.waiting) {
        observed.waiting = true;
        observed.heldAboveBaseline = governor.usedBytes - baseline;
        observed.requested = requested;
        stop.abort(reason);
      }
    }, 0);
    return pending;
  };
  governor.admit = (options) => watch(() => admit(options), options.minimumBytes);
  governor.admitGroup = (options) =>
    watch(
      () => group(options),
      options.grants.reduce((n, grant) => n + grant.minimumBytes, 0),
    );
  return {
    observed,
    reason,
    restore() {
      governor.admit = admit;
      governor.admitGroup = group;
    },
  };
}
