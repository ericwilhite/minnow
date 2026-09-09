import type { LiveMaintainedExecution } from "./live.js";

// Internal two-phase publication. A candidate may share a contribution map with the previous
// state, but may edit it only after the live set has accepted its complete result and byte cost.
// Failed execution, comparison, or admission simply drops the candidate and its bounded journal.
const publications = new WeakMap<LiveMaintainedExecution, () => void>();

export function stageLiveExecution<T extends LiveMaintainedExecution>(
  execution: T,
  publish: () => void,
): T {
  publications.set(execution, publish);
  return execution;
}

export function acceptLiveExecution(execution: LiveMaintainedExecution): void {
  publications.get(execution)?.();
  publications.delete(execution);
}
