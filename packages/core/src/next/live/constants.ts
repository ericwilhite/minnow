/** One bounded delivery may remain unacknowledged for this long before re-subscription. */
export const LIVE_UNACKED_DROP_MS = 60_000;

/** Prepaid bounded diagnostic sizing/fallback, even when query admission has exhausted credit. */
export const LIVE_ERROR_SCRATCH_BYTES = 16 * 1024;
/** Match the runtime's 64 KiB frame target; larger diagnostics become a 53200 error. */
export const LIVE_ERROR_MAX_BYTES = 64 * 1024;
