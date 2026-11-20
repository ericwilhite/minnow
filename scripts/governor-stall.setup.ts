/**
 * Vitest setup with MINNOW_GOVERNOR_STALL_MS set: a governor request that sees no progress for
 * that long fails with a GovernorStallError naming every holder, instead of hanging until the
 * test timeout (packages/core/src/next/memory/governor.ts, `Governor.stallAfterMs`).
 */
import { Governor } from "../packages/core/src/next/memory/governor.ts";

const afterMs = Number(process.env.MINNOW_GOVERNOR_STALL_MS);
if (!Number.isSafeInteger(afterMs) || afterMs <= 0)
  throw new Error("MINNOW_GOVERNOR_STALL_MS must be a positive integer of milliseconds");
Governor.stallAfterMs = afterMs;
