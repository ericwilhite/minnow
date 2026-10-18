/**
 * Vitest setup for `npm run test:packed`: forced packing before every test, so a test that
 * resets the packing hooks in its own teardown still runs the next one packed. See
 * scripts/lib/forced-packing.mts.
 */
import { beforeEach } from "vitest";
import { applyForcedPacking } from "./lib/forced-packing.mts";

beforeEach(applyForcedPacking);
