/** Deterministic native work and real task boundaries, safe beside parallel suites. */
import { registerEventLoopStallTests } from "./event-loop-stalls.test-support.js";
registerEventLoopStallTests("work");
