/** Original wall-clock guarantees; run only on the isolated one-worker CI runner. */
import { registerEventLoopStallTests } from "./event-loop-stalls.test-support.js";
registerEventLoopStallTests("wall");
