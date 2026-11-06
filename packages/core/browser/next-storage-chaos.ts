/**
 * The storage kill campaign's workload and recovery (Gate A) live in
 * src/next/spill/chaos-workload.ts, so the Node power-loss sweep (chaos-crash.test.ts) runs the
 * same code the browser worker does.
 */
export * from "../src/next/spill/chaos-workload.js";
