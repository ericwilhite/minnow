/** Page-safe byte sizing and admission diagnostics; no governor state or scheduling. */
export const OBJECT_OVERHEAD_BYTES = 64;
/** Bytes a BigInt occupies in V8 (header plus 64-bit digits). */
export function bigintBytes(value: bigint): number {
  const magnitude = value < 0n ? -value : value;
  return 16 + 8 * Math.max(1, Math.ceil(magnitude.toString(16).length / 16));
}

/** Bytes of a JS string kept on the collation fallback (two bytes per code unit plus header). */
export function stringBytes(value: string): number {
  return 24 + 2 * value.length;
}

/** A non-spillable input or a fixed minimum cannot fit the owner's normal capacity. */
export class MemoryAdmissionError extends RangeError {
  readonly code = "MINNOW_MEMORY_ADMISSION";
  /** PostgreSQL's out_of_memory resource error (Appendix A). */
  readonly sqlState = "53200";
  constructor(
    readonly label: string,
    readonly bytes: number,
    readonly capacity: number,
  ) {
    super(
      `${label} needs a minimum above the cap: plan smaller batches (${String(bytes)} bytes, ${String(capacity)} available)`,
      { cause: Object.freeze({ requestedBytes: bytes, admittedBytes: capacity }) },
    );
    this.name = "MemoryAdmissionError";
  }
}
