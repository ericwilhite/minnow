/** Pre-evaluation window continuation: native rows plus immutable ordinal position.
 * No frame transition has begun at this ancestor-only input boundary. */
import type { RecordCheckpoint } from "./record-checkpoint.js";
export const WINDOW_ORDINAL_BYTES = 2048 * 8;
export class WindowCheckpoint {
  /** No additional private helper: restore reads use admitted ordinary workspace. */
  readonly restoreLoanSlots = 0;
  constructor(
    readonly records: RecordCheckpoint,
    readonly nextOrdinal: bigint,
    readonly priorSpilledBytes: number,
    /** False for a lazy snapshot: native runs retain stable order within peers. */
    readonly hasOrdinals = true,
  ) {}
  /** Additional ordinary reader/copy workspace beyond the normal window recipe. */
  get minimumBytes(): number {
    return 6 * this.records.maxPageBytes + 64 * 1024;
  }
  readonly progressBytes = 0;
  get spilledBytes(): number {
    return this.priorSpilledBytes + this.records.files.written;
  }
  close(): Promise<void> {
    return this.records.close();
  }
}
