/**
 * The two rekeying names live queries need, kept apart from `key-forwards.ts` so a standalone
 * live entry does not carry key encoding it never runs.
 */

/**
 * The internal key-forwards table's ID. It is fixed, so a query can name the table as a
 * dependency before the first rekey creates it, and two tabs racing to create it collide on the
 * ID instead of making two.
 */
export const KEY_FORWARDS_TABLE_ID = "minnow-key-forwards";

/** One rekeyed row, as a live delivery reports it. */
export interface KeyChange {
  readonly table: string;
  /** The retired key: a scalar, or one value per column of a composite key. */
  readonly from: KeyChangeValue | readonly KeyChangeValue[];
  readonly to: KeyChangeValue | readonly KeyChangeValue[];
}

/** One key column's value. */
export type KeyChangeValue = boolean | number | string | Date | null;
