/**
 * Checks for what a live subscription publishes, delivery by delivery.
 *
 * The live oracles compare a subscription's latest result with a fresh execution of its SQL. That
 * judges where a subscriber ends up, not the deliveries that took it there, and it never sees the
 * patch stream a worker-hosted subscriber rebuilds its rows from. TanStack DB's 1.0 hardening
 * found its bugs in exactly that gap: a subscriber that applies every message must end with the
 * rows a direct read returns, and a message that is wrong can be repaired by a later one before
 * anything compares the final state. Two recorders close it.
 *
 * `LivePublicationLedger` takes every `onChange` and holds each delivery to the contract on
 * `LiveQueryDelivery`:
 *
 *   - the first delivery is `initial` and carries no `retained`; no later one is `initial`
 *   - `retained` has one entry per row; each entry is -1 or the index that row held in this
 *     subscriber's previous delivery, no index is claimed twice, and a retained row has the
 *     same values it had there
 *   - `manifestVersion` (null ordered first, as the engine orders it) never goes backwards,
 *     which holds for any history that does not replace the database wholesale
 *
 * `LivePatchReplica` takes every `onPatch` and rebuilds the rows the way a consumer must: a reset
 * replaces them, and a patch takes each row from its retained position or from `changedRows`.
 * It refuses a patch before any reset, a retained position outside the rows it holds, a position
 * claimed twice, and `changedRows` that are out of order or do not fill exactly the -1 positions.
 *
 * Neither throws from inside a callback, where the engine would route the error to `onError`
 * and stop counting the delivery as received. Both record violations, and the caller checks them
 * at its checkpoint along with the three-way agreement: the ledger's latest rows, the replica's
 * rows, and a fresh query. Like ./seeds.ts, this module is excluded from the published tarball.
 */
import type { LiveQueryDelivery } from "../engine/live.js";
import type { LiveQueryPatch } from "../engine/live-patch.js";
import type { QueryResult, QueryRow, QueryValue } from "../plan/model.js";

/** What a delivery did, for a campaign's declared reach. */
export type LivePublicationState =
  /** Delivered with `retained`: the engine patched the previous result. */
  | "patched"
  /** A retained row moved to a different index. */
  | "moved"
  /** A patched delivery replaced or added at least one row. */
  | "replaced"
  /** A patched delivery has fewer rows than the one before it. */
  | "shrunk"
  /** A later delivery without `retained`: the statement ran again. */
  | "rerun"
  /** A delivery that names rekeyed rows. */
  | "keyChanges";

function sameValue(left: QueryValue, right: QueryValue): boolean {
  if (left instanceof Date || right instanceof Date) {
    return left instanceof Date && right instanceof Date && left.getTime() === right.getTime();
  }
  return Object.is(left, right);
}

function sameRow(left: QueryRow, right: QueryRow): boolean {
  const keys = Object.keys(left);
  if (keys.length !== Object.keys(right).length) return false;
  return keys.every(
    (key) =>
      Object.prototype.hasOwnProperty.call(right, key) &&
      sameValue(left[key] ?? null, right[key] ?? null),
  );
}

function versionOrdinal(version: number | null): number {
  return version ?? -1;
}

/** Holds each `onChange` delivery to the `LiveQueryDelivery` contract. */
export class LivePublicationLedger {
  readonly violations: string[] = [];
  readonly #label: string;
  readonly #observe: ((state: LivePublicationState) => void) | undefined;
  #previous: readonly QueryRow[] | undefined;
  #latest: QueryResult | undefined;
  #version = Number.NEGATIVE_INFINITY;
  #deliveries = 0;

  constructor(label: string, observe?: (state: LivePublicationState) => void) {
    this.#label = label;
    this.#observe = observe;
  }

  get deliveries(): number {
    return this.#deliveries;
  }

  /** The latest delivered result. */
  latest(): QueryResult | undefined {
    return this.#latest;
  }

  /** Pass as `onChange`. */
  readonly change = (result: QueryResult, delivery: LiveQueryDelivery): void => {
    const at = `${this.#label} delivery ${String(this.#deliveries)}`;
    const first = this.#deliveries === 0;
    this.#deliveries += 1;
    if (delivery.initial !== first) {
      this.violations.push(`${at}: initial is ${String(delivery.initial)}`);
    }
    const version = versionOrdinal(delivery.manifestVersion);
    if (version < this.#version) {
      this.violations.push(
        `${at}: manifestVersion went back from ${String(this.#version)} to ${String(version)}`,
      );
    }
    this.#version = Math.max(this.#version, version);
    if (delivery.keyChanges !== undefined && delivery.keyChanges.length > 0) {
      this.#observe?.("keyChanges");
    }
    const rows = result.rows;
    const retained = delivery.retained;
    if (retained === undefined) {
      if (!first) this.#observe?.("rerun");
    } else if (first || this.#previous === undefined) {
      this.violations.push(`${at}: the first delivery carries retained positions`);
    } else {
      this.#checkRetained(at, this.#previous, rows, retained);
    }
    // A private result is the subscriber's own, but its rows are kept by reference only for the
    // next comparison; a copy of the array is enough because rows are never edited in place.
    this.#previous = rows.slice();
    this.#latest = { ...result, rows: rows.slice() };
  };

  #checkRetained(
    at: string,
    previous: readonly QueryRow[],
    rows: readonly QueryRow[],
    retained: Int32Array,
  ): void {
    this.#observe?.("patched");
    if (retained.length !== rows.length) {
      this.violations.push(
        `${at}: retained has ${String(retained.length)} entries for ${String(rows.length)} rows`,
      );
      return;
    }
    if (rows.length < previous.length) this.#observe?.("shrunk");
    const claimed = new Set<number>();
    for (let index = 0; index < retained.length; index += 1) {
      const was = retained[index] ?? -1;
      if (was < 0) {
        if (was !== -1)
          this.violations.push(`${at}: row ${String(index)} retained as ${String(was)}`);
        this.#observe?.("replaced");
        continue;
      }
      if (was >= previous.length) {
        this.violations.push(
          `${at}: row ${String(index)} retained from ${String(was)}, past the previous ${String(previous.length)} rows`,
        );
        continue;
      }
      if (claimed.has(was)) {
        this.violations.push(`${at}: previous row ${String(was)} retained twice`);
        continue;
      }
      claimed.add(was);
      if (was !== index) this.#observe?.("moved");
      const before = previous[was];
      const after = rows[index];
      if (before === undefined || after === undefined || !sameRow(before, after)) {
        this.violations.push(
          `${at}: row ${String(index)} retained from ${String(was)} but changed: ` +
            `${JSON.stringify(before)} -> ${JSON.stringify(after)}`,
        );
      }
    }
  }
}

/** Rebuilds a subscription's rows from its patch stream, refusing an ill-formed patch. */
export class LivePatchReplica {
  readonly violations: string[] = [];
  readonly #label: string;
  #rows: QueryRow[] | undefined;
  #patches = 0;

  constructor(label: string) {
    this.#label = label;
  }

  /** The rows rebuilt so far, or undefined before the first reset. */
  rows(): readonly QueryRow[] | undefined {
    return this.#rows;
  }

  /** Pass as `onPatch`. */
  readonly apply = (patch: LiveQueryPatch): void => {
    const at = `${this.#label} patch ${String(this.#patches)}`;
    this.#patches += 1;
    if (patch.type === "reset") {
      this.#rows = patch.result.rows.slice();
      return;
    }
    const previous = this.#rows;
    if (previous === undefined) {
      this.violations.push(`${at}: a patch arrived before any reset`);
      return;
    }
    const { retained, changedRows } = patch;
    const changed = new Map<number, QueryRow>();
    let last = -1;
    for (const { index, row } of changedRows) {
      if (index <= last) {
        this.violations.push(`${at}: changed row ${String(index)} is out of order or repeated`);
        return;
      }
      last = index;
      if (index >= retained.length || (retained[index] ?? -1) >= 0) {
        this.violations.push(`${at}: changed row ${String(index)} is not a -1 position`);
        return;
      }
      changed.set(index, row);
    }
    const claimed = new Set<number>();
    const next: QueryRow[] = [];
    for (let index = 0; index < retained.length; index += 1) {
      const was = retained[index] ?? -1;
      if (was < 0) {
        const row = changed.get(index);
        if (row === undefined) {
          this.violations.push(`${at}: position ${String(index)} has no changed row`);
          return;
        }
        next.push(row);
        continue;
      }
      const row = previous[was];
      if (row === undefined || claimed.has(was)) {
        this.violations.push(
          `${at}: position ${String(index)} retains ${String(was)} of ${String(previous.length)} rows` +
            (claimed.has(was) ? " a second time" : ""),
        );
        return;
      }
      claimed.add(was);
      next.push(row);
    }
    this.#rows = next;
  };
}
