/** BENCH-only bounded request trace. Diagnostic captures are not CPU/RSS acceptance runs.
 * Events are flat quadruples: take [0,size,actualHit,0], issue [1,id,size,frame],
 * return [2,id,size,through], explicit governor eviction [3,requested,freed,0].
 * Only the first 200 issued frames of the first pass are retained. Size counters are per pass.
 */
import type { ReturnPool } from "../../../packages/core/src/next/exec/return-pool.js";
export interface PoolTraceSummary {
  readonly requests: number;
  readonly hits: number;
  readonly requestedBytes: number;
  readonly hitBytes: number;
  readonly zeroRequests: number;
  readonly missesPendingReturn: number;
  readonly missesSeenSize: number;
  readonly missesNewSize: number;
  readonly explicitEvictions: number;
  readonly classes: ReadonlyArray<{
    size: number;
    requests: number;
    hits: number;
    missesPendingReturn: number;
    missesSeenSize: number;
    missesNewSize: number;
  }>;
  readonly events: readonly number[];
}
export function traceReturnPool(pool: ReturnPool) {
  const take = pool.take.bind(pool),
    issue = pool.issue.bind(pool),
    release = pool.release.bind(pool),
    evict = pool.evict.bind(pool);
  const pending = new Map<number, number>(),
    sizes = new Map<number, number>(),
    seen = new Set<number>();
  let pass = 0,
    lastFrame = -1,
    events: number[] = [],
    classes = new Map<
      number,
      {
        size: number;
        requests: number;
        hits: number;
        missesPendingReturn: number;
        missesSeenSize: number;
        missesNewSize: number;
      }
    >();
  let requests = 0,
    hits = 0,
    requestedBytes = 0,
    hitBytes = 0,
    zeroRequests = 0,
    missesPendingReturn = 0,
    missesSeenSize = 0,
    missesNewSize = 0,
    explicitEvictions = 0;
  const record = (a: number, b: number, c: number, d: number) => {
    if (pass === 0 && lastFrame < 200) events.push(a, b, c, d);
  };
  pool.take = (size) => {
    const buffer = take(size);
    record(0, size, buffer === null ? 0 : 1, 0);
    if (size <= 0) {
      zeroRequests++;
      return buffer;
    }
    requests++;
    requestedBytes += size;
    let row = classes.get(size);
    if (!row) {
      row = {
        size,
        requests: 0,
        hits: 0,
        missesPendingReturn: 0,
        missesSeenSize: 0,
        missesNewSize: 0,
      };
      classes.set(size, row);
    }
    row.requests++;
    if (buffer) {
      hits++;
      hitBytes += size;
      row.hits++;
    } else if ((pending.get(size) ?? 0) > 0) {
      missesPendingReturn++;
      row.missesPendingReturn++;
    } else if (seen.has(size)) {
      missesSeenSize++;
      row.missesSeenSize++;
    } else {
      missesNewSize++;
      row.missesNewSize++;
    }
    return buffer;
  };
  pool.issue = (frame, buffers) => {
    const ids = issue(frame, buffers);
    lastFrame = frame;
    buffers.forEach((buffer, i) => {
      const id = ids[i] ?? -1;
      record(1, id, buffer.byteLength, frame);
      if (id >= 0) {
        sizes.set(id, buffer.byteLength);
        pending.set(buffer.byteLength, (pending.get(buffer.byteLength) ?? 0) + 1);
      }
    });
    return ids;
  };
  pool.release = (through, buffers) => {
    for (const item of buffers) {
      record(2, item.id, item.buffer.byteLength, through);
      const size = sizes.get(item.id);
      if (size !== undefined) {
        sizes.delete(item.id);
        pending.set(size, Math.max(0, (pending.get(size) ?? 0) - 1));
        seen.add(size);
      }
    }
    release(through, buffers);
  };
  pool.evict = (bytes) => {
    const freed = evict(bytes);
    explicitEvictions++;
    record(3, bytes, freed, 0);
    return freed;
  };
  return {
    begin(nextPass: number) {
      pass = nextPass;
      lastFrame = -1;
      events = [];
      classes = new Map();
      requests =
        hits =
        requestedBytes =
        hitBytes =
        zeroRequests =
        missesPendingReturn =
        missesSeenSize =
        missesNewSize =
        explicitEvictions =
          0;
    },
    summary(): PoolTraceSummary {
      return {
        requests,
        hits,
        requestedBytes,
        hitBytes,
        zeroRequests,
        missesPendingReturn,
        missesSeenSize,
        missesNewSize,
        explicitEvictions,
        classes: [...classes.values()].sort((a, b) => b.requests - a.requests),
        events,
      };
    },
  };
}
