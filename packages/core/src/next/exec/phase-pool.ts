/** One shared idle account per governor, never a per-waiter reservation. */
import type { Grant, MemoryGovernor } from "../memory/governor.js";
const pools = new WeakMap<MemoryGovernor, { refs: number; ready: Promise<Grant> }>();
export async function acquirePhasePool(
  owner: MemoryGovernor,
  signal: AbortSignal,
): Promise<{ pool: Grant; release: () => void }> {
  signal.throwIfAborted();
  let entry = pools.get(owner);
  if (!entry) {
    entry = {
      refs: 0,
      ready: owner
        .admitGroup({
          label: "executor/phase-idle",
          grants: [{ label: "shared idle credit", minimumBytes: 0, spillable: false }],
        })
        .then(async (parts) => {
          const pool = parts[0];
          if (!pool) throw new Error("Missing native idle pool");
          try {
            await owner.admitGroup({
              label: "executor/register-idle",
              idleCredit: pool,
              grants: [],
            });
            return pool;
          } catch (error) {
            pool.close();
            throw error;
          }
        }),
    };
    pools.set(owner, entry);
  }
  const current = entry;
  current.refs++;
  let pool: Grant;
  try {
    pool = await current.ready;
  } catch (error) {
    if (--current.refs === 0) pools.delete(owner);
    throw error;
  }
  let released = false;
  const release = () => {
    if (released) return;
    released = true;
    if (--current.refs === 0) {
      pools.delete(owner);
      pool.close();
    }
  };
  try {
    signal.throwIfAborted();
    return { pool, release };
  } catch (error) {
    release();
    throw error;
  }
}
