/** These are injected I/O/completion/allocation failures, not SQL semantic oracles. */
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_ALLOCATION_FAULT_REACH } from "../../testing/reach.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { Pool } from "../exec/spill-pool.js";
import { expect, it, vi } from "vitest";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
// A chunked/prepaid grant is not one request per buffer. Inject at the actual
// constructor's admission/suballocation boundaries, retaining the original fault positions.
const constructorFault = vi.hoisted(() => ({ active: false }));
vi.mock("../exec/json-phase.js", async (loadOriginal) => {
  const original = await loadOriginal<typeof import("../exec/json-phase.js")>();
  return {
    ...original,
    JsonPhaseOperator: class extends original.JsonPhaseOperator {
      constructor(...args: ConstructorParameters<typeof original.JsonPhaseOperator>) {
        constructorFault.active = true;
        try {
          super(...args);
        } finally {
          constructorFault.active = false;
        }
      }
    },
  };
});
function hasCause(error: unknown, cause: unknown): boolean {
  if (error === cause) return true;
  if (error instanceof AggregateError && error.errors.some((e) => hasCause(e, cause))) return true;
  return error instanceof Error && error.cause !== undefined && hasCause(error.cause, cause);
}
const sql = `SELECT ('{"n":1e1000,"z":2.00}'::jsonb -> $1::text)::text AS n`;
it.each(["append", "read", "close"] as const)(
  "settles JSON phase files and credit after %s failure",
  async (operation) => {
    const core = await new StorageCore().open(),
      governor = new Governor(32 << 20),
      fault = new Error("injected JSON " + operation);
    let hit = false;
    const create = core.spill.create.bind(core.spill);
    const createSpy = vi.spyOn(core.spill, "create").mockImplementation(async (id) => {
      const file = await create(id);
      if (operation === "append") {
        const append = file.append.bind(file);
        file.append = async (page) => {
          if (!hit) {
            hit = true;
            throw fault;
          }
          return append(page);
        };
      } else if (operation === "read") {
        const read = file.read.bind(file);
        file.read = async (ref) => {
          if (!hit) {
            hit = true;
            throw fault;
          }
          return read(ref);
        };
      } else {
        const close = file.close.bind(file);
        file.close = async () => {
          await close();
          if (!hit) {
            hit = true;
            throw fault;
          }
        };
      }
      return file;
    });
    const prepare = core.executor.prepare.bind(core.executor);
    const prepareSpy = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
      const prepared = prepare(plan);
      return { ...prepared, execute: (req) => prepared.execute({ ...req, governor }) };
    });
    try {
      let thrown: unknown;
      try {
        await core.execute(sql, ["n"]);
      } catch (e) {
        thrown = e;
      }
      expect(hit).toBe(true);
      expect(hasCause(thrown, fault)).toBe(true);
      expect(governor.usedBytes).toBe(0);
      expect(core.spill.bytesInUse).toBe(0);
    } finally {
      createSpy.mockRestore();
      prepareSpy.mockRestore();
      await core.close();
    }
  },
  60000,
);
type AllocationKind = "arena" | "pool" | "grant";
async function allocationFault(
  at: number,
  boundary: "all" | "grant" = "all",
): Promise<AllocationKind> {
  const core = await new StorageCore().open(),
    governor = new Governor(32 << 20),
    fault = new Error("injected JSON allocation");
  let hit: AllocationKind | undefined,
    count = 0;
  const point = (kind: AllocationKind) => {
    if (boundary === "grant" && kind !== "grant") return;
    if (constructorFault.active && hit === undefined && ++count === at) {
      hit = kind;
      throw fault;
    }
  };
  // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the real arena
  const take = OutputArena.prototype.take;
  const arenas = vi.spyOn(OutputArena.prototype, "take").mockImplementation(function (
    this: OutputArena,
    bytes,
  ) {
    point("arena");
    return take.call(this, bytes);
  });
  // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the real pool
  const allocatePool = Pool.prototype.allocate;
  const pools = vi.spyOn(Pool.prototype, "allocate").mockImplementation(function (
    this: Pool,
    bytes,
  ) {
    point("pool");
    return allocatePool.call(this, bytes);
  });
  const admit = governor.admitGroup.bind(governor);
  governor.admitGroup = async (options) => {
    const parts = await admit(options);
    for (const part of parts)
      if (/project#.*\/minimum$/.test(part.label)) {
        const allocate = part.allocate.bind(part);
        part.allocate = (bytes) => {
          point("grant");
          return allocate(bytes);
        };
      }
    return parts;
  };
  const prepare = core.executor.prepare.bind(core.executor);
  const spy = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
    const prepared = prepare(plan);
    return { ...prepared, execute: (req) => prepared.execute({ ...req, governor }) };
  });
  try {
    let thrown: unknown;
    try {
      await core.execute(sql, ["n"]);
    } catch (e) {
      thrown = e;
    }
    expect(hit, `constructor allocation ${String(at)} must be reached`).toBeDefined();
    expect(hasCause(thrown, fault)).toBe(true);
    expect(governor.usedBytes).toBe(0);
    expect(core.spill.bytesInUse).toBe(0);
    if (hit === undefined) throw new Error("Missing allocation fault");
    return hit;
  } finally {
    constructorFault.active = false;
    arenas.mockRestore();
    pools.mockRestore();
    spy.mockRestore();
    await core.close();
  }
}
it.each([1, 3, 5, 8, 12])(
  "owns partially constructed JSON phase buffers (allocation %i)",
  async (at) => {
    await allocationFault(at);
  },
  60000,
);
it.each(seedsFor("next-jsonb-allocation-faults", [0xb6ea, 0xb6eb]))(
  "owns every generated constructor allocation boundary (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSONB constructor allocation faults",
      JSONB_ALLOCATION_FAULT_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(
          fc.record({ at: fc.integer({ min: 1, max: 12 }), boundary: fc.constant("all" as const) }),
          fc.record({
            at: fc.integer({ min: 1, max: 3 }),
            boundary: fc.constant("grant" as const),
          }),
        ),
        async ({ at, boundary }) => {
          reach.beginHistory();
          const kind = await allocationFault(at, boundary);
          reach.reach("allocator " + kind);
          reach.reach(
            at <= 3 ? "early constructor" : at <= 8 ? "middle constructor" : "late constructor",
          );
          reach.reach("original failure cause");
          reach.reach("zero files/grants before close");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          ...Array.from(
            { length: 12 },
            (_, i) => [{ at: i + 1, boundary: "all" }] as [{ at: number; boundary: "all" }],
          ),
          ...Array.from(
            { length: 3 },
            (_, i) => [{ at: i + 1, boundary: "grant" }] as [{ at: number; boundary: "grant" }],
          ),
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it("reports actual JSON checkpoint writes and restore passes", async () => {
  const core = await new StorageCore().open();
  let written = 0;
  const create = core.spill.create.bind(core.spill);
  const spy = vi.spyOn(core.spill, "create").mockImplementation(async (id) => {
    const file = await create(id),
      append = file.append.bind(file);
    file.append = async (page) => {
      const ref = await append(page);
      written += page.length;
      return ref;
    };
    return file;
  });
  try {
    await core.execute(sql, ["n"]);
    const operators = core.lastProfile?.operators ?? [];
    expect(written).toBeGreaterThan(0);
    expect(operators.reduce((n, p) => n + p.spilledBytes, 0)).toBe(written);
    expect(operators.reduce((n, p) => n + p.spillPasses, 0)).toBeGreaterThan(0);
    expect(core.spill.bytesInUse).toBe(0);
  } finally {
    spy.mockRestore();
    await core.close();
  }
});
