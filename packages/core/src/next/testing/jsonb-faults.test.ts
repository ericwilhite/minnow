/** These are injected I/O/completion/allocation failures, not SQL semantic oracles. */
import { expect, it, vi } from "vitest";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
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
it.each([1, 3, 5, 8, 12])(
  "owns partially constructed JSON phase buffers (allocation %i)",
  async (at) => {
    const core = await new StorageCore().open(),
      governor = new Governor(32 << 20),
      fault = new Error("injected JSON allocation");
    let hit = false,
      count = 0;
    const admit = governor.admitGroup.bind(governor);
    governor.admitGroup = async (options) => {
      const parts = await admit(options);
      for (const part of parts)
        if (/project#.*\/minimum$/.test(part.label)) {
          const allocate = part.allocate.bind(part);
          part.allocate = (bytes) => {
            if (++count === at) {
              hit = true;
              throw fault;
            }
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
      expect(hit).toBe(true);
      expect(hasCause(thrown, fault)).toBe(true);
      expect(governor.usedBytes).toBe(0);
      expect(core.spill.bytesInUse).toBe(0);
    } finally {
      spy.mockRestore();
      await core.close();
    }
  },
  60000,
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
