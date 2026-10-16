// @vitest-environment happy-dom
import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConfirmLayer } from "../confirm.js";
import { toCatalog } from "./catalog.js";
import { createExplorer } from "./explorer.js";

vi.setConfig({ testTimeout: 30_000 });

/** The explorer over a real in-memory database: 450 events, more than two pages of 200. */
async function harness() {
  const database = new MinnowDatabase(new MemoryBlockStore());
  await database.createTable({
    name: "events",
    uniqueKey: "event_id",
    columns: [
      { name: "event_id", type: "number" },
      { name: "kind", type: "string" },
    ],
  });
  await database.insertBatch(
    "events",
    Array.from({ length: 450 }, (_, event_id) => ({ event_id, kind: "created" })),
  );
  const confirm: ConfirmLayer = {
    node: document.createElement("div"),
    ask: async () => true,
    dismiss: () => undefined,
  };
  const explorer = createExplorer({
    target: database,
    confirm,
    write: true,
    storageKey: `test-${crypto.randomUUID()}`,
  });
  document.body.append(explorer.node);
  const viewport = explorer.node.querySelector<HTMLElement>(".grid-viewport");
  if (viewport === null) throw new Error("no grid");
  Object.defineProperty(viewport, "clientHeight", { value: 26 * 15 });
  const catalog = toCatalog(await database.listTables(), await database.introspect());
  await explorer.setCatalog(catalog);
  const rows = () =>
    Number(explorer.node.querySelector(".grid")?.getAttribute("aria-rowcount")) - 1;
  const cell = (index: number, column: number) =>
    explorer.node.querySelector(`.grid-row[data-index="${String(index)}"]`)?.children[column]
      ?.textContent;
  const button = (name: string) => {
    const found = [...explorer.node.querySelectorAll("button")].find(
      (entry) => entry.textContent === name,
    );
    if (found === undefined) throw new Error(`no ${name} button`);
    return found;
  };
  return { database, explorer, viewport, catalog, rows, cell, button };
}

let close: (() => Promise<void>) | undefined;
afterEach(async () => {
  await close?.();
  close = undefined;
  document.body.replaceChildren();
});

describe("explorer live mode", () => {
  it("follows every loaded row, so turning it on or a commit never cuts the grid back", async () => {
    const { database, explorer, viewport, rows, cell, button } = await harness();
    close = async () => {
      explorer.destroy();
      await database.close();
    };
    // Scrolling near the end loads the next page.
    viewport.scrollTop = 26 * 190;
    viewport.dispatchEvent(new Event("scroll"));
    await vi.waitFor(() => {
      expect(rows()).toBe(400);
    });
    button("Live").click();
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(rows()).toBe(400);
    await database.updateBatch("events", { keys: [195], changes: { kind: ["paid"] } });
    await vi.waitFor(() => {
      expect(cell(195, 1)).toBe("paid");
    });
    expect(rows()).toBe(400);
    expect(viewport.scrollTop).toBe(26 * 190);
  });

  it("holds a live update back while a cell is being edited, and applies it after", async () => {
    const { database, explorer, rows, cell, button } = await harness();
    close = async () => {
      explorer.destroy();
      await database.close();
    };
    button("Live").click();
    await new Promise((resolve) => setTimeout(resolve, 200));
    const target = explorer.node.querySelector('.grid-row[data-index="2"]')?.children[1];
    target?.dispatchEvent(new MouseEvent("dblclick", { bubbles: true }));
    const input = explorer.node.querySelector<HTMLInputElement>(".cell-input");
    expect(input).not.toBeNull();
    if (input !== null) input.value = "half typed";
    await database.updateBatch("events", { keys: [5], changes: { kind: ["paid"] } });
    await new Promise((resolve) => setTimeout(resolve, 200));
    // The edit survived the commit; the grid has not moved under it.
    expect(explorer.node.querySelector<HTMLInputElement>(".cell-input")?.value).toBe("half typed");
    expect(cell(5, 1)).toBe("created");
    input?.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    expect(cell(5, 1)).toBe("paid");
    expect(rows()).toBe(200);
  });
});

describe("explorer catalog reload", () => {
  it("keeps the filters and sort of the table it reopens", async () => {
    const { database, explorer, catalog, cell } = await harness();
    close = async () => {
      explorer.destroy();
      await database.close();
    };
    await explorer.open("events", [
      { column: "event_id", type: "number", operator: ">=", values: [100] },
    ]);
    explorer.node
      .querySelector<HTMLButtonElement>('button[aria-label^="Sort by event_id"]')
      ?.click();
    explorer.node
      .querySelector<HTMLButtonElement>('button[aria-label^="Sort by event_id"]')
      ?.click();
    await vi.waitFor(() => {
      expect(cell(0, 0)).toBe("449");
    });
    await explorer.setCatalog(catalog);
    await vi.waitFor(() => {
      expect(cell(0, 0)).toBe("449");
    });
    expect(
      [...explorer.node.querySelectorAll(".chip-text")].map((chip) => chip.textContent),
    ).toEqual(["event_id ≥ 100"]);
  });
});

describe("explorer live mode and a page in flight", () => {
  it("restarts the subscription after a reload lands, so a commit during it is not lost", async () => {
    const { database, explorer, button, cell } = await harness();
    close = async () => {
      explorer.destroy();
      await database.close();
    };
    button("Live").click();
    await new Promise((resolve) => setTimeout(resolve, 200));
    // Hold the first page of a re-sort open, commit while it is held, then let it finish: the
    // held page predates the commit, and the live result that saw the commit came in meanwhile.
    let release: (() => void) | undefined;
    const query = database.query.bind(database);
    const held = vi.spyOn(database, "query").mockImplementation(async (sql, options) => {
      if (release === undefined && /ORDER BY/i.test(sql) && /LIMIT 200/i.test(sql)) {
        const snapshot = await query(sql, options);
        await new Promise<void>((resolve) => {
          release = resolve;
        });
        return snapshot;
      }
      return query(sql, options);
    });
    explorer.node.querySelector<HTMLButtonElement>('button[aria-label^="Sort by kind"]')?.click();
    await vi.waitFor(() => {
      expect(release).toBeDefined();
    });
    await database.updateBatch("events", { keys: [0], changes: { kind: ["a-first"] } });
    await new Promise((resolve) => setTimeout(resolve, 200));
    held.mockRestore();
    release?.();
    await vi.waitFor(() => {
      expect(cell(0, 1)).toBe("a-first");
    });
  });
});
