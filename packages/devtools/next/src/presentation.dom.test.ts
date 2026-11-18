// @vitest-environment happy-dom
import { afterEach, expect, it, vi } from "vitest";
import { decodeResult } from "../../../core/dist/next/owner/result.js";
import type { Result } from "../../../core/dist/next/owner/api.js";
import type { CatalogDescription } from "../../../core/dist/next/owner/inspection.js";
import { createNativeResultGrid, selectionText } from "./result.js";
import { renderCatalog, renderExplain, editorSchema } from "./catalog.js";
import { bindNativeDevtools } from "./binding.js";
import type { NativeDevtoolsTarget } from "./target.js";
const catalog: CatalogDescription = {
  version: 1,
  catalogVersion: 7,
  tables: [],
  views: [],
  types: [],
  sequences: [],
  fts: [],
  triggers: [],
};
function result(): Result {
  return decodeResult({
    columns: ["x", "x", "<img src=x>"].map((name) => ({
      name,
      type: { kind: "int32", bits: 32 },
      sqlType: "integer",
      nullable: false,
      render: null,
    })),
    frames: [
      {
        sequence: 0,
        rowCount: 1,
        dictionaries: [],
        columns: [1, 2, 3].map((value) => ({
          form: "flat",
          length: 1,
          validity: null,
          data: { layout: "i32", values: Int32Array.of(value) },
        })),
      },
    ],
    summary: {
      command: "SELECT",
      rowCount: 1,
      counts: null,
      lsn: null,
      replayed: false,
      closedEarly: false,
      transaction: "idle",
      catalogVersion: 7,
    },
  });
}
afterEach(() => {
  document.body.replaceChildren();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
it("renders and copies duplicate headers by ordinal without parsing returned text", async () => {
  const writeText = vi.fn(async () => undefined);
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  const grid = createNativeResultGrid();
  document.body.append(grid.node);
  grid.show(result());
  expect(
    Array.from(grid.node.querySelectorAll(".grid-name")).map((node) => node.textContent),
  ).toEqual(["x", "x", "<img src=x>"]);
  expect(grid.node.querySelector("img")).toBeNull();
  const cells = grid.node.querySelectorAll<HTMLElement>(".grid-row .cell");
  expect(Array.from(cells).map((cell) => cell.textContent)).toEqual(["1", "2", "3"]);
  cells[0]?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
  cells[0]?.dispatchEvent(new KeyboardEvent("keydown", { key: "c", ctrlKey: true, bubbles: true }));
  expect(writeText).toHaveBeenCalledWith("x\tx\t<img src=x>\n1\t2\t3\n");
  grid.clear();
  expect(grid.node.querySelectorAll(".grid-row")).toHaveLength(0);
});
it("quotes clipboard text without losing label positions or NULL distinction", () => {
  expect(
    selectionText(
      [
        { name: "@0", displayName: 'a"\n' },
        { name: "@1", displayName: "a" },
      ],
      [{ "@0": "one\ttwo", "@1": null }],
    ),
  ).toBe('"a""\n"\ta\n"one\ttwo"\t\n');
});
it("renders catalog and explain as text with no invented runtime health", () => {
  const input: CatalogDescription = {
    ...catalog,
    tables: [
      {
        id: 2,
        name: "<table>",
        schemaVersion: 1,
        managed: false,
        primaryKey: null,
        columns: [
          {
            id: 3,
            name: "body<img>",
            sqlType: "text",
            typeId: null,
            autoIncrement: false,
            nullable: true,
            defaultSql: null,
            generatedSql: null,
            identity: null,
            enumValues: null,
          },
        ],
        indexes: [],
        checks: [],
        foreignKeys: [],
      },
    ],
    fts: [
      {
        id: 4,
        tableId: 2,
        columnId: 3,
        tokenizerVersion: 1,
        rendererVersion: 2,
        scorerVersion: 1,
        configVersion: 1,
      },
    ],
    triggers: [
      {
        id: 5,
        name: "<trigger>",
        tableId: 2,
        timing: "before",
        event: "delete",
        statements: [
          {
            sql: "INSERT INTO audit VALUES($1)",
            bindings: [{ source: "old", columnId: 3, name: "body<img>" }],
          },
        ],
      },
    ],
    types: [{ id: 1, name: "<script>", labels: ["ok"] }],
    sequences: [
      {
        id: 1,
        name: "seq",
        start: "9007199254740993",
        increment: "1",
        min: "1",
        max: "9223372036854775807",
        cycle: false,
        ownedBy: null,
      },
    ],
    views: [
      {
        id: 1,
        name: "__proto__",
        managed: true,
        sql: "SELECT '<img>'",
        columns: [{ name: "a.b", sqlType: "bigint", nullable: false }],
        dependencies: [],
      },
    ],
  };
  const node = renderCatalog(input);
  document.body.append(node);
  expect(node.querySelector("script,img")).toBeNull();
  expect(node.textContent).toContain("9007199254740993");
  expect(node.textContent).toContain("<trigger>: before delete (definition)");
  expect(node.textContent).toContain("FTS 4: column body<img> (definition only)");
  expect(node.textContent).not.toMatch(/\b(?:healthy|running|firing)\b/u);
  expect(node.textContent).toContain("definition only");
  expect(node.textContent).toContain("__proto__");
  expect(editorSchema(input).__proto__).toEqual(["a.b"]);
  const plan = renderExplain({
    version: 1,
    catalogVersion: 7,
    command: "SELECT",
    kind: "query",
    text: "<script> logical; not executed",
    parameters: [],
    columns: [],
  });
  expect(plan.textContent).toBe("<script> logical; not executed");
  expect(plan.querySelector("script")).toBeNull();
});
it("destroy cancels and drains its operation, preserving an eventual committed result", async () => {
  let release: (value: Result) => void = () => undefined,
    entered: () => void = () => undefined,
    signal: AbortSignal | undefined;
  const running = new Promise<Result>((resolve) => {
      release = resolve;
    }),
    started = new Promise<void>((resolve) => {
      entered = resolve;
    });
  const close = vi.fn();
  const target: NativeDevtoolsTarget & { close: () => void } = {
    query: async () => result(),
    execute: (_sql, _params, options) => {
      signal = options?.signal;
      entered();
      return running;
    },
    cursor: () => {
      throw Error("unused cursor");
    },
    describeCatalog: async () => catalog,
    explain: async () => ({
      version: 1,
      catalogVersion: 7,
      command: "SELECT",
      kind: "query",
      text: "logical; not executed",
      parameters: [],
      columns: [],
    }),
    close,
  };
  const view = bindNativeDevtools(target);
  document.body.append(view.node);
  const pending = view.execute("UPDATE t SET v=1");
  await started;
  await expect(view.query("SELECT 1")).rejects.toThrow("already running");
  let drained = false;
  const closing = view.destroy().then(() => {
    drained = true;
  });
  expect(signal?.aborted).toBe(true);
  await Promise.resolve();
  expect(drained).toBe(false);
  await expect(view.query("SELECT 1")).rejects.toThrow("closed");
  const committed = decodeResult({
    columns: [],
    frames: [],
    summary: {
      command: "UPDATE",
      rowCount: 1,
      counts: { t: 1 },
      lsn: 8,
      replayed: false,
      closedEarly: false,
      transaction: "idle",
      catalogVersion: 7,
    },
  });
  release(committed);
  expect(await pending).toBe(committed);
  await closing;
  expect(drained).toBe(true);
  expect(close).not.toHaveBeenCalled();
  expect(view.node.isConnected).toBe(false);
  await view.destroy();
});
it("uses typed catalog/explain inspection in the binding without executing a plan", async () => {
  const query = vi.fn(async () => result()),
    execute = vi.fn(async () => result()),
    describeCatalog = vi.fn(async () => catalog),
    explain = vi.fn(async () => ({
      version: 1 as const,
      catalogVersion: 7,
      command: "INSERT",
      kind: "modify",
      text: "INSERT logical; not executed",
      parameters: [],
      columns: [],
    }));
  const view = bindNativeDevtools({
    query,
    execute,
    describeCatalog,
    explain,
    cursor: () => {
      throw Error("unused cursor");
    },
  });
  document.body.append(view.node);
  await view.refreshCatalog();
  await view.explain("INSERT INTO t VALUES(1)");
  expect(view.node.textContent).toContain("not executed");
  expect(execute).not.toHaveBeenCalled();
  expect(query).not.toHaveBeenCalled();
  await view.query("SELECT 1");
  expect(view.node.textContent).toContain("SELECT: 1 of 1 rows");
  await view.destroy();
});

it("retires grid observer/frame ownership when the binding is destroyed", async () => {
  const disconnect = vi.fn();
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe = vi.fn();
      disconnect = disconnect;
    },
  );
  const view = bindNativeDevtools({
    query: async () => result(),
    execute: async () => result(),
    cursor: () => {
      throw Error("unused");
    },
    describeCatalog: async () => catalog,
    explain: async () => ({
      version: 1,
      catalogVersion: 7,
      command: "SELECT",
      kind: "query",
      text: "logical",
      parameters: [],
      columns: [],
    }),
  });
  document.body.append(view.node);
  await view.query("SELECT 1");
  await view.destroy();
  await view.destroy();
  expect(disconnect).toHaveBeenCalledTimes(1);
});
it("retains committed-without-result counts and never invents replayed rows", async () => {
  const { CommittedWithoutResultError } = await import("../../../core/dist/next/owner/errors.js");
  const failure = new CommittedWithoutResultError({ t: 3 });
  const view = bindNativeDevtools({
    query: async () => result(),
    execute: async () => {
      throw failure;
    },
    cursor: () => {
      throw Error("unused");
    },
    describeCatalog: async () => catalog,
    explain: async () => ({
      version: 1,
      catalogVersion: 7,
      command: "SELECT",
      kind: "query",
      text: "logical",
      parameters: [],
      columns: [],
    }),
  });
  await expect(view.execute("UPDATE t SET v=1 RETURNING v")).rejects.toBe(failure);
  expect(failure.counts).toEqual({ t: 3 });
  expect(view.node.textContent).toContain("returned rows were lost");
  expect(view.node.querySelectorAll(".grid-row")).toHaveLength(0);
  await view.destroy();
});
