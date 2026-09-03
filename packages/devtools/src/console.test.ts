// @vitest-environment happy-dom
import type { QueryResult } from "@minnowdb/core";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConfirmLayer, ConfirmRequest } from "./confirm.js";
import { createConsole, rowCap } from "./console.js";
import { el } from "./dom.js";

function result(count: number): QueryResult {
  return {
    columns: ["id"],
    columnDomains: [null],
    rows: Array.from({ length: count }, (_, id) => ({ id })),
  };
}

interface Harness {
  view: ReturnType<typeof createConsole>;
  query: ReturnType<typeof vi.fn>;
  execute: ReturnType<typeof vi.fn>;
  asked: ConfirmRequest[];
  onCatalogChange: ReturnType<typeof vi.fn>;
  editor: HTMLTextAreaElement;
  runButton: HTMLButtonElement;
  status: () => string;
  notice: () => string | undefined;
  rowCount: () => number;
}

function harness(
  options: {
    write?: boolean;
    rows?: (sql: string) => QueryResult | Promise<QueryResult>;
    confirm?: boolean;
    extra?: Record<string, unknown>;
  } = {},
): Harness {
  const query = vi.fn(async (sql: string) => (options.rows ?? (() => result(3)))(sql));
  const execute = vi.fn(async (sql: string) => ({
    kind: sql.startsWith("CREATE") ? "create-table" : "insert",
    table: "t",
    rowCount: 1,
    version: 1,
  }));
  const asked: ConfirmRequest[] = [];
  const confirm: ConfirmLayer = {
    node: el("div"),
    ask: async (request) => {
      asked.push(request);
      return options.confirm ?? true;
    },
    dismiss: () => undefined,
  };
  const onCatalogChange = vi.fn(async () => undefined);
  const host = el("div");
  const root = host.attachShadow({ mode: "open" });
  document.body.append(host);
  const view = createConsole({
    target: {
      listTables: async () => [],
      explain: async () => "plan",
      query,
      execute,
      ...options.extra,
    } as never,
    confirm,
    write: options.write ?? true,
    initialQuery: "",
    storageKey: `test-${crypto.randomUUID()}`,
    root,
    onCatalogChange,
  });
  root.append(view.node);
  const editor = view.node.querySelector("textarea");
  const runButton = [...view.node.querySelectorAll("button")].find((b) => b.textContent === "Run");
  if (editor === null || runButton === undefined) throw new Error("console missing controls");
  return {
    view,
    query,
    execute,
    asked,
    onCatalogChange,
    editor,
    runButton,
    status: () => view.node.querySelector(".console-main > .statusbar")?.textContent ?? "",
    notice: () => {
      const node = view.node.querySelector<HTMLElement>(".notice");
      return node === null || node.hidden ? undefined : node.textContent;
    },
    rowCount: () =>
      Number(view.node.querySelector(".grid")?.getAttribute("aria-rowcount") ?? "0") - 1,
  };
}

async function settled(): Promise<void> {
  for (let turn = 0; turn < 20; turn += 1) await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

async function until(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 200 && !condition(); attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  expect(condition()).toBe(true);
}

afterEach(() => {
  document.body.replaceChildren();
});

describe("row cap", () => {
  it("caps an unbounded SELECT, says so, and offers the rest", async () => {
    const h = harness({ rows: (sql) => result(sql.includes("LIMIT") ? rowCap + 1 : 5000) });
    await h.view.runQuery("SELECT * FROM t");
    expect(h.query.mock.calls[0]?.[0]).toBe(`SELECT * FROM t\nLIMIT ${String(rowCap + 1)}`);
    expect(h.rowCount()).toBe(rowCap);
    expect(h.status()).toContain("first 1,000 rows");
    const more = [...h.view.node.querySelectorAll("button")].find(
      (b) => b.textContent === "Load all rows",
    );
    if (more === undefined) throw new Error("no load-all button");
    more.click();
    await settled();
    expect(h.query.mock.calls[1]?.[0]).toBe("SELECT * FROM t");
    expect(h.rowCount()).toBe(5000);
    expect(h.status()).toContain("5,000 rows");
  });

  it("leaves a SELECT with its own LIMIT alone", async () => {
    const h = harness();
    await h.view.runQuery("SELECT * FROM t LIMIT 5");
    expect(h.query.mock.calls[0]?.[0]).toBe("SELECT * FROM t LIMIT 5");
    expect(h.status()).toContain("3 rows");
  });
});

describe("scripts", () => {
  it("runs each statement in turn behind one confirmation and lists the outcomes", async () => {
    const h = harness();
    await h.view.runQuery(
      "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t (id) VALUES (1); SELECT * FROM t;",
    );
    expect(h.asked).toHaveLength(1);
    expect(h.asked[0]?.title).toBe("Run 3 statements");
    expect(h.asked[0]?.facts.map(([key]) => key)).toEqual(["statements", "changes"]);
    expect(h.execute).toHaveBeenCalledTimes(2);
    expect(h.query).toHaveBeenCalledTimes(1);
    expect(h.notice()).toContain("1. created table t");
    expect(h.notice()).toContain("2. insert: 1 row in t");
    expect(h.notice()).toContain("3. 3 rows");
    expect(h.rowCount()).toBe(3);
    expect(h.onCatalogChange).toHaveBeenCalledTimes(1);
    expect(h.status()).toContain("3 of 3 statements");
  });

  it("stops at the first failure and points at it", async () => {
    const h = harness({
      rows: (sql) => {
        if (sql.includes("boom")) throw new Error("no such table: boom");
        return result(1);
      },
    });
    await h.view.runQuery("SELECT 1; SELECT * FROM boom; SELECT 3");
    expect(h.query).toHaveBeenCalledTimes(2);
    expect(h.notice()).toContain("2. no such table: boom");
    expect(h.status()).toBe("failed");
  });

  it("points a compile error at its statement inside the script", async () => {
    const h = harness();
    await h.view.runQuery("SELECT 1;\nSELEC 2");
    expect(h.query).not.toHaveBeenCalled();
    expect(h.notice()).toBeDefined();
    expect(h.editor.selectionStart).toBeGreaterThanOrEqual(10);
  });
});

describe("the selection", () => {
  it("runs alone when there is one", async () => {
    const h = harness();
    h.editor.value = "SELECT 1;\nSELECT 2";
    h.editor.setSelectionRange(10, 18);
    h.runButton.click();
    await settled();
    expect(h.query).toHaveBeenCalledTimes(1);
    expect(h.query.mock.calls[0]?.[0]).toBe("SELECT 2\nLIMIT 1001");
  });
});

describe("cancel", () => {
  it("aborts the running query and records the run as cancelled", async () => {
    const h = harness({
      rows: () =>
        new Promise<QueryResult>((_, reject) => {
          setTimeout(() => {
            reject(new DOMException("stopped", "AbortError"));
          }, 50);
        }),
    });
    const run = h.view.runQuery("SELECT * FROM t");
    const cancel = [...h.view.node.querySelectorAll("button")].find(
      (b) => b.textContent === "Cancel",
    );
    await until(() => cancel?.hidden === false);
    expect(h.runButton.hidden).toBe(true);
    cancel?.click();
    await run;
    expect(h.status()).toBe("cancelled");
    expect(h.runButton.hidden).toBe(false);
    expect(h.view.node.querySelector(".hitem.failed .houtcome")?.textContent).toBe("cancelled");
    const options = h.query.mock.calls[0]?.[1] as { signal: AbortSignal };
    expect(options.signal.aborted).toBe(true);
  });
});

describe("statistics", () => {
  it("shows the peak memory the engine reports", async () => {
    const h = harness({
      extra: {
        query: vi.fn(
          async (_sql: string, options: { onStats?: (s: { peakMemoryBytes: number }) => void }) => {
            options.onStats?.({ peakMemoryBytes: 3 * 1024 * 1024 });
            return result(2);
          },
        ),
      },
    });
    await h.view.runQuery("SELECT * FROM t");
    expect(h.status()).toContain("peak 3.0 MB");
  });
});

describe("writes", () => {
  it("refuses a write when the panel is read-only, before anything runs", async () => {
    const h = harness({ write: false });
    await h.view.runQuery("INSERT INTO t (id) VALUES (1)");
    expect(h.execute).not.toHaveBeenCalled();
    expect(h.notice()).toContain("Writes are turned off");
  });

  it("counts the rows an UPDATE will touch for the confirmation", async () => {
    const h = harness({
      rows: (sql) =>
        sql.startsWith("SELECT COUNT")
          ? { columns: ["row_count"], columnDomains: [null], rows: [{ row_count: 12 }] }
          : result(0),
    });
    await h.view.runQuery("UPDATE t SET id = 2 WHERE id > 5");
    expect(h.asked[0]?.preview).toBeDefined();
    await expect(h.asked[0]?.preview?.()).resolves.toBe("12 rows");
    expect(h.query.mock.calls[0]?.[0]).toBe("SELECT COUNT(*) AS row_count FROM t WHERE id > 5");
  });

  it("does nothing when the confirmation is declined", async () => {
    const h = harness({ confirm: false });
    await h.view.runQuery("DELETE FROM t");
    expect(h.execute).not.toHaveBeenCalled();
    expect(h.status()).toBe("cancelled");
  });
});

describe("live", () => {
  it("follows the last SELECT and stops after a write", async () => {
    const close = vi.fn();
    const subscription = { close };
    let onChange: ((result: QueryResult) => void) | undefined;
    const subscribe = vi.fn(
      async (_sql: string, options: { onChange: (r: QueryResult) => void }) => {
        onChange = options.onChange;
        return subscription;
      },
    );
    const setClose = vi.fn();
    const set = { subscribe, close: setClose };
    const h = harness({ extra: { liveQueries: () => set } });
    const live = [...h.view.node.querySelectorAll("button")].find((b) => b.textContent === "Live");
    if (live === undefined) throw new Error("no live toggle");
    live.click();
    expect(live.getAttribute("aria-pressed")).toBe("true");
    await h.view.runQuery("SELECT * FROM t");
    await settled();
    expect(subscribe).toHaveBeenCalledWith("SELECT * FROM t\nLIMIT 1001", expect.anything());
    onChange?.(result(7));
    expect(h.rowCount()).toBe(7);
    expect(h.status()).toContain("live · 7 rows");
    await h.view.runQuery("INSERT INTO t (id) VALUES (1)");
    expect(close).toHaveBeenCalled();
    h.view.destroy();
    expect(setClose).toHaveBeenCalled();
  });

  it("offers no toggle when the target cannot follow", () => {
    const h = harness();
    const live = [...h.view.node.querySelectorAll("button")].find((b) => b.textContent === "Live");
    expect(live?.hidden).toBe(true);
  });
});

describe("copy and download", () => {
  it("copies the shown rows as CSV and JSON", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    const h = harness({ rows: () => result(2) });
    await h.view.runQuery("SELECT * FROM t");
    const button = (label: string): HTMLButtonElement => {
      const found = [...h.view.node.querySelectorAll("button")].find(
        (b) => b.textContent === label,
      );
      if (found === undefined) throw new Error(`no ${label}`);
      return found;
    };
    button("Copy CSV").click();
    await settled();
    expect(writeText).toHaveBeenLastCalledWith("id\n0\n1\n");
    button("Copy JSON").click();
    await settled();
    expect(JSON.parse(writeText.mock.calls[1]?.[0] as string)).toEqual([{ id: 0 }, { id: 1 }]);
    expect(h.status()).toBe("copied 2 rows as JSON");
  });

  it("reads a capped result again through the cursor for the download", async () => {
    const batches = [result(2), result(2)];
    const queryCursor = vi.fn(async function* () {
      for (const batch of batches) yield batch;
    });
    const created: string[] = [];
    URL.createObjectURL = vi.fn((blob: Blob) => {
      void blob.text().then((text) => created.push(text));
      return "blob:x";
    });
    URL.revokeObjectURL = vi.fn();
    const h = harness({
      rows: (sql) => result(sql.includes("LIMIT") ? rowCap + 1 : 0),
      extra: { queryCursor },
    });
    await h.view.runQuery("SELECT * FROM t");
    [...h.view.node.querySelectorAll("button")]
      .find((b) => b.textContent === "Download CSV")
      ?.click();
    await settled();
    expect(queryCursor).toHaveBeenCalledWith("SELECT * FROM t");
    expect(created[0]).toBe("id\n0\n1\n0\n1\n");
    expect(h.status()).toBe("saved 4 rows");
  });
});
