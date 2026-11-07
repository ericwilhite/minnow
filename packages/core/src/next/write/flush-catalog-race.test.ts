/** Checkpoints must carry the indexes their catalog requires, even across a held flush. */
import { expect, it } from "vitest";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { Harness, autocommit, batch, column, ints, INT, tableInput } from "./test-support.js";
import { need } from "./bytes.js";

it.each(["create", "drop"] as const)(
  "retries a flush across concurrent index %s",
  async (change) => {
    let armed = false;
    let entered!: () => void;
    let release!: () => void;
    const atPublish = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const h = new Harness("strict", "sim", 2, undefined, {
      beforeFlushPublish: async () => {
        if (armed) {
          armed = false;
          entered();
          await gate;
        }
      },
    });
    try {
      const p = await h.open();
      let table = 0;
      await autocommit(p, async (s) => {
        table =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [column("id", INT, false)]),
            })
          ).id ?? 0;
      });
      const id = need(p.catalog.table(table)).columns[0]?.id ?? 0;
      const create = () =>
        autocommit(p, (s) =>
          s.changeCatalog({
            kind: "createIndex",
            index: {
              tableId: table,
              name: "late",
              columnIds: [id],
              unique: false,
              role: "secondary",
            },
          }),
        );
      if (change === "drop") await create();
      await p.quiesce();
      armed = true;
      await autocommit(p, (s) => s.table(table).insert(batch(ints([1, 2])), null));
      await autocommit(p, (s) => s.table(table).insert(batch(ints([3])), null));
      await atPublish;
      if (change === "create") await create();
      else
        await autocommit(p, (s) =>
          s.changeCatalog({
            kind: "dropIndex",
            indexId: need(p.catalog.table(table)?.indexes.find((i) => i.name === "late")).id,
          }),
        );
      release();
      await p.quiesce();
      expect(p.failure).toBeNull();
      const manifest = decodeManifestRoot(need(h.space.root), {
        allocate: () => true,
        free: () => undefined,
      });
      const root = need(manifest.value.tables.find((t) => t.tableId === table));
      expect(root.secondaryIndexes.map((i) => i.indexId)).toEqual(
        need(p.catalog.table(table))
          .indexes.filter((i) => i.role === "secondary")
          .map((i) => i.id),
      );
      const index =
        change === "create"
          ? need(p.catalog.table(table)?.indexes.find((i) => i.name === "late")).id
          : (need(p.catalog.table(table)).primaryKey ?? 0);
      for (let attempt = 0; attempt < 2; attempt++) {
        const path = attempt === 0 ? p : await h.reopen();
        const keys = new KeyBatch({ allocate: () => true, free: () => undefined });
        const view = path.pin();
        try {
          encodeKeyBatch(
            "equality",
            [{ vector: ints([1, 2, 3]), direction: "asc", nulls: "first" }],
            { length: 3, selection: null },
            keys,
          );
          const out = { probes: new Int32Array(3), locators: new Float64Array(3), count: 0 };
          await view.index(table, index).probe(keys, out);
          expect(out.count).toBe(3);
        } finally {
          keys.close();
          view.release();
        }
      }
      await h.space.audit();
    } finally {
      release();
      await h.close();
    }
  },
);
