/** Every awaited native preparation edge must retire a stale capability before its WAL claim. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_AWAIT_STALE_REACH } from "../../testing/reach.js";
import { DURABLE_FORMATS } from "../durable-format.js";
import { ftsSemantics } from "../fts-config.js";
import { ftsDef } from "../access/fts-def.js";
import { ftsValue, FTS_WORKSPACE_BYTES } from "../exec/fts-value.js";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  TEXT,
  batch,
  texts,
  rules,
  rowsOf,
} from "../write/test-support.js";
import { setupFts, ftsParts, lastFtsCommit } from "./fts-transaction.test-support.js";

for (const tables of [1, 2])
  for (const group of ["pages", "metadata"] as const)
    for (const seed of seedsFor("next-fts-await-stale", [0xb879, 0xb87a]))
      it(`fences ${group} preparation IO for ${String(tables)} tables seed ${String(seed)}`, async (context) => {
        let stopped = false,
          retire: (() => void) | undefined;
        context.onTestFinished(() => {
          stopped = true;
          retire?.();
        });
        const reach = createReach(
          "FTS awaited capability",
          FTS_AWAIT_STALE_REACH.filter((state) =>
            group === "pages"
              ? state !== "publication" && state !== "builder IO"
              : state !== "final journal",
          ),
        );
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 1, max: 2 }),
            fc.constantFrom("renderer", "tokenizer", "scorer", "config", "workspace"),
            async (n, field) => {
              reach.beginHistory();
              const run = async (target: number | null): Promise<string[]> => {
                if (stopped) throw new Error("Retired FTS await history");
                const h = new Harness(),
                  restores: Array<() => void> = [],
                  events: string[] = [];
                let armed = false,
                  changed = false;
                const restore = () => {
                  armed = false;
                  for (const undo of restores.splice(0)) undo();
                };
                retire = () => {
                  restore();
                  void h.close().catch(() => undefined);
                };
                try {
                  const path = await setupFts(h, false),
                    tableIds = [1];
                  if (tables === 2) {
                    await autocommit(path, async (s) => {
                      const id = (
                        await s.changeCatalog({
                          kind: "createTable",
                          table: tableInput("other", [column("body", TEXT)], { primaryKey: null }),
                        })
                      ).id;
                      if (id === null) throw new Error("Missing FTS test table");
                      tableIds.push(id);
                    });
                    const id = tableIds[1];
                    const col =
                      id === undefined ? undefined : path.catalog.table(id)?.columns[0]?.id;
                    if (id === undefined || col === undefined)
                      throw new Error("Missing second FTS binding");
                    const { id: _id, ...fts } = ftsDef(2, id, col);
                    void _id;
                    await autocommit(path, (s) => s.changeCatalog({ kind: "createFts", fts }));
                  }
                  const adapter = {
                    ...ftsSemantics(),
                    config: { version: 1 },
                    workspaceBytes: FTS_WORKSPACE_BYTES,
                    render: ftsValue,
                  };
                  path.installFtsRenderer(adapter);
                  const tx = await path.begin(),
                    s = tx.statement({ rules });
                  const values = Array.from({ length: n }, (_, i) => `alpha beta ${String(i)}`);
                  for (const id of tableIds) await s.table(id).insert(batch(texts(values)), null);
                  await s.end();
                  const edge = (kind: string) => {
                    if (!armed || stopped) return;
                    events.push(kind);
                    if (!changed && target === events.length - 1) {
                      changed = true;
                      if (field === "renderer") adapter.rendererVersion++;
                      else if (field === "tokenizer") adapter.tokenizerVersion++;
                      else if (field === "scorer") adapter.scorerVersion++;
                      else if (field === "config") adapter.config.version++;
                      else adapter.workspaceBytes *= 2;
                    }
                  };
                  const begin = h.space.beginWrite.bind(h.space);
                  h.space.beginWrite = (label) => {
                    const out = begin(label);
                    if (label !== "FTS preparation") return out;
                    const write = out.write.bind(out),
                      open = out.open.bind(out);
                    out.write = async (parts) => {
                      const bytes = parts[0];
                      const journal =
                        bytes !== undefined &&
                        bytes.byteLength >= 4 &&
                        new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
                          0,
                          true,
                        ) === DURABLE_FORMATS.ftsJournal.magic;
                      const result = await write(parts);
                      edge(journal ? "journal" : "write");
                      return result;
                    };
                    out.open = (...args) => {
                      const builder = open(...args),
                        append = builder.append.bind(builder),
                        seal = builder.seal.bind(builder);
                      builder.append = async (...input) => {
                        const result = await append(...input);
                        edge("append");
                        return result;
                      };
                      builder.seal = async (...input) => {
                        const result = await seal(...input);
                        edge("seal");
                        return result;
                      };
                      restores.push(() => {
                        builder.append = append;
                        builder.seal = seal;
                      });
                      return builder;
                    };
                    restores.push(() => {
                      out.write = write;
                      out.open = open;
                    });
                    return out;
                  };
                  restores.push(() => {
                    h.space.beginWrite = begin;
                  });
                  const publish = h.space.publish.bind(h.space),
                    read = h.space.read.bind(h.space);
                  h.space.publish = async (...args) => {
                    const result = await publish(...args);
                    edge("publication");
                    return result;
                  };
                  h.space.read = async (...args) => {
                    const result = await read(...args);
                    edge("read");
                    return result;
                  };
                  restores.push(() => {
                    h.space.publish = publish;
                    h.space.read = read;
                  });
                  armed = true;
                  const receipt = await tx.commit(null);
                  restore();
                  await receipt.durable;
                  if (target !== null) {
                    expect(changed, "selected preparation boundary must be visited").toBe(true);
                    const entries = (await ftsParts(h)).entries;
                    for (let i = 0; i < tables; i++)
                      expect(
                        entries.some((e) => e.kind === "invalidate" && e.ftsId === i + 1),
                        "every stale binding must be invalidated by the SAME row claim",
                      ).toBe(true);
                    expect(
                      (await lastFtsCommit(h)).body.effects.filter((e) => e.kind === "fts"),
                    ).toHaveLength(1);
                    reach.reach("same row claim");
                    reach.reach("all touched bindings");
                  }
                  await path.recycleLog();
                  await h.reopen();
                  const pin = h.path.pin();
                  try {
                    for (const id of tableIds)
                      expect(await rowsOf(pin, id)).toEqual(values.map((v) => [v]));
                  } finally {
                    pin.release();
                  }
                  reach.reach("exact reopen");
                  return events;
                } finally {
                  restore();
                  await h.close();
                  expect(h.governor.usedBytes).toBe(0);
                  retire = undefined;
                }
              };
              const trace = await run(null);
              expect(trace).toContain("journal");
              expect(trace).toContain("publication");
              // Independent histories, partitioned only for bounded CI bodies. Every seed,
              // generated input and IO position still runs; the groups share no live state.
              for (let i = 0; i < trace.length; i++) {
                const isPage = trace[i] === "write" || trace[i] === "journal";
                if (isPage !== (group === "pages")) continue;
                const events = await run(i);
                expect(events.slice(0, i + 1)).toEqual(trace.slice(0, i + 1));
                reach.reach("every IO boundary");
                if (trace[i] === "journal" && i === trace.lastIndexOf("journal"))
                  reach.reach("final journal");
                if (trace[i] === "publication") reach.reach("publication");
                if (trace[i] === "append" || trace[i] === "seal") reach.reach("builder IO");
              }
            },
          ),
          {
            seed,
            numRuns: 8,
            examples: [
              [1, "renderer"],
              [2, "workspace"],
            ],
          },
        );
        reach.assertReached();
      }, 60_000);
