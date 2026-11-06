import { it, expect } from "vitest";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  INT,
  ints,
  batch,
  rules,
} from "./test-support.js";
it.each([0, 1].flatMap((prior) => ["write", "publish"].map((phase) => ({ prior, phase }))))(
  "close waits at $phase after $prior prior staging publications",
  async ({ prior, phase }) => {
    const h = new Harness("strict", "memory", 8, undefined, { merges: { mode: "off" } });
    const p = await h.open();
    let t = 0;
    await autocommit(p, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await p.quiesce();
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const started = new Promise<void>((r) => (entered = r));
    let stages = 0;
    const begin = h.space.beginWrite.bind(h.space);
    h.space.beginWrite = (label, opts) => {
      const w = begin(label, opts);
      if (label === "stage" && stages++ === prior) {
        let first = true;
        const pause = async () => {
          if (first) {
            first = false;
            entered();
            await gate;
          }
        };
        if (phase === "write") {
          const write = w.write.bind(w);
          w.write = async (parts) => {
            await pause();
            return write(parts);
          };
        } else {
          const publish = h.space.publish.bind(h.space);
          h.space.publish = async (...args) => {
            await pause();
            return publish(...args);
          };
        }
      }
      return w;
    };
    const tx = await p.begin();
    const s = tx.statement({ rules });
    let error: unknown;
    const inserting = (async () => {
      for (let i = 0; i < 40; i += 8)
        await s.table(t).insert(batch(ints(Array.from({ length: 8 }, (_, k) => i + k))), null);
    })().catch((e: unknown) => {
      error = e;
    });
    await started;
    let closed = false;
    const closing = p.close().then(() => {
      closed = true;
    });
    for (let i = 0; i < 100; i++) await Promise.resolve();
    const early = closed;
    release();
    await inserting;
    await closing;
    await h.close();
    expect(early).toBe(false);
    expect(error).toMatchObject({ name: "WritePathClosedError" });
    expect(h.space.stats.pinCount).toBe(0);
  },
);
