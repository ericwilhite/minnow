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
it("review: unclaimed staging walk gives timers a turn", async () => {
  const h = new Harness("strict", "sim", 8, undefined, { merges: { mode: "off" } });
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
  const tx = await p.begin();
  const s = tx.statement({ rules });
  for (let i = 0; i < 2400; i += 8)
    await s.table(t).insert(batch(ints(Array.from({ length: 8 }, (_, k) => i + k))), null);
  await s.end();
  const q = await h.crash();
  let timer = false;
  let handle: ReturnType<typeof setTimeout> | undefined;
  let start = 0;
  let reads = 0;
  let yielded = false;
  let elapsed = 0;
  const read = h.space.read.bind(h.space);
  h.space.read = async (refs, opts) => {
    const lease = await read(refs, opts);
    if (lease.bytes.some((b) => b[0] === 77 && b[1] === 71 && b[2] === 82 && b[3] === 49)) {
      if (reads++ === 0) {
        start = performance.now();
        handle = setTimeout(() => {
          timer = true;
        }, 0);
      } else {
        yielded ||= timer;
        elapsed = performance.now() - start;
      }
    }
    return lease;
  };
  const peek = h.space.peek.bind(h.space);
  h.space.peek = (ref) => {
    const b = peek(ref);
    if (b?.[0] === 77 && b[1] === 71 && b[2] === 82 && b[3] === 49) {
      if (reads++ === 0) {
        start = performance.now();
        handle = setTimeout(() => {
          timer = true;
        }, 0);
      } else {
        yielded ||= timer;
        elapsed = performance.now() - start;
      }
    }
    return b;
  };
  await autocommit(q, (s) =>
    s.table(t).insert(batch(ints(Array.from({ length: 40 }, (_, k) => k))), null),
  );
  await autocommit(q, (s) => s.table(t).insert(batch(ints([40])), null));
  await q.quiesce();
  if (handle) clearTimeout(handle);
  console.log({ reads, yielded, elapsed });
  await h.close();
  expect(reads).toBeGreaterThan(100);
  expect(yielded).toBe(true);
}, 300000);
