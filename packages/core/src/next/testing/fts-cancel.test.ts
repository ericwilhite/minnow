/** A suspended FTS borrower fences source buffers as well as its own spill page. */
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, FTS_CANCEL_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";
for (const seed of seedsFor("next-fts-cancel", [0xb7ff, 0xb800]))
  it(`drains a gated append before retiring the source borrow (${String(seed)})`, async () => {
    const core = await new StorageCore(64).open(),
      governor = new Governor(32 << 20),
      random = mulberry32(seed);
    const reach = createReach(`FTS cancel ${String(seed)}`, FTS_CANCEL_REACH);
    reach.beginHistory();
    const sourceGrants: Grant[] = [];
    const admit = governor.admitGroup.bind(governor);
    const controlled = new Proxy(governor, {
      get(target, key) {
        if (key === "tryAdmit")
          return (options: Parameters<Governor["tryAdmit"]>[0]) =>
            options.label.endsWith("/fixed-buffers") ? null : target.tryAdmit(options);
        if (key === "admitGroup")
          return async (options: Parameters<Governor["admitGroup"]>[0]) => {
            const grants = await admit(options);
            sourceGrants.push(
              ...grants.filter((grant) => /\/scan#\d+\/minimum$/u.test(grant.label)),
            );
            return grants;
          };
        const value: unknown = Reflect.get(target, key, target);
        return typeof value === "function"
          ? (value as (...args: unknown[]) => unknown).bind(target)
          : value;
      },
    });
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>((resolve) => {
        enter = resolve;
      }),
      gate = new Promise<void>((resolve) => {
        release = resolve;
      });
    const create = core.spill.create.bind(core.spill);
    core.spill.create = async (queryId) => {
      const file = await create(queryId);
      return new Proxy(file, {
        get(target, key) {
          if (key === "append")
            return async (page: Uint8Array) => {
              enter();
              await gate;
              return target.append(page);
            };
          const value: unknown = Reflect.get(target, key, target);
          return typeof value === "function"
            ? (value as (...args: unknown[]) => unknown).bind(target)
            : value;
        },
      });
    };
    let run: ReturnType<StorageCore["start"]> | undefined, consume: Promise<void> | undefined;
    try {
      await core.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY,a TEXT)");
      await core.writeBatch(
        "insertBatch",
        "docs",
        Array.from({ length: 8192 }, (_, id) => ({
          id,
          a: (random() < 0.5 ? "alpha " : "beta ") + "x".repeat(4096),
        })),
      );
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      const prepared = core.prepare("SELECT id,BM25(a) AGAINST 'alpha' s FROM docs");
      const pin = core.harness.path.pin();
      const execution = prepared.execute({
        snapshot: pin,
        statement: null,
        parameters: [],
        statementTime: 0n,
        governor: controlled,
        spill: core.spill,
        queryId: seed,
        reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
        sequences: null,
        sequenceSession: null,
        returning: "atExecuted",
      });
      run = { execution, release: () => pin.release() };
      consume = (async () => {
        for (;;) {
          const frame = await execution.results.next();
          if (!frame) break;
          frame.release();
        }
      })();
      void consume.catch(() => undefined);
      await entered;
      reach.reach("append suspended");
      const used = () => sourceGrants.reduce((n, grant) => n + grant.allocated, 0),
        before = used();
      expect(before).toBeGreaterThan(0);
      execution.cancel(new Error("generated FTS cancel"));
      let settled = false;
      const end = execution.executed.finally(() => {
        settled = true;
      });
      void end.catch(() => undefined);
      await Promise.resolve();
      await Promise.resolve();
      expect(settled).toBe(false);
      expect(used()).toBe(before);
      reach.reach("source still charged");
      reach.reach("closed fenced");
      release();
      await Promise.allSettled([end, execution.delivered, consume]);
    } finally {
      release();
      run?.execution.cancel(new Error("cleanup"));
      if (run) await Promise.allSettled([run.execution.executed, run.execution.delivered]);
      if (consume) await consume.catch(() => undefined);
      run?.release();
      await core.close();
    }
    expect(governor.usedBytes).toBe(0);
    expect(core.spill.bytesInUse).toBe(0);
    reach.reach("drained");
    reach.assertReached();
  }, 60000);
