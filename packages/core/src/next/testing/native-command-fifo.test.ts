/** Holder priority (GOVERNOR-WAIT-POLICY.md, governor.ts `holdsCredit`). A native root holds its
 * retained base while it grows a step; a foreign request that holds nothing, queued earlier, can
 * only be served once that base is released. Under plain FIFO neither moves (the root waited
 * behind the foreign request; the stall guard named both). Holder growth is served first: the
 * root finishes and releases, and the foreign request is then admitted. */
import { expect, it, vi } from "vitest";
import { executeNativeCommand } from "../owner/native-command.js";
import { NativeRootCredits } from "../owner/native-root-credits.js";
import { Governor, type Grant } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";

it("serves a native root's step before an earlier foreign request only its release can serve", async () => {
  const previousStall = Governor.stallAfterMs;
  // MINNOW_GOVERNOR_STALL_MS (the governor-law suites' setting) when set; 5 s otherwise.
  Governor.stallAfterMs = previousStall ?? 5_000;
  const governor = new Governor(64 << 20),
    core = new StorageCore(64);
  Object.defineProperty(core.harness, "governor", { value: governor });
  await core.open();
  // The foreign client releases as soon as it is admitted (it holds nothing while it waits).
  let foreign: Promise<number> | null = null;
  try {
    for (const sql of [
      "CREATE TABLE p(id INT PRIMARY KEY, n INT)",
      "CREATE TABLE log(id INT PRIMARY KEY, v INT)",
      "INSERT INTO p VALUES (1, 0), (2, 0), (3, 0)",
      "INSERT INTO log VALUES (1, 0), (2, 0), (3, 0)",
      "CREATE TRIGGER r AFTER UPDATE ON p BEGIN UPDATE log SET v = NEW.n WHERE id = NEW.id; END",
    ])
      await core.execute(sql);
    const lowered = core.lowerer.lower("UPDATE p SET n = 7 WHERE id <= 2", core.context());
    if (lowered.kind !== "modify") throw new Error("modify plan required");
    // What the idle engine keeps between statements; the foreign request fits beside it only.
    const baseline = governor.usedBytes;
    const want = governor.capBytes - governor.reserveBytes - baseline - (64 << 10);
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with this below.
    const source = NativeRootCredits.prototype.prepareSource;
    vi.spyOn(NativeRootCredits.prototype, "prepareSource").mockImplementation(function (
      this: NativeRootCredits,
      bytes,
      signal,
    ) {
      if (foreign === null) {
        // The root holds its retained base now; queue a request only its release can serve.
        expect(governor.usedBytes + want).toBeGreaterThan(
          governor.capBytes - governor.reserveBytes,
        );
        foreign = governor
          .admitGroup({
            label: "foreign whole",
            grants: [{ label: "foreign whole", minimumBytes: want, spillable: false }],
            signal: new AbortController().signal,
          })
          .then((grants: readonly Grant[]) => {
            const held = grants.reduce((sum, grant) => sum + grant.held, 0);
            for (const grant of grants) grant.close();
            return held;
          });
      }
      return source.call(this, bytes, signal);
    });
    const tx = await core.harness.path.begin();
    const result = await executeNativeCommand({
      transaction: tx,
      rootId: 1,
      plan: lowered.plan,
      parameters: [],
      planner: core.lowerer,
      context: { ...core.context(), catalog: tx.catalog },
      governor,
      signal: new AbortController().signal,
      execution: {
        reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
        spill: core.spill,
        queryId: 1,
        statementTime: 0n,
        sequences: null,
        sequenceSession: null,
        returning: "atExecuted",
      },
    }).catch((error: unknown) => {
      tx.rollback();
      throw error;
    });
    await (
      await tx.commit(null)
    ).durable;
    expect(result.rowCount).toBe(2);
    expect(foreign).not.toBeNull();
    // Admitted once the root released: everyone progressed, nothing refused below the cap.
    expect(await (foreign as unknown as Promise<number>)).toBe(want);
    const log = await core.execute("SELECT v FROM log ORDER BY id");
    if (!("batches" in log)) throw new Error("rows required");
  } finally {
    vi.restoreAllMocks();
    Governor.stallAfterMs = previousStall;
    await core.close();
  }
  expect(governor.usedBytes).toBe(0);
}, 120_000);
