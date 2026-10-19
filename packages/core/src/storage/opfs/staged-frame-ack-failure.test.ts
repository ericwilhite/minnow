/**
 * A large commit's frame is staged in continuation pieces and completed by one ordinary frame.
 * Once that completing frame is written and flushed, the log holds the commit whole; whatever
 * fails after it — the acknowledgement slot's flush, most plausibly — must not take the frame
 * back. The acknowledgement slot's bytes may already have landed, so a rewound log would be
 * shorter than its acknowledged boundary, and every later open would refuse the database.
 *
 * The first test is the exact failure; the sweep crashes at every flush of such a commit, under
 * tab death (every written byte survives) and power loss (only flushed bytes survive), and asks
 * for the same two things each time: the database opens, and the commit is all-or-nothing.
 */
import { afterEach, describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsUncertainOutcomeError } from "../types.js";
import { walStagingTestHooks } from "./leader.js";
import { PowerLossModel } from "./power-loss-model.js";
import {
  LEADER_T1,
  errorText,
  keyTokens,
  leaderHandles,
  leaderTransaction,
  leaderUniqueKeys,
  stageKeyedTransaction,
  walFrameRanges,
} from "./leader-test-helpers.js";

const DEFAULT_PIECE_BYTES = walStagingTestHooks.pieceBytes;
afterEach(() => {
  walStagingTestHooks.pieceBytes = DEFAULT_PIECE_BYTES;
});

function largeCommit(transactionId: string, revision: number) {
  return {
    transactionId,
    expectedTransactionRevision: revision,
    expectedManifestVersion: null,
    levelZeroSegmentLimits: [{ tableId: "keyed", limit: 4096 }],
    uniqueKeyChanges: [{ tableId: "keyed", keyTokens: keyTokens(20_000), requireAbsent: true }],
    committedAt: LEADER_T1,
  };
}

describe("acknowledgement flush fails after a staged commit frame is durable", () => {
  for (const staged of [true, false]) {
    it(`${staged ? "staged" : "whole"} frame: the frame stays and the reopened log holds the commit`, async () => {
      walStagingTestHooks.pieceBytes = staged ? 4096 : DEFAULT_PIECE_BYTES;
      const shim = new MemoryOpfs();
      const h = await leaderHandles(shim, `ack-flush-${staged ? "staged" : "whole"}`);
      let failAck = false;
      shim.setWriteFault((path, phase) => {
        if (failAck && phase === "flush" && path.endsWith("wal-acknowledgements")) {
          failAck = false;
          throw new DOMException("acknowledgement flush refused", "InvalidStateError");
        }
      });
      const leader = await h.recover({ strict: true });
      const { revision } = await stageKeyedTransaction(leader, "tx");
      const walBefore = shim.readFileBytes(h.walPath)?.byteLength ?? 0;
      failAck = true;
      await expect(
        leader._loggedGenerated("commitTransaction", [largeCommit("tx", revision)]),
      ).rejects.toBeInstanceOf(OpfsUncertainOutcomeError);
      expect(failAck).toBe(false);
      const image = shim.readFileBytes(h.walPath) ?? new Uint8Array();
      // The log kept the frame (its pieces too, when staged).
      expect(image.byteLength).toBeGreaterThan(walBefore);
      const magics = walFrameRanges(image).map((frame) => frame.magic);
      expect(magics.filter((magic) => magic === "MNWC").length > 10).toBe(staged);
      // The poisoned leader reloads from its handles and finds the commit.
      expect((await leaderTransaction(leader, "tx"))?.status).toBe("committed");
      leader.crash();
      const reopened = await h.recover({ strict: true });
      expect((await leaderTransaction(reopened, "tx"))?.status).toBe("committed");
      expect(await leaderUniqueKeys(reopened, "keyed", ["number:0", "number:19999"])).toEqual([
        "number:0",
        "number:19999",
      ]);
      reopened.crash();
    });
  }
});

async function crashAtFlush(crashAt: number, powerLoss: boolean) {
  walStagingTestHooks.pieceBytes = 16 * 1024;
  const shim = new MemoryOpfs();
  const h = await leaderHandles(shim, `sweep-${String(crashAt)}-${powerLoss ? "p" : "d"}`);
  let flushes = 0;
  let armed = false;
  let crashedAt: string | undefined;
  const model = new PowerLossModel(shim, (path, phase) => {
    if (!armed || phase !== "flush") return;
    flushes += 1;
    if (flushes === crashAt) {
      crashedAt = path.split("/").at(-1);
      throw new DOMException("crash", "InvalidStateError");
    }
  });
  const leader = await h.recover({ strict: true });
  const { revision } = await stageKeyedTransaction(leader, "tx");
  armed = true;
  let outcome = "ok";
  try {
    await leader._loggedGenerated("commitTransaction", [largeCommit("tx", revision)]);
  } catch (error) {
    outcome = error instanceof Error ? error.name : String(error);
  }
  armed = false;
  const totalFlushes = flushes;
  leader.crash();
  if (powerLoss) model.powerLoss();
  try {
    const reopened = await h.recover({ strict: true });
    const status = (await leaderTransaction(reopened, "tx"))?.status;
    const keys = await leaderUniqueKeys(reopened, "keyed", ["number:0", "number:19999"]);
    reopened.crash();
    const allOrNothing =
      (status === "committed" && keys.length === 2) || (status === "active" && keys.length === 0);
    return { crashAt, crashedAt, totalFlushes, outcome, status, keys: keys.length, allOrNothing };
  } catch (error) {
    return { crashAt, crashedAt, totalFlushes, outcome, refused: errorText(error) };
  }
}

describe("crash at every flush of a staged strict commit", () => {
  for (const powerLoss of [false, true]) {
    it(`${powerLoss ? "power loss" : "tab death"}: the database opens and the commit is all-or-nothing`, async () => {
      const dry = await crashAtFlush(Number.POSITIVE_INFINITY, powerLoss);
      expect(dry.totalFlushes).toBeGreaterThan(10);
      const rows = [];
      for (let at = 1; at <= dry.totalFlushes; at += 1)
        rows.push(await crashAtFlush(at, powerLoss));
      expect(rows.every((row) => row.crashedAt !== undefined)).toBe(true);
      expect(rows.filter((row) => "refused" in row || !row.allOrNothing)).toEqual([]);
      // The last flush is the acknowledgement's: the frame before it is durable, so it commits.
      expect(rows.at(-1)).toMatchObject({ crashedAt: "wal-acknowledgements", status: "committed" });
    }, 120_000);
  }
});
