import type { StoreSession } from "../storage/contract.js";
import { scanLog } from "../format/log-frame.js";
import { decodeBody } from "../write/body.js";
import { expect } from "vitest";
import { ftsDef } from "../access/fts-def.js";
import { ftsSemantics } from "../fts-config.js";
import { ftsValue, FTS_WORKSPACE_BYTES } from "../exec/fts-value.js";
import { Harness, autocommit, column, tableInput, TEXT } from "../write/test-support.js";
import { readFtsJournal, ftsPreparedEntries, type FtsJournalEntry } from "../format/fts-journal.js";
import type { BlobRef } from "../storage/extents.js";
const sessions = new WeakMap<Harness, StoreSession>();

export function ftsBudget() {
  return {
    held: 0,
    peak: 0,
    allocate(n: number) {
      this.held += n;
      this.peak = Math.max(this.peak, this.held);
      return true;
    },
    free(n: number) {
      this.held -= n;
      expect(this.held).toBeGreaterThanOrEqual(0);
    },
  };
}
/** Open/capture the real store session with the stage4 renderer; SQL/DML fixtures may
 * create their own table/key shapes before declaring an FTS definition. */
export async function openFtsHarness(h: Harness, render = true) {
  const open = h.backend.open.bind(h.backend);
  h.backend.open = async (...args) => {
    const session = await open(...args);
    sessions.set(h, session);
    return session;
  };
  const path = await h.open();
  if (render)
    path.installFtsRenderer({
      ...ftsSemantics(),
      workspaceBytes: FTS_WORKSPACE_BYTES,
      render: ftsValue,
    });
  return path;
}
export async function setupFts(h: Harness, render = true) {
  const path = await openFtsHarness(h, render);
  await autocommit(path, async (s) => {
    await s.changeCatalog({
      kind: "createTable",
      table: tableInput("docs", [column("body", TEXT)], { primaryKey: null }),
    });
    const { id: _id, ...fts } = ftsDef(1, 1, 1);
    void _id;
    await s.changeCatalog({ kind: "createFts", fts });
  });
  return path;
}
export async function ftsParts(h: Harness) {
  const b = ftsBudget(),
    root = h.path.state().root;
  let ref: BlobRef | null = root.fts.staged,
    lsn: number | null = null;
  if (!ref && root.fts.checkpoint) {
    const held = await readFtsJournal(root.fts.checkpoint, b, h.space);
    try {
      const p = held.value;
      expect(p.kind).toBe("checkpoint");
      if (p.kind !== "checkpoint") throw new Error("Missing checkpoint");
      ref = p.staged;
      lsn = p.lsn;
    } finally {
      held.release();
    }
  }
  const latest = await maybeLastFtsCommit(h);
  const inline = latest?.body.effects.filter((effect) => effect.kind === "ftsMutation") ?? [];
  if (latest && inline.length > 0 && (lsn === null || latest.lastLsn > lsn)) {
    const entries: FtsJournalEntry[] = inline.map((effect) => ({
      kind: "keyed",
      tableId: effect.tableId,
      schemaVersion: effect.schemaVersion,
      ftsId: 0,
      columnId: 0,
      generation: 0,
      mutationEpoch: latest.lastLsn,
    }));
    if (ref && latest.body.effects.some((effect) => effect.kind === "fts"))
      for await (const e of ftsPreparedEntries(ref, b, h.space)) entries.push(structuredClone(e));
    expect(b.held).toBe(0);
    return { entries, lsn: latest.lastLsn };
  }
  if (!ref) throw new Error("Missing native FTS preparation");
  const entries: FtsJournalEntry[] = [];
  for await (const e of ftsPreparedEntries(ref, b, h.space)) entries.push(structuredClone(e));
  expect(b.held).toBe(0);
  return { entries, lsn };
}

async function maybeLastFtsCommit(h: Harness) {
  const session = sessions.get(h);
  if (!session) throw new Error("Missing FTS fixture session");
  const scan = scanLog(
    await session.readLog(0, session.logLength()),
    await session.readLogHeaders(),
    h.path.state().root.log,
  );
  const last = scan.frames.at(-1);
  if (!last) return null;
  const start = last.lsn - last.partIndex,
    frames = scan.frames.filter((f) => f.lsn - f.partIndex === start);
  const bytes = new Uint8Array(frames.reduce((n, f) => n + f.body.length, 0));
  let at = 0;
  for (const f of frames) {
    bytes.set(f.body, at);
    at += f.body.length;
  }
  return { body: decodeBody(bytes), firstLsn: start, lastLsn: last.lsn, parts: frames.length };
}

export async function lastFtsCommit(h: Harness) {
  const result = await maybeLastFtsCommit(h);
  if (!result) throw new Error("Missing commit frame");
  return result;
}
