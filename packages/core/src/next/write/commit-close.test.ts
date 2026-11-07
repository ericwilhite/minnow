/** Close cannot release the inputs/grants of a commit suspended before its first log append. */
import { expect, it } from "vitest";
import { beginTask, taskClock } from "../exec/pipeline.js";
import {
  Harness,
  INT,
  TEXT,
  autocommit,
  batch,
  column,
  ints,
  texts,
  tableInput,
  rules,
} from "./test-support.js";

it("settles a commit suspended while counting before releasing its buffers", async () => {
  const realNow = taskClock.now;
  let armed = false;
  let closeRequested = false;
  let writerSettled = false;
  const state: { closing: Promise<void> | null } = { closing: null };
  const releases: boolean[] = [];
  const h = new Harness("strict", "memory", undefined, undefined, {
    onCommitBuffer: (buffer, owned) => {
      if (armed && owned && buffer.length === 4096) {
        armed = false;
        taskClock.now = () => realNow() + 1000;
        queueMicrotask(() => {
          closeRequested = true;
          state.closing = h.path.close();
        });
      }
      if (closeRequested && !owned && buffer.length >= 64 * 1024) releases.push(writerSettled);
    },
  });
  try {
    const path = await h.open();
    let table = 0;
    await autocommit(path, async (st) => {
      table =
        (
          await st.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
          })
        ).id ?? 0;
    });
    await path.quiesce();
    const tx = await path.begin();
    const st = tx.statement({ rules });
    await st
      .table(table)
      .insert(
        batch(
          ints(Array.from({ length: 2048 }, (_, k) => k)),
          texts(Array.from({ length: 2048 }, () => "x".repeat(768))),
        ),
        null,
      );
    await st.end();
    armed = true;
    let rejected = false;
    await tx.commit(null).then(
      () => {
        writerSettled = true;
      },
      () => {
        rejected = true;
        writerSettled = true;
      },
    );
    expect(rejected).toBe(true);
    expect(state.closing).not.toBeNull();
    await state.closing;
    expect(releases.length).toBeGreaterThan(0);
    expect(releases.every(Boolean), "close released commit inputs before the writer settled").toBe(
      true,
    );
    await h.close();
    expect(h.governor.usedBytes).toBe(0);
  } finally {
    taskClock.now = realNow;
    beginTask();
    await h.close();
  }
});
