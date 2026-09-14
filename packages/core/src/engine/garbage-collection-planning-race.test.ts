import { expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabase } from "./database.js";

class SegmentDisappearsDuringPlanningStore extends MemoryBlockStore {
  writer: MinnowDatabase | undefined;
  armed = false;
  insertedAfterSnapshot = false;
  droppedBeforeNomination = false;
  provenanceRefusals = 0;

  override async listSegmentPage(
    afterId: Parameters<MemoryBlockStore["listSegmentPage"]>[0],
    limit: Parameters<MemoryBlockStore["listSegmentPage"]>[1],
  ) {
    if (this.armed && !this.insertedAfterSnapshot) {
      this.insertedAfterSnapshot = true;
      await this.writer?.execute("INSERT INTO items VALUES (1, 10)");
    }
    return super.listSegmentPage(afterId, limit);
  }

  override async updateGarbageCollectionPlanning(
    input: Parameters<MemoryBlockStore["updateGarbageCollectionPlanning"]>[0],
  ) {
    if (
      this.armed &&
      !this.droppedBeforeNomination &&
      (input.candidateSegmentIds?.length ?? 0) > 0
    ) {
      this.droppedBeforeNomination = true;
      await this.writer?.execute("DROP TABLE items");
    }
    try {
      return await super.updateGarbageCollectionPlanning(input);
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("GC segment has no provenance:")) {
        this.provenanceRefusals += 1;
      }
      throw error;
    }
  }
}

it("retries GC planning when DROP deletes a segment between discovery and nomination", async () => {
  const store = new SegmentDisappearsDuringPlanningStore();
  const options = { autoCollect: false, autoCompact: false } as const;
  const collector = new MinnowDatabase(store, options);
  const writer = new MinnowDatabase(store, options);
  store.writer = writer;
  try {
    await collector.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, amount INTEGER)");
    store.armed = true;

    const result = await collector.collectGarbage({
      maxPlanningItems: 32,
      maxItemsPerStep: 32,
    });

    expect(store.insertedAfterSnapshot).toBe(true);
    expect(store.droppedBeforeNomination).toBe(true);
    expect(store.provenanceRefusals).toBe(1);
    expect(result.missingSegmentCount).toBe(0);
    expect(await store.listTables()).toEqual([]);
    expect(await store.listGarbageCollectionJobs()).toEqual([
      expect.objectContaining({ state: "completed" }),
    ]);
  } finally {
    await Promise.allSettled([collector.close(), writer.close()]);
    store.close();
  }
});
