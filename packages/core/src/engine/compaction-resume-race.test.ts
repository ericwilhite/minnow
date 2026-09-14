import { expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { TransactionClosedError } from "../transactions/index.js";
import { MinnowDatabase } from "./database.js";

class ResumeRenewalBarrierStore extends MemoryBlockStore {
  #blockedTransactionId: string | undefined;
  #blocked = false;
  #release: (() => void) | undefined;
  #reached: (() => void) | undefined;
  readonly renewalReached = new Promise<void>((resolve) => {
    this.#reached = resolve;
  });
  readonly #renewalRelease = new Promise<void>((resolve) => {
    this.#release = resolve;
  });

  blockNextRenewal(transactionId: string): void {
    this.#blockedTransactionId = transactionId;
  }

  releaseRenewal(): void {
    this.#release?.();
  }

  override async renewTransaction(
    input: Parameters<MemoryBlockStore["renewTransaction"]>[0],
  ): ReturnType<MemoryBlockStore["renewTransaction"]> {
    if (input.transactionId === this.#blockedTransactionId && !this.#blocked) {
      this.#blocked = true;
      this.#reached?.();
      await this.#renewalRelease;
    }
    return super.renewTransaction(input);
  }
}

class UnchangedOwnerRefusalStore extends MemoryBlockStore {
  #refusedTransactionId: string | undefined;
  renewalAttempts = 0;

  refuseRenewal(transactionId: string): void {
    this.#refusedTransactionId = transactionId;
  }

  override async renewTransaction(
    input: Parameters<MemoryBlockStore["renewTransaction"]>[0],
  ): ReturnType<MemoryBlockStore["renewTransaction"]> {
    if (input.transactionId === this.#refusedTransactionId) {
      this.renewalAttempts += 1;
      if (this.renewalAttempts > 1) {
        throw new Error("Compaction retried an unchanged active owner");
      }
      return false;
    }
    return super.renewTransaction(input);
  }
}

it("reconciles when another connection commits a compaction during owner resume", async () => {
  const store = new ResumeRenewalBarrierStore();
  const winner = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  const contender = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await winner.execute("CREATE TABLE events(value INTEGER)");
    for (let value = 1; value <= 4; value += 1) {
      await winner.insert("events", { value });
    }

    const partial = await winner.compactTableStep("events", {
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    expect(partial.result).toBeNull();
    if (partial.jobId === null) throw new Error("Expected a persisted compaction job");
    const running = await store.getCompactionJob(partial.jobId);
    if (running?.transactionId === null || running?.transactionId === undefined) {
      throw new Error("Expected a linked compaction transaction");
    }
    store.blockNextRenewal(running.transactionId);

    const racedResume = contender.resumeCompactionJob(partial.jobId, { maxBlocks: 1 });
    await store.renewalReached;
    const published = await winner.resumeCompactionJob(partial.jobId, { maxBlocks: 64 });
    expect(published).toMatchObject({
      jobId: partial.jobId,
      state: "published",
      result: { compacted: true, rowCount: 4 },
    });
    if (published.result === null) throw new Error("Expected the competing fold to publish");
    store.releaseRenewal();

    const reconciled = await racedResume;
    expect(reconciled).toMatchObject({
      jobId: partial.jobId,
      state: "published",
      result: { compacted: true, rowCount: 4, version: published.result.version },
    });
    expect(await contender.readTable("events")).toEqual([
      { value: 1 },
      { value: 2 },
      { value: 3 },
      { value: 4 },
    ]);
    expect(await store.getTransaction(running.transactionId)).toMatchObject({
      status: "committed",
      committedVersion: published.result.version,
    });
  } finally {
    store.releaseRenewal();
    await Promise.all([winner.close(), contender.close()]);
    store.close();
  }
});

it("propagates lost ownership when neither the compaction nor its active owner changed", async () => {
  const store = new UnchangedOwnerRefusalStore();
  const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await database.execute("CREATE TABLE events(value INTEGER)");
    for (let value = 1; value <= 4; value += 1) {
      await database.insert("events", { value });
    }
    const partial = await database.compactTableStep("events", {
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (partial.jobId === null) throw new Error("Expected a persisted compaction job");
    const running = await store.getCompactionJob(partial.jobId);
    if (running?.transactionId === null || running?.transactionId === undefined) {
      throw new Error("Expected a linked compaction transaction");
    }
    store.refuseRenewal(running.transactionId);

    await expect(database.resumeCompactionJob(partial.jobId)).rejects.toBeInstanceOf(
      TransactionClosedError,
    );
    expect(store.renewalAttempts).toBe(1);
    expect(await store.getCompactionJob(partial.jobId)).toEqual(running);
    expect(await store.getTransaction(running.transactionId)).toMatchObject({ status: "active" });
  } finally {
    await database.close();
    store.close();
  }
});
