import type { Manifest, StoragePage } from "../storage/types.js";

/** Bounded, process-local table generations proved by a contiguous durable commit history. */
export class QueryGenerations {
  #version: number | null | undefined;
  #base: number | null = null;
  readonly #tables = new Map<string, number>();
  #pending = Promise.resolve();

  constructor(
    readonly page: (after: number | null, limit: number) => Promise<StoragePage<Manifest, number>>,
  ) {}

  key(tableIds: readonly string[], version: number | null): Promise<string> {
    const task = this.#pending.then(async () => {
      // Historical snapshots and a racing older probe retain their own exact version identity.
      if (this.#version !== undefined && (version ?? -1) < (this.#version ?? -1))
        return JSON.stringify(["snapshot", version, tableIds]);
      if (this.#version === undefined) this.#reset(version);
      if (version !== this.#version) {
        const tables = new Map(this.#tables);
        let cursor = this.#version ?? null;
        let complete = false;
        for (let pageIndex = 0; pageIndex < 8 && !complete; pageIndex += 1) {
          const page = await this.page(cursor, 64);
          for (const manifest of page.records) {
            if (manifest.previousVersion !== cursor || manifest.version > (version ?? -1)) break;
            for (const id of manifest.changedTableIds) tables.set(id, manifest.version);
            cursor = manifest.version;
            if (cursor === version) {
              complete = true;
              break;
            }
          }
          if (page.nextCursor === null || tables.size > 4096) break;
        }
        if (!complete || tables.size > 4096) this.#reset(version);
        else {
          this.#tables.clear();
          for (const [id, generation] of tables) this.#tables.set(id, generation);
          this.#version = version;
        }
      }
      return JSON.stringify([
        this.#base,
        tableIds.map((id) => [id, this.#tables.get(id) ?? this.#base]),
      ]);
    });
    this.#pending = task.then(
      () => undefined,
      () => undefined,
    );
    return task;
  }

  #reset(version: number | null): void {
    this.#version = version;
    this.#base = version;
    this.#tables.clear();
  }
}
