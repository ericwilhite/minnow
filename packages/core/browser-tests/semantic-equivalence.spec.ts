import { expect } from "@playwright/test";
import { mulberry32, seedsFor } from "../src/testing/seeds.js";
import { SEMANTIC_NATIVE_CLEANUP_REACH } from "../src/testing/reach.js";
import { requireWorkerOpfs, test } from "./fixtures.js";
for (const kind of ["indexeddb", "opfs"] as const) {
  test(`${kind}: generated semantic values, domains and refusals agree across native RPC`, async ({
    page,
  }) => {
    await page.goto("/packages/core/browser/");
    if (kind === "opfs") await requireWorkerOpfs(page);
    const result = await page.evaluate(async (kind) => {
      const url = "/packages/core/browser/semantic-run.ts";
      const module = (await import(url)) as typeof import("../browser/semantic-run.js");
      return module.runSemanticEquivalence(kind);
    }, kind);
    expect(result.compared).toBe(292);
    expect(result.failures).toBe(0);
  });
}

// The dispose acknowledgement precedes OpfsBlockStore's asynchronous handle drain.
// Native locks and a checkpoint byte make termination-before-drain fail deterministically,
// even on browsers which happen to release killed-worker file handles quickly.
for (const seed of seedsFor("semantic-worker-cleanup", [47077, 47078])) {
  test(`OPFS semantic cleanup retains delayed worker shutdown, seed ${String(seed)}`, async ({
    page,
  }) => {
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
    const random = mulberry32(seed);
    const ticks = [0, 8, ...Array.from({ length: 14 }, () => Math.floor(random() * 9))];
    const completed = await page.evaluate(async (ticks) => {
      const url = "/packages/core/browser/semantic-cleanup.ts";
      const { closeSemanticResources } = (await import(
        url
      )) as typeof import("../browser/semantic-cleanup.js");
      let histories = 0;
      const reached = new Set<string>();
      for (const count of ticks) {
        reached.add(count === 0 ? "zero drain turns" : "multiple drain turns");
        const name = `semantic-drain-${crypto.randomUUID()}`;
        const lock = `semantic-drain-lock:${name}`;
        const root = await navigator.storage.getDirectory();
        const script = `
          let resume, closeAck;
          const gate = new Promise(resolve => { resume = resolve; });
          self.onmessage = async ({data}) => {
            if(data === "resume") { resume(); return; }
            if(data === "close") { closeAck(); return; }
          };
          const closing = new Promise(resolve => { closeAck = resolve; });
          navigator.locks.request(${JSON.stringify(lock)}, {mode:"shared"}, async () => {
            const root = await navigator.storage.getDirectory();
            const dir = await root.getDirectoryHandle(${JSON.stringify(name)}, {create:true});
            const file = await dir.getFileHandle("checkpoint", {create:true});
            const handle = await file.createSyncAccessHandle();
            self.postMessage("ready");
            await closing;
            self.postMessage("closing");
            await gate;
            for(let i=0;i<${String(count)};i++) await Promise.resolve();
            handle.write(Uint8Array.of(37), {at:0});
            handle.flush();
            handle.close();
          }).catch(error => self.postMessage({error:String(error)}));
        `;
        const url = URL.createObjectURL(new Blob([script], { type: "text/javascript" }));
        const worker = new Worker(url);
        const messages = new Map<string, () => void>();
        const waiting = (message: string) =>
          new Promise<void>((resolve) => {
            messages.set(message, resolve);
          });
        const ready = waiting("ready");
        let workerError: string | undefined;
        worker.onmessage = ({ data }: MessageEvent<string | { error: string }>) => {
          if (typeof data === "string") messages.get(data)?.();
          else workerError = data.error;
        };
        const state = { deleted: false };
        try {
          await ready;
          const failures = await closeSemanticResources(
            {
              terminate() {
                if (state.deleted) reached.add("worker terminated after deletion");
                worker.terminate();
              },
            },
            async () => {
              const ack = waiting("closing");
              worker.postMessage("close");
              await ack;
              reached.add("close acknowledged before drain");
            },
            async () => undefined,
            async () => {
              // Deletion begins while shutdown is pending. It must not kill that continuation.
              worker.postMessage("resume");
              await navigator.locks.request(lock, { mode: "exclusive" }, async () => {
                const dir = await root.getDirectoryHandle(name);
                const checkpoint = await (await dir.getFileHandle("checkpoint")).getFile();
                const bytes = new Uint8Array(await checkpoint.arrayBuffer());
                if (bytes.length !== 1 || bytes[0] !== 37)
                  throw new Error("Worker terminated before the checkpoint drained");
                reached.add("checkpoint drained before delete");
                await root.removeEntry(name, { recursive: true });
                reached.add("files deleted");
                state.deleted = true;
              });
            },
          );
          if (workerError !== undefined) throw new Error(workerError);
          if (failures.length) throw new AggregateError(failures, failures.map(String).join("; "));
          if (!state.deleted) throw new Error("Semantic cleanup did not delete the drained files");
          histories += 1;
        } finally {
          worker.terminate();
          URL.revokeObjectURL(url);
          // Failure already propagates above; discard its raw probe files when possible.
          if (!state.deleted)
            await root.removeEntry(name, { recursive: true }).catch(() => undefined);
        }
      }
      return { histories, reached: [...reached].sort() };
    }, ticks);
    expect(completed.histories).toBe(ticks.length);
    expect(completed.reached).toEqual([...SEMANTIC_NATIVE_CLEANUP_REACH].sort());
  });
}
