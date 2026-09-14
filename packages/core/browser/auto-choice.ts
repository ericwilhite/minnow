import { autoStoreTestHooks, forgetStoreChoice, openAutoStore } from "../dist/engine/auto-store.js";

/** Abort a real choice transaction after its add request succeeds, before it can commit. */
export async function abortChoiceAfterRequestSuccess(): Promise<{
  requestSucceeded: boolean;
  transactionAborted: boolean;
  opened: boolean;
  errorName: string;
}> {
  const name = `choice-abort-${crypto.randomUUID()}`;
  // Preserve the native method for restoration; the wrapper calls it with its original receiver.
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const add = IDBObjectStore.prototype.add;
  let requestSucceeded = false;
  let transactionAborted = false;
  let opened = false;
  let errorName = "resolved";
  autoStoreTestHooks.opfsAvailable = async () => true;
  IDBObjectStore.prototype.add = function (
    value: unknown,
    key?: IDBValidKey,
  ): IDBRequest<IDBValidKey> {
    const request = key === undefined ? add.call(this, value) : add.call(this, value, key);
    if (this.name === "choices" && key === name) {
      const transaction = this.transaction;
      transaction.addEventListener("abort", () => {
        transactionAborted = true;
      });
      request.addEventListener("success", () => {
        requestSucceeded = true;
        transaction.abort();
      });
    }
    return request;
  };
  try {
    try {
      await openAutoStore(
        name,
        async () => {
          opened = true;
          return {};
        },
        {
          opfsDatabaseExists: async () => false,
        },
      );
    } catch (error) {
      errorName = error instanceof Error ? error.name : String(error);
    }
    // The old code can resolve before the abort event. Wait for the native event loop to drain
    // by opening the same catalog and deleting the key, which queues behind the aborted write.
    await forgetStoreChoice(name);
    return { requestSucceeded, transactionAborted, opened, errorName };
  } finally {
    IDBObjectStore.prototype.add = add;
    delete autoStoreTestHooks.opfsAvailable;
  }
}
