import { decodeResult, type ResultInput } from "../owner/result.js";
import { StreamDictionaries } from "../owner/frame.js";
/** Synthetic port-boundary result source for receiving admission laws, without native sizing. */
import { ProtocolClient, type ClientLiveOptions, type ClientLiveHandle } from "../owner/client.js";
export class MetadataClient extends ProtocolClient {
  constructor(readonly result: ResultInput) {
    super();
  }
  override subscribeLive(
    _sql: string,
    _params: Parameters<ProtocolClient["subscribeLive"]>[1],
    options: ClientLiveOptions,
  ): ClientLiveHandle {
    let closed = false;
    const deliver = () => {
      options.onLoading?.();
      queueMicrotask(() => {
        if (closed) return;
        void Promise.resolve(
          options.onResult(
            {
              ...this.result,
              dictionaries: new StreamDictionaries(),
              rows: () => decodeResult(this.result).tuples(),
            },
            1,
            1,
            { reset: true, keyChanges: [] },
          ),
        ).catch((error: unknown) =>
          options.onError(error instanceof Error ? error : new Error(String(error))),
        );
      });
    };
    deliver();
    return {
      refresh: deliver,
      close() {
        closed = true;
      },
    };
  }
}
