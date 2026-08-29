/// <reference lib="webworker" />
/**
 * Benchmark worker entry point: parses requests, dispatches to the dataset, query, and
 * suite modules, and reports failures. All engine work — including SQLite Wasm and
 * PGlite — runs inside this worker; SQLite's OPFS persistence requires it.
 *
 * The Minnow-only storage and transaction checks that used to live here are library tests, not
 * an engine comparison, and now run as browser tests under packages/core.
 */
import { failure, parseRequest, success } from "@minnowdb/core/worker-protocol";
import { datasetCreate, datasetDelete, datasetList, validateCreatePayload } from "./datasets";
import { runFeatureSuite, validateFeaturePayload } from "./feature-suite";
import { runLiveSuite, validateLivePayload } from "./live-suite";
import { runQuery, validateRunQueryPayload } from "./run-query";
import { runReferenceSuite, validateReferencePayload } from "./reference-suite";
import { runWriteSuite, validateWritePayload } from "./write-suite";
import { cancelledRuns, getRequestId } from "./support";

self.addEventListener("message", (event: MessageEvent<unknown>) => {
  void runRequest(event.data);
});

async function runRequest(raw: unknown): Promise<void> {
  let requestId = "unknown";
  try {
    const request = parseRequest(raw);
    requestId = request.requestId;
    switch (request.operation) {
      case "cancelBenchmark": {
        const payload = request.payload as { requestId?: unknown };
        if (typeof payload.requestId === "string") cancelledRuns.add(payload.requestId);
        self.postMessage(success(request.requestId, { cancelled: payload.requestId }));
        return;
      }
      case "datasetList": {
        self.postMessage(success(request.requestId, await datasetList()));
        return;
      }
      case "datasetCreate": {
        const payload = validateCreatePayload(request.payload);
        try {
          self.postMessage(
            success(request.requestId, await datasetCreate(request.requestId, payload)),
          );
        } finally {
          cancelledRuns.delete(request.requestId);
        }
        return;
      }
      case "datasetDelete": {
        self.postMessage(
          success(request.requestId, await datasetDelete(request.payload as { id: string })),
        );
        return;
      }
      case "runQuery": {
        const payload = validateRunQueryPayload(request.payload);
        self.postMessage(success(request.requestId, await runQuery(payload)));
        return;
      }
      case "suiteReference": {
        const payload = validateReferencePayload(request.payload);
        try {
          self.postMessage(
            success(request.requestId, await runReferenceSuite(request.requestId, payload)),
          );
        } finally {
          cancelledRuns.delete(request.requestId);
        }
        return;
      }
      case "suiteWrite": {
        const payload = validateWritePayload(request.payload);
        try {
          self.postMessage(
            success(request.requestId, await runWriteSuite(request.requestId, payload)),
          );
        } finally {
          cancelledRuns.delete(request.requestId);
        }
        return;
      }
      case "suiteLive": {
        const payload = validateLivePayload(request.payload);
        try {
          self.postMessage(
            success(request.requestId, await runLiveSuite(request.requestId, payload)),
          );
        } finally {
          cancelledRuns.delete(request.requestId);
        }
        return;
      }
      case "suiteFeatureMatrix": {
        const payload = validateFeaturePayload(request.payload);
        try {
          self.postMessage(
            success(request.requestId, await runFeatureSuite(request.requestId, payload)),
          );
        } finally {
          cancelledRuns.delete(request.requestId);
        }
        return;
      }
      default:
        throw new Error(`Unsupported worker operation: ${request.operation}`);
    }
  } catch (error) {
    self.postMessage(failure(requestId === "unknown" ? getRequestId(raw) : requestId, error));
  }
}
