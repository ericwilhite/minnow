/// <reference lib="webworker" />
/**
 * Benchmark worker entry point: parses requests, dispatches to the dataset, query, and
 * suite modules, and reports failures. All engine work — including SQLite Wasm and
 * PGlite — runs inside this worker; SQLite's OPFS persistence requires it.
 */
import { failure, parseRequest, success } from "@minnowdb/core/worker-protocol";
import type { MemorySample } from "../memory-probe.js";
import { datasetCreate, datasetDelete, datasetList, validateCreatePayload } from "./datasets.js";
import { runFeatureSuite, validateFeaturePayload } from "./feature-suite.js";
import { runQuery, validateRunQueryPayload } from "./run-query.js";
import { runReferenceSuite, validateReferencePayload } from "./reference-suite.js";
import { runStorageBenchmark, validateConfig } from "./storage-suite.js";
import { cancelledRuns, getRequestId, resolveMemorySample } from "./support.js";

self.addEventListener("message", (event: MessageEvent<unknown>) => {
  void runRequest(event.data);
});

async function runRequest(raw: unknown): Promise<void> {
  let requestId = "unknown";
  try {
    const request = parseRequest(raw);
    requestId = request.requestId;
    switch (request.operation) {
      case "memorySample": {
        const payload = request.payload as { token?: unknown; sample?: MemorySample };
        if (typeof payload.token === "string" && payload.sample !== undefined) {
          resolveMemorySample(payload.token, payload.sample);
        }
        return;
      }
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
      case "benchmark": {
        const config = validateConfig(request.payload);
        self.postMessage(
          success(request.requestId, await runStorageBenchmark(request.requestId, config)),
        );
        return;
      }
      default:
        throw new Error(`Unsupported worker operation: ${request.operation}`);
    }
  } catch (error) {
    self.postMessage(failure(requestId === "unknown" ? getRequestId(raw) : requestId, error));
  }
}
