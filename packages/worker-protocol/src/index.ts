export const protocolVersion = 1 as const;

export type WorkerOperation =
  | "ping"
  | "writeBlock"
  | "readBlock"
  | "publishManifest"
  | "benchmark"
  | "compareEngines"
  | "cancelBenchmark"
  | "adHocQuery"
  | "datasetStatus"
  | "wipeDatasets";

export interface WorkerRequest<T = unknown> {
  version: typeof protocolVersion;
  requestId: string;
  operation: WorkerOperation;
  payload: T;
}

export interface SuccessResponse<T = unknown> {
  version: typeof protocolVersion;
  requestId: string;
  kind: "success";
  result: T;
}

export interface FailureResponse {
  version: typeof protocolVersion;
  requestId: string;
  kind: "failure";
  error: { name: string; message: string };
}

export interface ProgressResponse<T = unknown> {
  version: typeof protocolVersion;
  requestId: string;
  kind: "progress";
  progress: T;
}

export type WorkerResponse<T = unknown> = SuccessResponse<T> | FailureResponse | ProgressResponse;

export function parseRequest(value: unknown): WorkerRequest {
  if (typeof value !== "object" || value === null) throw new TypeError("Request must be an object");
  const candidate = value as Partial<WorkerRequest>;
  if (candidate.version !== protocolVersion) throw new Error("Unsupported protocol version");
  if (typeof candidate.requestId !== "string" || candidate.requestId.length === 0) {
    throw new TypeError("Request ID must be a non-empty string");
  }
  if (!isOperation(candidate.operation)) throw new Error("Unsupported worker operation");
  return candidate as WorkerRequest;
}

export function success<T>(requestId: string, result: T): SuccessResponse<T> {
  return { version: protocolVersion, requestId, kind: "success", result };
}

export function failure(requestId: string, error: unknown): FailureResponse {
  const normalized = error instanceof Error ? error : new Error(String(error));
  return {
    version: protocolVersion,
    requestId,
    kind: "failure",
    error: { name: normalized.name, message: normalized.message },
  };
}

function isOperation(value: unknown): value is WorkerOperation {
  return [
    "ping",
    "writeBlock",
    "readBlock",
    "publishManifest",
    "benchmark",
    "compareEngines",
    "cancelBenchmark",
    "adHocQuery",
    "datasetStatus",
    "wipeDatasets",
  ].includes(String(value));
}
