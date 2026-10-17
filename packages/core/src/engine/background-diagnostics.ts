import {
  CompactionJobConflictError,
  OpfsUncertainOutcomeError,
  PostingBuildConflictError,
  SchemaConflictError,
  TransactionRecordConflictError,
  UniqueIndexCoverageError,
} from "../storage/types.js";
import { TransactionClosedError } from "../transactions/index.js";

/**
 * A compaction step that lost its job to another connection: the shared job record or the
 * transaction every driver of it stages into had moved by the time this step wrote it, or that
 * transaction was already closed. Revisions only move when a writer succeeds, so the job made
 * progress -- just not here. The controller retries after its backoff and re-reads the job.
 */
export function isLostCompactionRace(error: unknown): boolean {
  return (
    error instanceof CompactionJobConflictError ||
    error instanceof TransactionRecordConflictError ||
    error instanceof TransactionClosedError
  );
}

/** What each store says when a build session cannot begin because another one is live. */
const LIVE_POSTING_BUILD_REASONS = new Set([
  "another live build owns the column",
  "another live build exists",
  "owned by another caller",
]);

/**
 * A posting build session refused at its start because another connection's build of the same
 * index holds the column's live session. That build is doing the work; it is not a failure of
 * this connection. Only meaningful for the begin call — later steps that lose their session are
 * reported.
 */
export function isLivePostingBuildRefusal(error: unknown): boolean {
  return error instanceof PostingBuildConflictError && LIVE_POSTING_BUILD_REASONS.has(error.reason);
}

/**
 * A maintenance mutation whose OPFS leader vanished before acknowledging it — another tab closed
 * or crashed mid-step — or a fold that kept losing to schema changes (`CREATE INDEX`, `ALTER`,
 * a `UNIQUE` index that began enforcing after the fold read its table) through its immediate
 * retries. Each maintenance job is durable and revision-guarded, and its
 * next run re-reads it and plans against the current schema, so the scheduled retry reconciles
 * the outcome; reporting it would only tell the app that a tab went away or that DDL ran. A
 * wedged store (`StorageUnresponsiveError`) and every other failure are still reported.
 */
export function isReconciledByMaintenanceRetry(error: unknown): boolean {
  return (
    error instanceof OpfsUncertainOutcomeError ||
    error instanceof SchemaConflictError ||
    error instanceof UniqueIndexCoverageError
  );
}

/** One bounded owner of background evidence. Observers receive the original error; history
 * keeps only bounded text and timestamps so error graphs cannot retain database resources. */
function boundedText(read: () => unknown, fallback: string): string {
  try {
    return String(read()).slice(0, 1024);
  } catch {
    return fallback;
  }
}
export class BackgroundDiagnostics {
  readonly #history: Array<{
    sequence: number;
    context: string;
    name: string;
    message: string;
    at: number;
  }> = [];
  #sequence = 0;
  constructor(
    private readonly now: () => number,
    private readonly observer: ((error: unknown, context: string) => void) | undefined,
  ) {}

  report(error: unknown, context: string): void {
    this.#sequence += 1;
    this.#history.push({
      sequence: this.#sequence,
      context: context.slice(0, 1024),
      ...backgroundErrorSummary(error),
      at: this.now(),
    });
    if (this.#history.length > 32) this.#history.shift();
    if (this.observer !== undefined) {
      try {
        this.observer(error, context);
        return;
      } catch (hookError) {
        console.error("[minnowdb] onBackgroundError callback failed:", hookError);
      }
    }
    console.error(`[minnowdb] background failure (${context}):`, error);
  }

  snapshot() {
    return {
      backgroundFailureCount: this.#sequence,
      backgroundErrors: this.#history.map((entry) => ({ ...entry, at: new Date(entry.at) })),
    };
  }
}

export function backgroundErrorSummary(error: unknown) {
  return {
    name: boundedText(() => (error instanceof Error ? error.name : "Error"), "Error"),
    message: boundedText(
      () => (error instanceof Error ? error.message : error),
      "[unprintable error]",
    ),
  };
}
