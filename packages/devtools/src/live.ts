import type { QueryResult } from "@minnowdb/core";
import type { DevtoolsTarget } from "./target.js";

/**
 * The slice of the live-query API the panel follows a statement with. `MinnowDatabase` and
 * `MinnowDatabaseClient` both expose it as `liveQueries()`; the set is created the first time
 * something is followed and closed with the panel.
 */
export interface LiveTarget extends DevtoolsTarget {
  liveQueries(): LiveSet;
}

export interface LiveSet {
  subscribe(
    sql: string,
    options: { onChange(result: QueryResult): void; onError?(error: unknown): void },
  ): Promise<{ close(): void }>;
  close(): void;
}

export function isLiveTarget(target: DevtoolsTarget): target is LiveTarget {
  return typeof (target as { liveQueries?: unknown }).liveQueries === "function";
}

export interface Follower {
  /** Follows one statement; whatever was followed before is dropped first. */
  follow(
    sql: string,
    onRows: (result: QueryResult) => void,
    onError: (error: unknown) => void,
  ): void;
  /** Stops following, keeping the set for the next `follow`. */
  stop(): void;
  /** Closes the set. */
  destroy(): void;
}

/**
 * Follows one statement at a time. A grid that is live re-renders from `onRows` whenever the
 * database commits something the statement can see; the engine decides what that is, and skips
 * the re-run when a commit provably cannot change the result.
 */
export function createFollower(target: LiveTarget): Follower {
  let set: LiveSet | undefined;
  let current: { close(): void } | undefined;
  /** Rising token, so a subscription that resolves after a later `follow` closes itself. */
  let generation = 0;

  function stop(): void {
    generation += 1;
    current?.close();
    current = undefined;
  }

  return {
    follow: (sql, onRows, onError) => {
      stop();
      const token = generation;
      set ??= target.liveQueries();
      set.subscribe(sql, { onChange: onRows, onError }).then(
        (subscription) => {
          if (token === generation) current = subscription;
          else subscription.close();
        },
        (error: unknown) => {
          onError(error);
        },
      );
    },
    stop,
    destroy: () => {
      stop();
      set?.close();
      set = undefined;
    },
  };
}
