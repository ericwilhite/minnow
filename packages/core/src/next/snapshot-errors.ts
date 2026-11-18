/** Page-safe snapshot errors. Native codecs re-export these exact classes. */

export class SnapshotFormatError extends Error {
  readonly code = "MINNOW_SNAPSHOT_FORMAT";
  constructor(message: string) {
    super(message);
    this.name = "SnapshotFormatError";
  }
}

export class SnapshotBudgetError extends Error {
  readonly code = "MINNOW_SNAPSHOT_BUDGET";
  constructor(readonly bytes: number) {
    super(`Snapshot needs ${String(bytes)} charged bytes`);
    this.name = "SnapshotBudgetError";
  }
}

export class LegacySnapshotError extends Error {
  readonly code = "MINNOW_LEGACY_DATA";
  constructor() {
    super(
      "This snapshot was written by Minnow 0.x; Minnow 1.0 does not load it. The archive and target are unchanged. Use Minnow 0.x to read it or start fresh.",
    );
    this.name = "LegacySnapshotError";
  }
}

export class SnapshotScopeError extends Error {
  readonly code = "MINNOW_SNAPSHOT_SCOPE";
  constructor(message: string) {
    super(message);
    this.name = "SnapshotScopeError";
  }
}

export class SnapshotSourceChangedError extends Error {
  readonly code = "MINNOW_SNAPSHOT_SOURCE_CHANGED";
  constructor() {
    super("Snapshot source changed during load; retry with an immutable source");
    this.name = "SnapshotSourceChangedError";
  }
}

export class SnapshotHandleError extends Error {
  readonly code = "MINNOW_SNAPSHOT_HANDLE";
  constructor(message: string) {
    super(message);
    this.name = "SnapshotHandleError";
  }
}
