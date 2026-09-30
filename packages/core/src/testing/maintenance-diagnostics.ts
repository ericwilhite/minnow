/** Test-only classification of reported, unpublished maintenance contention. Every event stays
 * in the evidence. Message text never excuses an unknown error or corruption. */
export function isMaintenanceContention(value: unknown): boolean {
  if (typeof value !== "object" || value === null) return false;
  const event = value as Record<string, unknown>;
  if (
    event.source !== "worker" ||
    event.kind !== "maintenance" ||
    typeof event.context !== "string"
  )
    return false;
  if (typeof event.error !== "object" || event.error === null) return false;
  const error = event.error as Record<string, unknown>;
  if (typeof error.props !== "object" || error.props === null) return false;
  const props = error.props as Record<string, unknown>;
  const indexContext =
    /^(secondary index build|full-text index build|posting delta fold|posting build cleanup) for /u.test(
      event.context,
    );
  const nonempty = (value: unknown): boolean => typeof value === "string" && value.length > 0;
  if (error.name === "PostingBuildConflictError") {
    return (
      indexContext &&
      nonempty(props.buildId) &&
      nonempty(props.ownerId) &&
      [
        "another live build owns the column",
        "another live build exists",
        "session is missing or owned by another caller",
        "ownership changed or expired",
        "owned by another caller",
        "ownership is absent or expired",
        "Postings base build changed",
        "index is no longer active",
      ].includes(typeof props.reason === "string" ? props.reason : "")
    );
  }
  const identity =
    error.name === "CompactionJobConflictError"
      ? "jobId"
      : error.name === "TransactionRecordConflictError"
        ? "transactionId"
        : error.name === "TableRecordConflictError"
          ? "tableId"
          : undefined;
  if (identity === undefined || !nonempty(props[identity])) return false;
  if (
    error.name === "TableRecordConflictError"
      ? !indexContext
      : !event.context.startsWith("automatic compaction for ")
  )
    return false;
  return (
    typeof props.expectedRevision === "number" &&
    Number.isSafeInteger(props.expectedRevision) &&
    props.expectedRevision >= 0 &&
    (props.actualRevision === null ||
      (typeof props.actualRevision === "number" &&
        Number.isSafeInteger(props.actualRevision) &&
        props.actualRevision >= 0)) &&
    props.actualRevision !== props.expectedRevision
  );
}
