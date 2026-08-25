/**
 * The synchronous file surface the log toolkit writes through — structurally a subset of the
 * browser's `FileSystemSyncAccessHandle`, so an OPFS handle satisfies it as-is, and any
 * platform with positioned synchronous reads and writes (a Node file descriptor, an in-memory
 * buffer in tests) can implement it in a few lines.
 *
 * The toolkit's performance model assumes these calls do not yield: a write-ahead-log append
 * or an extent write happens inside one synchronous run, so state mutation and its durable
 * record can never interleave with another operation.
 */
export interface SyncFileHandle {
  /** Current file size in bytes. */
  getSize(): number;
  /** Reads at the byte offset into the buffer; returns the bytes read. */
  read(buffer: Uint8Array, options: { at: number }): number;
  /** Writes the buffer at the byte offset; returns the bytes written. */
  write(buffer: Uint8Array, options: { at: number }): number;
  /** Grows or shrinks the file to exactly this many bytes. */
  truncate(size: number): void;
  /** Forces written bytes to durable storage. */
  flush(): void;
  /** Releases the handle (and any exclusive lock the platform ties to it). */
  close(): void;
}

/**
 * Writes the complete buffer, retrying the short transfers permitted by sync-access handles.
 * The common path is one `write` call; retries pass subarray views and never copy payload bytes.
 */
export function writeFully(
  handle: SyncFileHandle,
  buffer: Uint8Array,
  at: number,
  operation = "writing file bytes",
): void {
  assertTransferRange(at, buffer.byteLength, "write", operation);
  let transferred = 0;
  while (transferred < buffer.byteLength) {
    const remaining = buffer.byteLength - transferred;
    const chunk = transferred === 0 ? buffer : buffer.subarray(transferred);
    const count = handle.write(chunk, { at: at + transferred });
    assertTransferCount(count, remaining, "write", operation, at + transferred);
    if (count === 0) {
      throw new Error(
        `OPFS write made no progress while ${operation}: wrote ${String(transferred)} of ` +
          `${String(buffer.byteLength)} bytes (stalled at file offset ${String(at + transferred)})`,
      );
    }
    transferred += count;
  }
}

/**
 * Reads exactly `buffer.byteLength` bytes, retrying short transfers without copying data. A zero
 * transfer before the requested range is complete is an unexpected EOF, not a successful read.
 */
export function readFully(
  handle: SyncFileHandle,
  buffer: Uint8Array,
  at: number,
  operation = "reading file bytes",
): void {
  assertTransferRange(at, buffer.byteLength, "read", operation);
  let transferred = 0;
  while (transferred < buffer.byteLength) {
    const remaining = buffer.byteLength - transferred;
    const chunk = transferred === 0 ? buffer : buffer.subarray(transferred);
    const count = handle.read(chunk, { at: at + transferred });
    assertTransferCount(count, remaining, "read", operation, at + transferred);
    if (count === 0) {
      throw new Error(
        `Unexpected EOF while ${operation}: read ${String(transferred)} of ` +
          `${String(buffer.byteLength)} bytes (EOF at file offset ${String(at + transferred)})`,
      );
    }
    transferred += count;
  }
}

function assertTransferRange(
  at: number,
  length: number,
  kind: "read" | "write",
  operation: string,
): void {
  if (!Number.isSafeInteger(at) || at < 0 || length > Number.MAX_SAFE_INTEGER - at) {
    throw new RangeError(
      `Invalid OPFS ${kind} range while ${operation}: ${String(at)}+${String(length)} ` +
        `must be a non-negative safe-integer byte range`,
    );
  }
}

function assertTransferCount(
  count: number,
  remaining: number,
  kind: "read" | "write",
  operation: string,
  at: number,
): void {
  if (!Number.isSafeInteger(count) || count < 0 || count > remaining) {
    throw new Error(
      `Invalid OPFS ${kind} result while ${operation}: handle reported ${String(count)} bytes ` +
        `for a ${String(remaining)}-byte request at file offset ${String(at)}`,
    );
  }
}
