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
