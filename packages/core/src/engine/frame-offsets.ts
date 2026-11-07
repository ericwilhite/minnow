/** Parser-owned frame precision and literal types, kept outside the logical plan fields. */
export interface FrameOffsetMetadata {
  readonly text?: string;
  /** Typed-only placeholder, not a public logical-plan field. */
  readonly parameter?: number;
  /** Original PostgreSQL literal type, used to resolve RANGE over integer keys. */
  readonly numeric?: true;
  /** Rounded ROWS/GROUPS integer, or integer RANGE literal, beyond the safe double range. */
  readonly exact?: bigint;
}

const frameOffsets = new WeakMap<object, FrameOffsetMetadata>();

export function frameOffsetMetadata(bound: object): FrameOffsetMetadata | undefined {
  return frameOffsets.get(bound);
}

export function rememberFrameOffset<T extends object>(
  bound: T,
  metadata: FrameOffsetMetadata | undefined,
): T {
  if (metadata !== undefined) frameOffsets.set(bound, metadata);
  return bound;
}

/** Plan copies retain the immutable literal metadata without exposing fields on the model. */
export function copyFrameOffset(source: object, target: object): void {
  const metadata = frameOffsets.get(source);
  if (metadata !== undefined) frameOffsets.set(target, metadata);
}
