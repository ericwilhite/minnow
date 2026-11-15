/** An optional composition capability, not a supported stored-format negotiation. */
import type { FtsWriteRenderer } from "../access/fts-write.js";
import { supportsFtsSemantics } from "../fts-config.js";
export function compatibleFtsRenderer(renderer: FtsWriteRenderer | null): FtsWriteRenderer | null {
  return renderer !== null &&
    supportsFtsSemantics(renderer) &&
    Number.isSafeInteger(renderer.workspaceBytes) &&
    renderer.workspaceBytes >= 0
    ? renderer
    : null;
}

/** No awaited preparation may keep using a retired or newly enlarged capability. */
export function stableFtsRenderer(renderer: FtsWriteRenderer | null, workspace: number): boolean {
  return (
    renderer !== null &&
    compatibleFtsRenderer(renderer) !== null &&
    renderer.workspaceBytes === workspace
  );
}

export interface FtsClaimSeal {
  readonly renderer: FtsWriteRenderer | null;
  readonly workspaceBytes: number;
  readonly invalidatesAll: boolean;
}
