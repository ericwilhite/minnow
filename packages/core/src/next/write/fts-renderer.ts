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
