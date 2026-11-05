/**
 * Provisional byte codecs for the manifest root, a table's group directory and a group root
 * (format/reachable.ts `ManifestCodecs`). Ruling R15.3 gives these codecs to the page-format
 * track (B2) in Phase 2; until B2 lands them, the write path stores these structures as tagged
 * JSON (bytes.ts `encodeJson`) behind the same interface, so swapping in B2's codecs changes no
 * caller. The format is not frozen: nothing outside next/ reads it, and B2's replaces it before
 * any release.
 */
import type { ManifestCodecs } from "../format/reachable.js";
import type { GroupRef, GroupRoot, ManifestRoot } from "../format/manifest.js";
import { MANIFEST_FORMAT_VERSION } from "../format/manifest.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { decodeJson, encodeJson } from "./bytes.js";

const ROOT_TAG = "minnow-root";
const GROUPS_TAG = "minnow-groups";
const GROUP_ROOT_TAG = "minnow-group-root";

function tagged(tag: string, value: unknown): Uint8Array {
  return encodeJson({ tag, value });
}

function untag(bytes: Uint8Array, tag: string): unknown {
  const decoded = decodeJson(bytes);
  if (
    typeof decoded !== "object" ||
    decoded === null ||
    !("tag" in decoded) ||
    decoded.tag !== tag ||
    !("value" in decoded)
  )
    throw new StoreCorruptionError(`Not a ${tag} structure`);
  return decoded.value;
}

export function encodeRoot(root: ManifestRoot): Uint8Array {
  return tagged(ROOT_TAG, root);
}

export function decodeRoot(bytes: Uint8Array): ManifestRoot {
  const root = untag(bytes, ROOT_TAG) as ManifestRoot;
  if (root.formatVersion !== MANIFEST_FORMAT_VERSION)
    throw new StoreCorruptionError(`Unsupported manifest version ${String(root.formatVersion)}`);
  return root;
}

export function encodeGroups(groups: readonly GroupRef[]): Uint8Array {
  return tagged(GROUPS_TAG, groups);
}

export function decodeGroups(bytes: Uint8Array): readonly GroupRef[] {
  const groups = untag(bytes, GROUPS_TAG);
  if (!Array.isArray(groups)) throw new StoreCorruptionError("A group directory is not a list");
  return groups as GroupRef[];
}

export function encodeGroupRoot(root: GroupRoot): Uint8Array {
  return tagged(GROUP_ROOT_TAG, root);
}

export function decodeGroupRoot(bytes: Uint8Array): GroupRoot {
  return untag(bytes, GROUP_ROOT_TAG) as GroupRoot;
}

export const manifestCodecs: ManifestCodecs = { decodeRoot, decodeGroups, decodeGroupRoot };
