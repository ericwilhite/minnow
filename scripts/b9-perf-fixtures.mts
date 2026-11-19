/** Branch-only probe fixture ownership. Development vocabularies are not predecessors:
 * share an image only when every exported durable descriptor agrees, otherwise let
 * that runtime's ordinary writer generate it from the same logical workload.
 */
import { createHash } from "node:crypto";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

export interface RuntimeFormat {
  readonly fingerprint: string;
  readonly reader: number | null;
  readonly writer: number | null;
  readonly manifest: number | null;
  readonly envelope: number | null;
}

export async function runtimeFormat(root: string): Promise<RuntimeFormat> {
  const registry = (await import(
    pathToFileURL(join(resolve(root), "packages/core/dist/next/durable-format.js")).href
  )) as Record<string, unknown>;
  const constants = Object.fromEntries(
    Object.entries(registry)
      .filter(([, value]) => typeof value !== "function")
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
  );
  const number = (key: string) => {
    const value = registry[key];
    return typeof value === "number" ? value : null;
  };
  return {
    fingerprint: createHash("sha256")
      .update(
        JSON.stringify(constants, (_key, value: unknown) =>
          typeof value === "bigint" ? { bigint: value.toString() } : value,
        ),
      )
      .digest("hex"),
    reader: number("DURABLE_READER_VERSION"),
    writer: number("DURABLE_WRITER_VERSION"),
    manifest: number("MANIFEST_FORMAT_VERSION"),
    envelope: number("METADATA_ENVELOPE_VERSION"),
  };
}

export interface FixtureArm {
  readonly label: string;
  readonly root: string;
  readonly format: RuntimeFormat;
}
export interface ProbeFixture {
  readonly label: string;
  readonly writer: string;
  readonly format: RuntimeFormat;
  readonly image: string;
  readonly imageIdentity: string;
  readonly readers: readonly string[];
}

export function planFixtures(
  arms: readonly FixtureArm[],
  output: string,
  workload = "W1M",
): ProbeFixture[] {
  if (!/^[A-Za-z0-9_.-]+$/.test(workload)) throw new Error("Bad logical workload identity");
  const byFormat = new Map<string, { fixture: ProbeFixture; readers: string[] }>(),
    roots = new Map<string, string>();
  for (const arm of arms) {
    const root = resolve(arm.root),
      fingerprint = arm.format.fingerprint;
    if (!/^[a-f0-9]{64}$/.test(fingerprint)) throw new Error("Bad durable descriptor fingerprint");
    const previous = roots.get(root);
    if (previous !== undefined && previous !== fingerprint)
      throw new Error("One runtime cannot have two durable vocabularies");
    roots.set(root, fingerprint);
    const existing = byFormat.get(fingerprint);
    if (existing) {
      if (!existing.readers.includes(root)) existing.readers.push(root);
      continue;
    }
    const readers = [root];
    byFormat.set(fingerprint, {
      readers,
      fixture: {
        label: arm.label,
        writer: root,
        format: arm.format,
        image: join(resolve(output), "input-image-" + workload + "-" + fingerprint),
        imageIdentity: "b9-" + workload + "-" + fingerprint,
        readers,
      },
    });
  }
  return [...byFormat.values()].map(({ fixture }) => fixture);
}
