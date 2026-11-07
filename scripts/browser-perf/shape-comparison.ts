/** Opt-in benchmark compiler only. Freeze the three changed modules at the pre-lever tip;
 * select a complete family once in each fresh worker, before setup/timing. No production flag.
 * Requires the reference Git object (an exported source archive cannot run this comparison).
 */
import { execFileSync } from "node:child_process";
import type { Plugin } from "vite";

export const SHAPE_REFERENCE = "74c79c9b";
const classes = new Map([
  ["page/reader.ts", "ChunkReader"],
  ["exec/column-target.ts", "ColumnTarget"],
  ["exec/table-scan.ts", "TableScan"],
]);

export function shapeComparison(): Plugin {
  const sources = new Map(
    [...classes.keys()].map((file) => [
      file,
      execFileSync("git", ["show", `${SHAPE_REFERENCE}:packages/core/src/next/${file}`], {
        encoding: "utf8",
      }),
    ]),
  );
  return {
    name: "export-shape-comparison",
    enforce: "pre",
    load(id) {
      const [file, query] = id.split("?");
      if (!new URLSearchParams(query).has("shapeBaseline")) return;
      return sources.get(file?.split("/next/")[1] ?? "");
    },
    transform(source, id) {
      if (id.includes("?")) return;
      const file = id.split("/next/")[1],
        name = classes.get(file ?? "");
      if (!file || !name) return;
      const anchor = `export class ${name}`;
      if (!source.includes(anchor)) throw new Error(`Shape comparison anchor moved: ${file}`);
      const basename = file.slice(file.lastIndexOf("/") + 1);
      return (
        `import {${name} as Baseline${name}} from "./${basename}?shapeBaseline";\n` +
        source.replace(anchor, `class Candidate${name}`) +
        `\nexport const ${name} = new URL(globalThis.location.href).searchParams.get("scanShape") === "baseline" ? Baseline${name} : Candidate${name};\nexport type ${name} = Candidate${name};\n`
      );
    },
  };
}
