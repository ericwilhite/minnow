/**
 * Independent source inventory of durable objects. No imports from the registry or adapters.
 * Discover typed persistence calls and BlobRef/ChunkData fields, then require an explicit
 * classification for EVERY discovered site. Adding a raw writer like #writeBitmap cannot
 * disappear merely because both the registry and its decoder adapter forgot it.
 *
 * Composed codecs (pages/arrays, log bodies/columns) are listed at their outer emission site.
 * This audits the typed storage seams; arbitrary computed/dynamic calls still need code review.
 */
import ts from "typescript";
import { readdirSync } from "node:fs";
import { join, relative } from "node:path";

const NEXT = join(import.meta.dirname, "..");
const TEST_ONLY = new Set([
  "storage/conformance.ts",
  "storage/kit.ts",
  "storage/extents.examples.ts",
  "storage/idb-scenarios.ts",
  "spill/chaos-workload.ts",
  "page/test-support.ts",
  "write/test-support.ts",
  "write/directory-fixture-files.ts",
  "write/directory-compat-api.ts",
  "write/directory-frozen.ts",
  "write/directory-tails-test-support.ts",
]);
function productionFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name),
      name = relative(NEXT, path);
    if (name === "testing") return [];
    if (entry.isDirectory()) return productionFiles(path);
    return entry.name.endsWith(".ts") && !entry.name.endsWith(".test.ts") && !TEST_ONLY.has(name)
      ? [path]
      : [];
  });
}
function* parents(node: ts.Node): Generator<ts.Node> {
  for (let parent = node; !ts.isSourceFile(parent);) {
    parent = parent.parent;
    yield parent;
  }
}
function owner(node: ts.Node): string {
  for (const parent of parents(node))
    if (ts.isInterfaceDeclaration(parent) || ts.isClassDeclaration(parent))
      return parent.name?.getText() ?? "";
  return "";
}
function context(node: ts.Node): string {
  for (const parent of parents(node)) {
    if (
      ts.isMethodDeclaration(parent) ||
      ts.isConstructorDeclaration(parent) ||
      ts.isGetAccessorDeclaration(parent) ||
      ts.isSetAccessorDeclaration(parent)
    )
      return `${owner(parent) || "<object>"}.${parent.name?.getText() ?? "constructor"}`;
    if (ts.isFunctionDeclaration(parent)) return parent.name?.getText() ?? "<anonymous>";
    if (
      ts.isVariableDeclaration(parent) &&
      parent.initializer !== undefined &&
      (ts.isArrowFunction(parent.initializer) || ts.isFunctionExpression(parent.initializer))
    )
      return parent.name.getText();
  }
  return "<module>";
}
const BOUNDARIES = new Set([
  "ExtentWriter.write",
  "ExtentBuilder.append",
  "ExtentSpace.publish",
  "Space.publish",
  "StoreSession.publish",
  "StoreSession.appendLog",
  "StoreSession.writeLogHeader",
  "StoreSession.writeData",
  "SyncFile.write",
  "IdbStoreLike.put",
  "IDBDatabase.createObjectStore",
  "IDBObjectStore.put",
]);
export function discoverDurableSources(): {
  sites: ReadonlyMap<string, number>;
  refs: readonly string[];
} {
  const files = productionFiles(NEXT);
  const program = ts.createProgram(files, {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    skipLibCheck: true,
    // All next/ production sources are explicit roots. Avoid loading unrelated packages' trees.
    noResolve: true,
    types: [],
    noEmit: true,
  });
  const checker = program.getTypeChecker(),
    sites = new Map<string, number>();
  const refs: string[] = [];
  for (const path of files) {
    const source = program.getSourceFile(path);
    if (source === undefined) throw new Error(`Missing inventory source ${path}`);
    const visit = (node: ts.Node): void => {
      if (
        ts.isCallExpression(node) &&
        // Resolve candidate storage methods, not every scalar/kernel call in the engine.
        ((ts.isPropertyAccessExpression(node.expression) &&
          [
            "write",
            "append",
            "publish",
            "appendLog",
            "writeLogHeader",
            "writeData",
            "put",
            "createObjectStore",
          ].includes(node.expression.name.text)) ||
          ts.isElementAccessExpression(node.expression) ||
          (ts.isIdentifier(node.expression) &&
            ["write", "append", "publish", "put"].includes(node.expression.text)))
      ) {
        const declaration = checker.getResolvedSignature(node)?.declaration;
        if (
          declaration !== undefined &&
          (ts.isMethodSignature(declaration) || ts.isMethodDeclaration(declaration))
        ) {
          const target = `${owner(declaration)}.${declaration.name.getText()}`;
          const file = declaration.getSourceFile().fileName;
          const seam =
            /storage\/(?:extents|contract|idb-like)\.ts$/u.test(file) ||
            file.endsWith("/lib.dom.d.ts");
          if (seam && BOUNDARIES.has(target)) {
            const key = `${relative(NEXT, path)}:${context(node)}:${node.expression.getText()} -> ${target}`;
            sites.set(key, (sites.get(key) ?? 0) + 1);
          }
        }
      }
      if (
        relative(NEXT, path) === "format/manifest.ts" &&
        ts.isTypeReferenceNode(node) &&
        ["BlobRef", "ChunkData"].includes(node.typeName.getText())
      ) {
        const parts: string[] = [];
        for (const parent of parents(node)) {
          if (ts.isPropertySignature(parent)) parts.unshift(parent.name.getText());
          if (ts.isInterfaceDeclaration(parent)) {
            parts.unshift(parent.name.getText());
            break;
          }
        }
        if (parts.length > 1) refs.push(parts.join("."));
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
  }
  return { sites, refs: refs.sort() };
}

const RUN = ["runBlock", "runIndex", "runFences", "runBloomDirectory", "runBloom"];
const LOG = ["logFrame", "commitBody", "column", "catalogChange", "arrayValue"];
/** count is source call sites, not executions. [] means transport/private scratch, not a new codec. */
export const DURABLE_EMISSIONS: Readonly<
  Record<string, { count: number; kinds: readonly string[] }>
> = {
  "format/manifest-codec.ts:writeGroupDirectory:writer.write -> ExtentWriter.write": {
    count: 2,
    kinds: ["groupPage", "groupIndex"],
  },
  "page/memory-extents.ts:MemoryWriter.write:builder.append -> ExtentBuilder.append": {
    count: 1,
    kinds: [],
  },
  "page/writer.ts:appendTo:builder.append -> ExtentBuilder.append": {
    count: 1,
    kinds: ["page", "pageDirectory", "largeValue", "sketch", "arrayValue"],
  },
  "storage/extent-list.ts:flush:delta.writer.write -> ExtentWriter.write": {
    count: 1,
    kinds: ["extentList"],
  },
  "storage/file-store.ts:writeAll:file.write -> SyncFile.write": {
    count: 1,
    kinds: ["rootSlot", "logHeader", "logFrame"],
  },
  "storage/idb-bytes.ts:ByteStore.#put:tx.store(this.name).put -> IdbStoreLike.put": {
    count: 1,
    kinds: ["indexedDb"],
  },
  "storage/idb-bytes.ts:ByteStore.#put:tx.store(this.indexName).put -> IdbStoreLike.put": {
    count: 1,
    kinds: ["indexedDb"],
  },
  "storage/idb-like.ts:openIndexedDb:request.result.createObjectStore -> IDBDatabase.createObjectStore":
    { count: 1, kinds: ["indexedDb"] },
  "storage/idb-like.ts:<object>.put:store.put -> IDBObjectStore.put": {
    count: 1,
    kinds: ["indexedDb"],
  },
  'storage/indexeddb.ts:IdbSession.publish:tx.store("roots").put -> IdbStoreLike.put': {
    count: 1,
    kinds: ["rootSlot"],
  },
  "storage/indexeddb.ts:IdbSession.publish:roots.put -> IdbStoreLike.put": {
    count: 1,
    kinds: ["rootSlot"],
  },
  'storage/indexeddb.ts:IdbSession.writeLogHeader:tx.store("meta").put -> IdbStoreLike.put': {
    count: 1,
    kinds: ["logHeader"],
  },
  'storage/indexeddb.ts:IdbSession.flushLog:tx.store("meta").put -> IdbStoreLike.put': {
    count: 1,
    kinds: ["indexedDb"],
  },
  'storage/indexeddb.ts:IdbMetadata.write:tx.store("meta").put -> IdbStoreLike.put': {
    count: 1,
    kinds: [],
  },
  'storage/indexeddb.ts:IdbMetadata.writeRetained:tx.store("meta").put -> IdbStoreLike.put': {
    count: 1,
    kinds: [],
  },
  'storage/indexeddb.ts:IdbScratch.metadata:tx.store("meta").put -> IdbStoreLike.put': {
    count: 1,
    kinds: [],
  },
  "storage/indexeddb.ts:ready:meta.put -> IdbStoreLike.put": { count: 1, kinds: ["indexedDb"] },
  "storage/opfs.ts:QuotaMappedFile.write:this.#handle.write -> SyncFile.write": {
    count: 1,
    kinds: [],
  },
  "storage/opfs.ts:ObservedFile.write:this.#handle.write -> SyncFile.write": {
    count: 1,
    kinds: [],
  },
  "storage/opfs.ts:nextEpoch:file.write -> SyncFile.write": { count: 1, kinds: ["epochSlot"] },
  "storage/sim-idb.ts:SimDatabase.#commit:this.#file.write -> SyncFile.write": {
    count: 1,
    kinds: [],
  },
  "storage/space.ts:Space.sessionWrite:this.#session.writeData -> StoreSession.writeData": {
    count: 1,
    kinds: [],
  },
  "storage/space.ts:Space.#publishNow:this.#session.publish -> StoreSession.publish": {
    count: 1,
    kinds: ["manifest"],
  },
  "storage/space.ts:Writer.write:builder.append -> ExtentBuilder.append": { count: 1, kinds: [] },
  "write/engine.ts:WritePathImpl.open:writer.write -> ExtentWriter.write": {
    count: 1,
    kinds: ["catalog"],
  },
  "write/engine.ts:WritePathImpl.#writeFlush:writer.write -> ExtentWriter.write": {
    count: 2,
    kinds: ["groupRoot", "tableStatistics"],
  },
  "write/engine.ts:WritePathImpl.#writeStaged:writer.write -> ExtentWriter.write": {
    count: 1,
    kinds: ["groupRoot"],
  },
  "write/engine.ts:WritePathImpl.#writeBitmap:writer.write -> ExtentWriter.write": {
    count: 1,
    kinds: ["deletionBitmap"],
  },
  "write/engine.ts:WritePathImpl.#nextRoot:writer.write -> ExtentWriter.write": {
    count: 1,
    kinds: ["catalog"],
  },
  "write/log.ts:LogAppender.appendStream:this.#session.appendLog -> StoreSession.appendLog": {
    count: 1,
    kinds: LOG,
  },
  "write/log.ts:LogAppender.#writeQueued:this.#session.appendLog -> StoreSession.appendLog": {
    count: 1,
    kinds: LOG,
  },
  "write/log.ts:LogAppender.#writeHeader:this.#session.writeLogHeader -> StoreSession.writeLogHeader":
    { count: 1, kinds: ["logHeader"] },
  "write/merge.ts:drain:target.append -> ExtentBuilder.append": { count: 1, kinds: RUN },
  "write/run-output.ts:RunSink.drain:target.append -> ExtentBuilder.append": {
    count: 1,
    kinds: RUN,
  },
  "write/engine.ts:DeltaPublication.publish:space.publish -> ExtentSpace.publish": {
    count: 1,
    kinds: ["manifest"],
  },
};
/** Durable references declared by the schema, independently of its encoders and registry. */
export const DURABLE_REFERENCES: Readonly<Record<string, readonly string[]>> = {
  "ManifestRoot.catalog": ["catalog"],
  "TableRoot.groups": ["groupIndex", "groupPage"],
  "TableRoot.staged.groups": ["groupIndex", "groupPage"],
  "TableRoot.statistics": ["tableStatistics"],
  "GroupRef.root": ["groupRoot"],
  "GroupRef.deletions": ["deletionBitmap"],
  "GroupRoot.rowIds.ref": ["page"],
  "ColumnChunk.data": ["page"],
  "ColumnChunk.directory": ["pageDirectory"],
  "ColumnChunk.distinctSketch": ["sketch"],
  "RunRef.entries": ["runIndex", "runBlock"],
  "RunRef.fences": ["runFences"],
  "RunRef.bloom": ["runBloomDirectory", "runBloom"],
};
