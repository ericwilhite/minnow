/** Concrete low-level root-graph fixture. Funding ports model prepaid test workspace;
 * production owner admission/dispatch remain separate and guarded. */
import { expect } from "vitest";
import type { Grant } from "../memory/governor.js";
import { Governor } from "../memory/governor.js";
import { StatementImpl } from "../write/transaction.js";
import { releaseIdle } from "../write/budget.js";
import { bindTableRules, tableRuleMinimumBytes } from "../exec/row-rules.js";
import { compileTableRules } from "../exec/rules.js";
import { TriggerBodyRunner } from "../exec/trigger-body.js";
import { clockParameter } from "../exec/parameters.js";
import { ChunkReader } from "../page/reader.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import {
  NativeCommandRoot,
  nativeCommandRootSetupBytes,
  type NativeRootPreparation,
  type NativeCommandRootScope,
} from "../owner/command-root.js";
import { nativePrefixSourceBytes } from "../owner/prefix-images.js";
import { StorageCore } from "./storage-core.js";
let identity = 100000;
function compareNames(left: string, right: string) {
  let i = 0,
    j = 0;
  while (i < left.length && j < right.length) {
    const a = left.codePointAt(i) ?? 0,
      b = right.codePointAt(j) ?? 0;
    if (a !== b) return a - b;
    i += a > 65535 ? 2 : 1;
    j += b > 65535 ? 2 : 1;
  }
  return left.length - i - (right.length - j);
}
export async function commandRootFixture(
  setup: readonly string[],
  sql: string,
  published = false,
  cap = 64 << 20,
) {
  const core = new StorageCore(256);
  Object.defineProperty(core.harness, "governor", { value: new Governor(cap) });
  await core.open();
  for (const text of setup) await core.execute(text);
  if (published) {
    await core.harness.path.recycleLog();
    await core.harness.path.quiesce();
  }
  const tx = await core.harness.path.begin(),
    catalog = tx.catalog,
    stop = new AbortController(),
    signal = stop.signal,
    lowered = core.lowerer.lower(sql, { ...core.context(), catalog });
  if (lowered.kind !== "modify") throw new Error("root modify");
  const definitions = catalog.tables.map((table) => {
      const expr = core.lowerer.lowerTableRules(table, catalog);
      return { table, expr, program: compileTableRules(table, catalog.version, expr) };
    }),
    triggers = catalog.triggers.map((definition) => {
      const owner = catalog.table(definition.tableId);
      if (!owner) throw new Error("trigger owner");
      return {
        definition,
        bodies: definition.statements.map((body) => {
          const lowered = core.lowerer.lower(body.sql, {
            ...core.context(),
            catalog,
            parameterTypes: body.bindings.map((b) => b.type),
          });
          if (lowered.kind !== "modify") throw new Error("body modify");
          return new TriggerBodyRunner(owner, body, lowered.plan);
        }),
      };
    });
  triggers.sort(
    (a, b) =>
      compareNames(a.definition.name, b.definition.name) || a.definition.id - b.definition.id,
  );
  const preparation: NativeRootPreparation = {
      rootId: ++identity,
      catalog,
      frames: 3,
      rowEvents: 16,
      root: lowered.plan,
      triggers,
      tableIds: definitions.map((d) => d.table.id),
      defaults: definitions.flatMap(({ table, expr }) =>
        table.columns
          .filter((c) => c.generatedSql === null)
          .map((c) => ({
            tableId: table.id,
            columnId: c.id,
            expr: expr.defaults?.find((d) => d.columnId === c.id)?.expr ?? null,
          })),
      ),
      clock: clockParameter({ kind: "timestamp", zoned: true }, 0n),
    },
    setupBytes = nativeCommandRootSetupBytes(preparation),
    governor = core.harness.governor;
  let bindingBytes = 0,
    sourceBytes = 0;
  for (const def of definitions) {
    bindingBytes += tableRuleMinimumBytes(def.program, 1);
    sourceBytes = Math.max(sourceBytes, nativePrefixSourceBytes(def.table));
  }
  const grants = await governor.admitGroup({
      label: "native graph fixture complete fixed workspace",
      signal,
      grants: [
        {
          label: "native graph metadata",
          minimumBytes: setupBytes + bindingBytes + (2 << 20),
          spillable: true,
        },
        { label: "native graph source", minimumBytes: sourceBytes, spillable: true },
      ],
    }),
    metadata = grants[0],
    source = grants[1];
  if (!metadata || !source) throw new Error("root grants");
  const tables = definitions.map((d) => ({
      table: d.table,
      rules: bindTableRules(d.program, metadata, { capacity: 1, signal }),
    })),
    statement = tx.statement({
      signal,
      rules: (id) => {
        const binding = tables.find((d) => d.table.id === id);
        if (!binding) throw new Error("root rules");
        return binding.rules;
      },
    });
  if (!(statement instanceof StatementImpl)) throw new Error("native statement");
  async function ensure(grant: Grant, bytes: number, s: AbortSignal) {
    signal.throwIfAborted();
    s.throwIfAborted();
    if (grant.loaned !== 0) throw new Error("root admission under loan");
    if (grant.held - grant.allocated < bytes && !(await grant.makeRoom?.(bytes, s)))
      throw new Error("missing root funding");
    signal.throwIfAborted();
    s.throwIfAborted();
  }
  const before = metadata.allocated,
    spill = new MemorySpillService(),
    scope: NativeCommandRootScope = {
      ...preparation,
      statement,
      tables,
      governor,
      signal,
      funding: {
        metadata,
        source,
        prepareMetadata: (n, s) => ensure(metadata, n + 65536, s),
        prepareSource: (n, s) => ensure(source, n, s),
        prepareRules: (_t, _i, _phase, s) => ensure(metadata, 256 << 10, s),
        releaseIdleStep: () => {
          releaseIdle(source);
          releaseIdle(metadata, metadata.allocated + 65536);
        },
      },
      execution: {
        reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
        spill,
        queryId: identity,
        statementTime: 0n,
        sequences: null,
        sequenceSession: null,
        returning: "atExecuted",
      },
      checkpoint: () => signal.throwIfAborted(),
    };
  const root = await NativeCommandRoot.create(scope);
  expect(metadata.allocated - before).toBeLessThanOrEqual(setupBytes);
  let retiring: Promise<void> | null = null;
  const retire = () =>
    (retiring ??= (async () => {
      await root.close();
      statement.rollback();
      tx.rollback();
      for (const binding of tables) binding.rules.close();
      expect(metadata.allocated).toBe(0);
      expect(source.allocated).toBe(0);
      expect(source.loaned).toBe(0);
      metadata.close();
      source.close();
      expect(spill.bytesInUse).toBe(0);
    })());
  return {
    core,
    tx,
    statement,
    root,
    scope,
    metadata,
    source,
    stop,
    spill,
    setupBytes,
    retire,
    async commit() {
      await root.end();
      const receipt = await tx.commit(null);
      await receipt.durable;
    },
    async close() {
      await retire();
      await core.close();
      expect(governor.usedBytes).toBe(0);
    },
  };
}
