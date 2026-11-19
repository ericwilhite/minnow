/** Test-only backing census. It observes constructors, not allocation requests.
 * Each synchronous factory step must fund every new backing before construction.
 * Nested factories are checked separately; views of existing buffers cost zero.
 * This measures backing capacity, not V8 object headers or browser RSS. */
import { vi } from "vitest";
import { OutputArena } from "../exec/kernels/vectors.js";
import * as stores from "../exec/column-store.js";
import * as varlen from "../vector/varlen.js";
import * as pools from "../exec/spill-pool.js";
import * as sorts from "../exec/sort.js";
import * as results from "../exec/result-buffer.js";
import * as scans from "../exec/table-scan.js";
import { MemoryOutputs } from "../exec/memory-output.js";
import type { ByteCharge } from "../vector/varlen.js";

interface Step {
  name: string;
  actual: number;
  credit: number;
  prepaid: number;
  freed: number;
  seen: WeakSet<object>;
}
export function allocationAudit(covered: () => number) {
  const steps: Step[] = [],
    completed: Array<{ name: string; actual: number; credit: number }> = [];
  const restore: Array<() => void> = [];
  let observedBytes = 0;
  const backing = (buffer: ArrayBufferLike) => {
    for (const step of steps) {
      if (step.seen.has(buffer)) continue;
      step.seen.add(buffer);
      step.actual += buffer.byteLength;
      const credit = step.credit + Math.max(0, step.prepaid - step.freed);
      if (step.actual > credit)
        throw new Error(
          `Uncharged backing in ${step.name}: actual=${String(step.actual)}, preceding credit=${String(credit)}`,
        );
      if (step.actual > covered())
        throw new Error(
          `Backing exceeds complete admission in ${step.name}: ${String(step.actual)}>${String(covered())}`,
        );
    }
    if (steps.length) observedBytes += buffer.byteLength;
  };
  const constructors = [
    "ArrayBuffer",
    "Uint8Array",
    "Int8Array",
    "Uint16Array",
    "Int16Array",
    "Uint32Array",
    "Int32Array",
    "Float32Array",
    "Float64Array",
    "BigInt64Array",
    "BigUint64Array",
  ] as const;
  for (const name of constructors) {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, name);
    if (!descriptor || typeof descriptor.value !== "function") throw Error(`Missing ${name}`);
    const original = descriptor.value as new (...args: unknown[]) => object;
    const proxy = new Proxy(original, {
      construct(target, args, newTarget) {
        const result = Reflect.construct(target, args, newTarget) as object;
        if (
          steps.length &&
          (name === "ArrayBuffer" || args[0] !== (result as ArrayBufferView).buffer)
        ) {
          if (name === "ArrayBuffer") backing(result as ArrayBuffer);
          else backing((result as ArrayBufferView).buffer);
        }
        return result;
      },
    });
    Object.defineProperty(globalThis, name, { ...descriptor, value: proxy });
    restore.push(() => Object.defineProperty(globalThis, name, descriptor));
    // Species allocations (.slice/.map/.from) must enter the same census.
    // The instance still has the native typed-array prototype/internal slots.
    const prototype = Reflect.get(original, "prototype") as object;
    const ctor = Object.getOwnPropertyDescriptor(prototype, "constructor");
    if (ctor) {
      Object.defineProperty(prototype, "constructor", { ...ctor, value: proxy });
      restore.push(() => Object.defineProperty(prototype, "constructor", ctor));
    }
  }
  const hooked = new WeakMap<object, Set<string>>();
  let depth = 0,
    freeDepth = 0;
  const hook = (charge: ByteCharge) => {
    for (const name of ["allocate", "free"] as const) {
      let prototype: object | null = charge;
      let descriptor: PropertyDescriptor | undefined;
      while (prototype) {
        descriptor = Object.getOwnPropertyDescriptor(prototype, name);
        if (descriptor) break;
        prototype = Object.getPrototypeOf(prototype) as object | null;
      }
      if (!prototype || !descriptor || typeof descriptor.value !== "function") continue;
      const methods = hooked.get(prototype) ?? new Set<string>();
      if (methods.has(name)) continue;
      methods.add(name);
      hooked.set(prototype, methods);
      const original = descriptor.value as (this: ByteCharge, bytes: number) => unknown;
      Object.defineProperty(prototype, name, {
        ...descriptor,
        value: function (this: ByteCharge, bytes: number) {
          if (name === "allocate") {
            const outer = depth++ === 0;
            try {
              const accepted = original.call(this, bytes);
              if (outer && accepted) for (const step of steps) step.credit += bytes;
              return accepted;
            } finally {
              depth--;
            }
          }
          const outer = freeDepth++ === 0;
          try {
            const result = original.call(this, bytes);
            if (outer) for (const step of steps) step.freed += bytes;
            return result;
          } finally {
            freeDepth--;
          }
        },
      });
      restore.push(() => Object.defineProperty(prototype, name, descriptor));
    }
  };
  const call = <T>(name: string, charge: ByteCharge, body: () => T, prepaid = 0): T => {
    hook(charge);
    const step: Step = { name, actual: 0, credit: 0, prepaid, freed: 0, seen: new WeakSet() };
    steps.push(step);
    try {
      return body();
    } finally {
      steps.pop();
      completed.push({
        name,
        actual: step.actual,
        credit: step.credit + Math.max(0, prepaid - step.freed),
      });
    }
  };
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the intercepted receiver via call/apply below.
  const flat = OutputArena.prototype.flat;
  const flatSpy = vi.spyOn(OutputArena.prototype, "flat").mockImplementation(function (
    this: OutputArena,
    ...args: Parameters<typeof flat>
  ) {
    return call("OutputArena.flat", this.charge, () => flat.apply(this, args));
  });
  restore.push(() => flatSpy.mockRestore());
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the intercepted receiver via call/apply below.
  const validity = OutputArena.prototype.validity;
  const validitySpy = vi.spyOn(OutputArena.prototype, "validity").mockImplementation(function (
    this: OutputArena,
  ) {
    return call("OutputArena.validity", this.charge, () => validity.call(this));
  });
  restore.push(() => validitySpy.mockRestore());
  const Scan = scans.TableScan;
  const scanSpy = vi.spyOn(scans, "TableScan").mockImplementation(function (
    ...args: ConstructorParameters<typeof Scan>
  ) {
    return call("TableScan.constructor", args[0].budget, () => new Scan(...args));
  });
  restore.push(() => scanSpy.mockRestore());
  // Native output replacements are synchronous and retain their old charge until
  // installation. Every new backing must follow the replacement's fresh charge.
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
  const prepareOutputs = MemoryOutputs.prototype.prepare;
  const outputsSpy = vi.spyOn(MemoryOutputs.prototype, "prepare").mockImplementation(function (
    this: MemoryOutputs,
    ...args: Parameters<typeof prepareOutputs>
  ) {
    return call("MemoryOutputs.prepare", this.budget, () => prepareOutputs.apply(this, args));
  });
  restore.push(() => outputsSpy.mockRestore());
  const storeCharges = new WeakMap<stores.ColumnStore, ByteCharge>();
  const Pool = pools.Pool;
  const poolSpy = vi.spyOn(pools, "Pool").mockImplementation(function (
    ...args: ConstructorParameters<typeof Pool>
  ) {
    const instance = call("Pool.constructor", args[0], () => new Pool(...args));
    const paid = completed.at(-1)?.credit ?? 0;
    if (instance.room + instance.used > paid) throw Error("Uncharged pool capacity");
    return instance;
  });
  restore.push(() => poolSpy.mockRestore());
  const Sort = sorts.SortOperator;
  const sortSpy = vi.spyOn(sorts, "SortOperator").mockImplementation(function (
    ...args: ConstructorParameters<typeof Sort>
  ) {
    return call("SortOperator.constructor", args[1].grant, () => new Sort(...args));
  });
  restore.push(() => sortSpy.mockRestore());
  const Result = results.ResultBuffers;
  const resultCredit = new WeakMap<results.ResultBuffers, number>();
  const resultSpy = vi.spyOn(results, "ResultBuffers").mockImplementation(function (
    ...args: ConstructorParameters<typeof Result>
  ) {
    const instance = call("ResultBuffers.constructor", args[0], () => new Result(...args));
    resultCredit.set(instance, completed.at(-1)?.credit ?? 0);
    return instance;
  });
  restore.push(() => resultSpy.mockRestore());
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the intercepted receiver via call/apply below.
  const resultBytes = Result.prototype.bytes;
  const resultBytesSpy = vi.spyOn(Result.prototype, "bytes").mockImplementation(function (
    this: results.ResultBuffers,
    bytes: number,
  ) {
    const output = call(
      "ResultBuffers.bytes",
      this.budget,
      () => resultBytes.call(this, bytes),
      resultCredit.get(this) ?? 0,
    );
    resultCredit.set(this, (resultCredit.get(this) ?? 0) - output.buffer.byteLength);
    return output;
  });
  restore.push(() => resultBytesSpy.mockRestore());
  const Store = stores.ColumnStore;
  const storeSpy = vi.spyOn(stores, "ColumnStore").mockImplementation(function (
    ...args: ConstructorParameters<typeof Store>
  ) {
    const instance = call("ColumnStore.constructor", args[1], () => new Store(...args));
    storeCharges.set(instance, args[1]);
    return instance;
  });
  restore.push(() => storeSpy.mockRestore());
  for (const name of ["append", "appendFrom", "reset"] as const) {
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the intercepted receiver via call/apply below.
    const original = Store.prototype[name];
    const spy = vi.spyOn(Store.prototype, name).mockImplementation(function (
      this: stores.ColumnStore,
      ...args: unknown[]
    ) {
      const charge = storeCharges.get(this);
      if (!charge) return Reflect.apply(original, this, args) as ReturnType<typeof original>;
      let prepaid = 0;
      if (name === "reset") {
        const buffers = new Set<ArrayBufferLike>();
        const visit = (value: unknown): void => {
          if (ArrayBuffer.isView(value)) {
            buffers.add(value.buffer);
            return;
          }
          if (value && typeof value === "object") for (const v of Object.values(value)) visit(v);
        };
        for (let c = 0; c < this.types.length; c++)
          for (let k = 0; k < this.chunkCount; k++) visit(this.chunk(c, k));
        for (const b of buffers) prepaid += b.byteLength;
      }
      return call(
        `ColumnStore.${name}`,
        charge,
        () => Reflect.apply(original, this, args) as ReturnType<typeof original>,
        prepaid,
      );
    });
    restore.push(() => spy.mockRestore());
  }
  const builders = new WeakMap<varlen.VarlenBuilder, ByteCharge>();
  const Builder = varlen.VarlenBuilder;
  const builderSpy = vi.spyOn(varlen, "VarlenBuilder").mockImplementation(function (
    ...args: ConstructorParameters<typeof Builder>
  ) {
    const instance = call("VarlenBuilder.constructor", args[1], () => new Builder(...args));
    builders.set(instance, args[1]);
    return instance;
  });
  restore.push(() => builderSpy.mockRestore());
  for (const name of ["appendBytes", "appendString", "reset"] as const) {
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the intercepted receiver via call/apply below.
    const original = Builder.prototype[name];
    const spy = vi.spyOn(Builder.prototype, name).mockImplementation(function (
      this: varlen.VarlenBuilder,
      ...args: unknown[]
    ) {
      const charge = builders.get(this);
      return charge
        ? call(
            `VarlenBuilder.${name}`,
            charge,
            () => Reflect.apply(original, this, args) as ReturnType<typeof original>,
            name === "reset" ? this.retainedBytes : 0,
          )
        : (Reflect.apply(original, this, args) as ReturnType<typeof original>);
    });
    restore.push(() => spy.mockRestore());
  }
  return {
    completed,
    summary() {
      const factories: Record<
        string,
        { steps: number; constructedBytes: number; maxStepBytes: number; minCreditSlack: number }
      > = {};
      for (const step of completed) {
        const item = (factories[step.name] ??= {
          steps: 0,
          constructedBytes: 0,
          maxStepBytes: 0,
          minCreditSlack: Infinity,
        });
        item.steps++;
        item.constructedBytes += step.actual;
        item.maxStepBytes = Math.max(item.maxStepBytes, step.actual);
        item.minCreditSlack = Math.min(item.minCreditSlack, step.credit - step.actual);
      }
      return factories;
    },
    get observedBytes() {
      return observedBytes;
    },
    close() {
      for (const undo of restore.reverse()) undo();
    },
  };
}
