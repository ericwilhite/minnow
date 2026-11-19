/**
 * Count work between real MessageChannel tasks, independently of host pauses and GC.
 * One synthetic clock unit is 1,024 source field/descriptor reads, one generator step,
 * 16 indexed/delivered native records, or 64 KiB transferred through storage handles. It drives the real shared slice clock;
 * it is not a prediction of wall time. No generator or storage operation is replaced.
 */
import { vi } from "vitest";
import * as slices from "../work-slicer.js";

export class EventLoopWork {
  constructor(readonly clock: "work" | "wall" = "work") {}
  #active = false;
  #clock = 0;
  #turnStart = 0;
  #largest = 0;
  #fields = 0;
  #steps = 0;
  #bytes = 0;
  #turns = 0;
  #items = 0;

  #add(units: number): void {
    this.#clock += units;
    this.#largest = Math.max(this.#largest, this.#clock - this.#turnStart);
  }
  #field(): void {
    if (!this.#active) return;
    this.#fields++;
    this.#add(1 / 1024);
  }
  #boundary(): void {
    if (!this.#active) return;
    this.#turns++;
    this.#turnStart = this.#clock;
  }

  rows<T extends object>(rows: readonly T[]): T[] {
    if (this.clock === "wall") return rows as T[];
    return rows.map(
      (row) =>
        new Proxy(row, {
          get: (target, property, receiver) => {
            if (typeof property === "string" && Object.hasOwn(target, property)) this.#field();
            return Reflect.get(target, property, receiver) as unknown;
          },
          getOwnPropertyDescriptor: (target, property) => {
            const descriptor = Reflect.getOwnPropertyDescriptor(target, property);
            if (descriptor !== undefined) this.#field();
            return descriptor;
          },
        }),
    );
  }

  /** A native index insertion or cursor-delivered record, e.g. an immutable key forward. */
  item(): void {
    if (!this.#active) return;
    this.#items++;
    this.#add(1 / 16);
  }

  transfer(bytes: number): void {
    if (!this.#active) return;
    this.#bytes += bytes;
    this.#add(bytes / 65536);
  }

  #counted<T>(steps: Generator<void, T>): Generator<void, T> {
    const proxy: Generator<void, T> = new Proxy(steps, {
      get: (target, property) => {
        if (property === Symbol.iterator) return () => proxy;
        if (property === "next")
          return (...args: [] | [unknown]) => {
            if (this.#active) {
              this.#steps++;
              this.#add(1);
            }
            return target.next(...args);
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? (value.bind(target) as unknown) : value;
      },
    });
    return proxy;
  }

  async #measureWall(work: () => Promise<void>) {
    const start = performance.now();
    let last = start,
      largest = 0;
    const watchdog = setInterval(() => {
      const now = performance.now();
      largest = Math.max(largest, now - last);
      last = now;
    }, 1);
    try {
      await work();
      const end = performance.now();
      return {
        // Preserve the original one-millisecond watchdog statistic.
        wallLargest: Math.round(largest),
        wallElapsed: end - start,
        largest: 0,
        fields: 0,
        steps: 0,
        bytes: 0,
        turns: 0,
        items: 0,
      };
    } finally {
      clearInterval(watchdog);
    }
  }
  async measure(work: () => Promise<void>) {
    // The latency lane runs ordinary records, native clocks and native scheduler
    // methods. Work-count proxies and spies would change what it measures.
    if (this.clock === "wall") return this.#measureWall(work);
    const run = slices.runSteps,
      sliced = slices.runStepsSliced,
      yieldTurn = slices.yieldToEventLoop;
    const NativeChannel = MessageChannel;
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, "MessageChannel");
    const boundary = () => this.#boundary();
    const mocks: Array<{ mockRestore(): void }> = [];
    this.#active = true;
    try {
      mocks.push(vi.spyOn(performance, "now").mockImplementation(() => this.#clock));
      mocks.push(
        vi
          .spyOn(slices, "runSteps")
          .mockImplementation(<T>(steps: Generator<void, T>) => run(this.#counted(steps))),
      );
      mocks.push(
        vi
          .spyOn(slices, "runStepsSliced")
          .mockImplementation(<T>(steps: Generator<void, T>) => sliced(this.#counted(steps))),
      );
      class ObservedChannel extends NativeChannel {
        constructor() {
          super();
          const observed = new Proxy(this.port1, {
            set(target, property, value: unknown) {
              if (property === "onmessage" && typeof value === "function") {
                const callback = value as (event: MessageEvent) => void;
                return Reflect.set(
                  target,
                  property,
                  (event: MessageEvent) => {
                    boundary();
                    callback.call(target, event);
                  },
                  target,
                );
              }
              return Reflect.set(target, property, value, target);
            },
            get(target, property) {
              const value: unknown = Reflect.get(target, property, target);
              return typeof value === "function" ? (value.bind(target) as unknown) : value;
            },
          });
          Object.defineProperty(this, "port1", { value: observed });
        }
      }
      Object.defineProperty(globalThis, "MessageChannel", {
        configurable: true,
        writable: true,
        enumerable: descriptor?.enumerable ?? true,
        value: ObservedChannel,
      });
      // Rebase the shared scheduler on this clock at an actual task boundary.
      await yieldTurn();
      await work();
      return {
        wallLargest: null,
        wallElapsed: null,
        largest: this.#largest,
        fields: this.#fields,
        steps: this.#steps,
        bytes: this.#bytes,
        turns: this.#turns,
        items: this.#items,
      };
    } finally {
      this.#active = false;
      for (const mock of mocks.reverse()) mock.mockRestore();
      if (descriptor !== undefined) Object.defineProperty(globalThis, "MessageChannel", descriptor);
      else Reflect.deleteProperty(globalThis, "MessageChannel");
      // Restore the shared clock too, so subsequent queries never inherit synthetic time.
      await yieldTurn();
    }
  }
}
