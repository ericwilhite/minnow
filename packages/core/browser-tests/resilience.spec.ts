import { expect, chromium } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

for (const kind of ["indexeddb", "opfs"] as const) {
  test(`strict ${kind}: multi-tab admission, staged reads, and worker crash recovery`, async ({
    storageContext,
    browserName,
  }) => {
    test.setTimeout(120_000);
    const first = await storageContext.newPage();
    const second = await storageContext.newPage();
    const name = `resilience-${crypto.randomUUID()}`;
    await Promise.all([
      first.goto("/packages/core/browser/"),
      second.goto("/packages/core/browser/"),
    ]);
    if (kind === "opfs") await requireWorkerOpfs(first);
    const open = async (page: typeof first) =>
      page.evaluate(
        async ({ name, kind }) => {
          const url = "/packages/core/browser/resilience.ts";
          const scenario = (await import(url)) as typeof import("../browser/resilience.js");
          // This test queues 32 durable writes per client; deadlines are tested separately.
          await scenario.open(name, kind, 30_000);
        },
        { name, kind },
      );
    await open(first);
    await first.evaluate(async () => {
      const url = "/packages/core/browser/resilience.ts";
      const scenario = (await import(url)) as typeof import("../browser/resilience.js");
      await scenario.initialize();
    });
    await open(second);
    if (kind === "indexeddb") {
      await first.evaluate(async (name) => {
        const url = "/packages/core/browser/resilience.ts";
        const scenario = (await import(url)) as typeof import("../browser/resilience.js");
        await scenario.holdAdmission(name);
      }, name);
      const cdp =
        browserName === "chromium" ? await storageContext.newCDPSession(first) : undefined;
      try {
        await cdp?.send("Page.setWebLifecycleState", { state: "frozen" });
        expect(
          await second.evaluate(async (name) => {
            const url = "/packages/core/browser/resilience.ts";
            const scenario = (await import(url)) as typeof import("../browser/resilience.js");
            return scenario.closeWhileAdmissionBlocked(name);
          }, name),
        ).toContain("closed");
      } finally {
        await cdp?.send("Page.setWebLifecycleState", { state: "active" });
        await cdp?.detach();
        await first.evaluate(async () => {
          const url = "/packages/core/browser/resilience.ts";
          const scenario = (await import(url)) as typeof import("../browser/resilience.js");
          await scenario.releaseAdmission();
        });
      }
    }

    await Promise.all(
      [first, second].map((page, index) =>
        page.evaluate(async (offset) => {
          const url = "/packages/core/browser/resilience.ts";
          const scenario = (await import(url)) as typeof import("../browser/resilience.js");
          await scenario.tickets(offset);
        }, index * 32),
      ),
    );
    await first.evaluate(async () => {
      const url = "/packages/core/browser/resilience.ts";
      const scenario = (await import(url)) as typeof import("../browser/resilience.js");
      await scenario.stage(1);
    });
    const counts = async () =>
      second.evaluate(async () => {
        const url = "/packages/core/browser/resilience.ts";
        const scenario = (await import(url)) as typeof import("../browser/resilience.js");
        return scenario.counts();
      });
    expect(await counts()).toEqual({ qty: 1000, sales: 0, lines: 0, tickets: 64 });
    await first.evaluate(async () => {
      const url = "/packages/core/browser/resilience.ts";
      const scenario = (await import(url)) as typeof import("../browser/resilience.js");
      await scenario.publish();
      await scenario.stage(2);
      await scenario.killStaged();
    });
    expect(await counts()).toEqual({ qty: 999, sales: 1, lines: 1, tickets: 64 });
    await open(first);
    await first.evaluate(async () => {
      const url = "/packages/core/browser/resilience.ts";
      const scenario = (await import(url)) as typeof import("../browser/resilience.js");
      await scenario.startLoad();
    });
    await expect
      .poll(() =>
        first.evaluate(async () => {
          const url = "/packages/core/browser/resilience.ts";
          const scenario = (await import(url)) as typeof import("../browser/resilience.js");
          return scenario.progress();
        }),
      )
      .toBeGreaterThanOrEqual(5);
    const acknowledged = await first.evaluate(async () => {
      const url = "/packages/core/browser/resilience.ts";
      const scenario = (await import(url)) as typeof import("../browser/resilience.js");
      return scenario.killDuringLoad();
    });
    await open(first);
    const recovered = await counts();
    expect(Number(recovered.sales)).toBeGreaterThanOrEqual(acknowledged + 1);
    expect(Number(recovered.sales)).toBeLessThanOrEqual(acknowledged + 2);
    expect(recovered.lines).toBe(recovered.sales);
    expect(recovered.qty).toBe(1000 - Number(recovered.sales));
    expect(recovered.tickets).toBe(64);
    const receipts = await first.evaluate(async () => {
      const url = "/packages/core/browser/resilience.ts";
      const scenario = (await import(url)) as typeof import("../browser/resilience.js");
      return scenario.receipts();
    });
    const ids = [
      1,
      ...Array.from({ length: Number(recovered.sales) - 1 }, (_, index) => index + 10),
    ];
    expect(receipts.sales).toEqual(ids.map((id) => ({ id, total: 1000 })));
    expect(receipts.lines).toEqual(ids.map((id) => ({ id, sale_id: id, qty: 1 })));

    await Promise.all(
      [first, second].map((page) =>
        page.evaluate(async () => {
          const url = "/packages/core/browser/resilience.ts";
          const scenario = (await import(url)) as typeof import("../browser/resilience.js");
          await scenario.close();
        }),
      ),
    );
  });
}

for (const kind of ["indexeddb", "opfs"] as const) {
  test(`strict ${kind}: reconciles a stable sale ID before and after a lost commit reply`, async ({
    storageContext,
  }) => {
    test.setTimeout(120_000);
    const first = await storageContext.newPage();
    const second = await storageContext.newPage();
    const name = `sale-recovery-${crypto.randomUUID()}`;
    await Promise.all([
      first.goto("/packages/core/browser/"),
      second.goto("/packages/core/browser/"),
    ]);
    if (kind === "opfs") await requireWorkerOpfs(first);
    const open = (page: typeof first) =>
      page.evaluate(
        async ({ name, kind }) => {
          const url = "/packages/core/browser/checkout-recovery.ts";
          await ((await import(url)) as typeof import("../browser/checkout-recovery.js")).open(
            name,
            kind,
          );
        },
        { name, kind },
      );
    await open(first);
    await first.evaluate(async () => {
      const url = "/packages/core/browser/checkout-recovery.ts";
      await ((await import(url)) as typeof import("../browser/checkout-recovery.js")).initialize();
    });
    for (const when of ["before-commit", "after-commit"] as const) {
      const id = `sale-${when}`;
      expect(
        await first.evaluate(
          async ({ id, when }) => {
            const url = "/packages/core/browser/checkout-recovery.ts";
            return (
              (await import(url)) as typeof import("../browser/checkout-recovery.js")
            ).interruptSale(id, when);
          },
          { id, when },
        ),
      ).toEqual({ error: "DatabaseWorkerOutcomeUnknownError", faultObserved: true });
      await open(first);
      await open(second);
      const outcomes = await Promise.all(
        [first, second].map((page) =>
          page.evaluate(async (id) => {
            const url = "/packages/core/browser/checkout-recovery.ts";
            return (
              (await import(url)) as typeof import("../browser/checkout-recovery.js")
            ).recordSale(id);
          }, id),
        ),
      );
      expect(outcomes.filter((outcome) => outcome === "recorded")).toHaveLength(
        when === "before-commit" ? 1 : 0,
      );
      expect(
        await first.evaluate(async (id) => {
          const url = "/packages/core/browser/checkout-recovery.ts";
          try {
            await (
              (await import(url)) as typeof import("../browser/checkout-recovery.js")
            ).recordSale(id, 2000);
            return "accepted";
          } catch (error) {
            return error instanceof Error ? error.message : String(error);
          }
        }, id),
      ).toBe("Sale ID reused with different details");
      await second.evaluate(async () => {
        const url = "/packages/core/browser/checkout-recovery.ts";
        await ((await import(url)) as typeof import("../browser/checkout-recovery.js")).close();
      });
    }
    expect(
      await first.evaluate(async () => {
        const url = "/packages/core/browser/checkout-recovery.ts";
        return ((await import(url)) as typeof import("../browser/checkout-recovery.js")).state();
      }),
    ).toEqual({
      stock: [{ id: 1, qty: 98 }],
      sales: ["sale-after-commit", "sale-before-commit"].map((id) => ({
        id,
        sku: 1,
        qty: 1,
        total: 1000,
      })),
      lines: ["sale-after-commit", "sale-before-commit"].map((id) => ({
        id,
        sale_id: id,
        sku: 1,
        qty: 1,
      })),
      intents: ["sale-after-commit", "sale-before-commit"].map((id) => ({
        id,
        sku: 1,
        qty: 1,
        total: 1000,
        state: "complete",
      })),
      audit: [
        { sku: 1, before_qty: 100, after_qty: 99 },
        { sku: 1, before_qty: 99, after_qty: 98 },
      ],
    });
    await first.evaluate(async () => {
      const url = "/packages/core/browser/checkout-recovery.ts";
      await ((await import(url)) as typeof import("../browser/checkout-recovery.js")).close();
    });
  });
}

for (const kind of ["indexeddb", "opfs"] as const) {
  test(`strict ${kind}: concurrent bursts across 4, 8, and 16 tabs`, async ({
    storageContext,
  }, info) => {
    test.setTimeout(180_000);
    const pages = await Promise.all(Array.from({ length: 16 }, () => storageContext.newPage()));
    const first = pages[0];
    if (first === undefined) throw new Error("Missing first tab");
    const name = `many-tabs-${crypto.randomUUID()}`;
    await Promise.all(pages.map((page) => page.goto("/packages/core/browser/")));
    if (kind === "opfs") await requireWorkerOpfs(first);
    const open = (page: typeof first) =>
      page.evaluate(
        async ({ name, kind }) => {
          const url = "/packages/core/browser/resilience.ts";
          await ((await import(url)) as typeof import("../browser/resilience.js")).open(
            name,
            kind,
            30_000,
          );
        },
        { name, kind },
      );
    await open(first);
    await first.evaluate(async () => {
      const url = "/packages/core/browser/resilience.ts";
      await ((await import(url)) as typeof import("../browser/resilience.js")).initialize();
    });
    await Promise.all(pages.slice(1).map(open));
    let expected = 0;
    let lastRead = 0;
    for (const tabs of [4, 8, 16]) {
      const started = Date.now();
      const progress = { writing: true, readTimedOut: false };
      const writes = Promise.all(
        pages.slice(0, tabs).map((page, index) =>
          page.evaluate(
            async (offset) => {
              const url = "/packages/core/browser/resilience.ts";
              return (
                (await import(url)) as typeof import("../browser/resilience.js")
              ).ticketOutcomes(offset, 8);
            },
            expected + index * 8,
          ),
        ),
      ).finally(() => {
        progress.writing = false;
      });
      const reads = (async () => {
        do {
          const counts = await first.evaluate(async () => {
            const url = "/packages/core/browser/resilience.ts";
            try {
              return await (
                (await import(url)) as typeof import("../browser/resilience.js")
              ).counts();
            } catch (error) {
              // A write deadline closes the transport and also rejects its pending reads.
              if (error instanceof Error && error.name === "DatabaseWorkerTimeoutError")
                return null;
              throw error;
            }
          });
          if (counts === null) {
            progress.readTimedOut = true;
            return;
          }
          expect(Number(counts.tickets)).toBeGreaterThanOrEqual(lastRead);
          expect(Number(counts.tickets)).toBeLessThanOrEqual(expected + tabs * 8);
          expect(counts).toMatchObject({ qty: 1000, sales: 0, lines: 0 });
          lastRead = Number(counts.tickets);
        } while (progress.writing);
      })();
      const [results] = await Promise.all([writes, reads]);
      const { readTimedOut } = progress;
      const elapsedMs = Date.now() - started;
      const outcomes = results.flat();
      const unknown = outcomes.filter((value) => value.outcome === "unknown");
      await info.attach(`contention-${String(tabs)}-tabs`, {
        body: JSON.stringify({
          tabs,
          submitted: outcomes.length,
          unknown: unknown.length,
          readTimedOut,
          elapsedMs,
        }),
        contentType: "application/json",
      });
      console.log(
        JSON.stringify({
          browser: info.project.name,
          kind,
          tabs,
          submitted: outcomes.length,
          unknown: unknown.length,
          readTimedOut,
          elapsedMs,
        }),
      );
      expected += tabs * 8;
      if (unknown.length > 0 || readTimedOut) {
        // A saturated browser can outlast the RPC deadline. Stop every old worker before
        // reconciling stable IDs, so none can race the fresh owner's recovery writes.
        await Promise.all(
          pages.map((page) =>
            page.evaluate(async () => {
              const url = "/packages/core/browser/resilience.ts";
              await ((await import(url)) as typeof import("../browser/resilience.js"))
                .close()
                .catch(() => undefined);
            }),
          ),
        );
        await open(first);
        const durable = await first.evaluate(async () => {
          const url = "/packages/core/browser/resilience.ts";
          return ((await import(url)) as typeof import("../browser/resilience.js")).ticketIds();
        });
        // Every acknowledged write must survive; only unknown writes may be absent.
        const uncertain = new Set(unknown.map((value) => value.id));
        for (let id = 0; id < expected; id++) {
          if (uncertain.has(id)) continue;
          expect(durable).toContain(id);
        }
        for (const { id } of unknown) {
          if (durable.includes(id)) continue;
          await first.evaluate(async (id) => {
            const url = "/packages/core/browser/resilience.ts";
            await ((await import(url)) as typeof import("../browser/resilience.js")).tickets(id, 1);
          }, id);
        }
        await Promise.all(pages.slice(1).map(open));
      }
    }
    expect(
      await first.evaluate(async () => {
        const url = "/packages/core/browser/resilience.ts";
        return ((await import(url)) as typeof import("../browser/resilience.js")).counts();
      }),
    ).toEqual({ qty: 1000, sales: 0, lines: 0, tickets: 224 });
    await Promise.all(
      pages.map((page) =>
        page.evaluate(async () => {
          const url = "/packages/core/browser/resilience.ts";
          await ((await import(url)) as typeof import("../browser/resilience.js")).close();
        }),
      ),
    );
  });
}

test("frozen OPFS leader bounds a follower's wait and resumes without data loss", async ({
  storageContext,
  browserName,
}) => {
  test.skip(browserName !== "chromium", "Page suspension control requires Chromium CDP");
  test.setTimeout(60_000);
  const first = await storageContext.newPage();
  const second = await storageContext.newPage();
  await Promise.all([
    first.goto("/packages/core/browser/"),
    second.goto("/packages/core/browser/"),
  ]);
  const name = `frozen-opfs-${crypto.randomUUID()}`;
  const open = (page: typeof first) =>
    page.evaluate(async (name) => {
      const url = "/packages/core/browser/resilience.ts";
      await ((await import(url)) as typeof import("../browser/resilience.js")).open(
        name,
        "opfs",
        15_000,
        true,
      );
    }, name);
  await open(first);
  await first.evaluate(async () => {
    const url = "/packages/core/browser/resilience.ts";
    await ((await import(url)) as typeof import("../browser/resilience.js")).initialize();
  });
  const browser = storageContext.browser();
  if (browser === null) throw new Error("Missing browser");
  const cdp = await browser.newBrowserCDPSession();
  const targets = await cdp.send("Target.getTargets");
  // Select the only owner worker before opening the follower. Page focus does not identify
  // which worker holds the file handles.
  const owner = targets.targetInfos.find(
    (target) => target.type === "worker" && target.url.includes("frozen-worker.ts"),
  );
  if (owner === undefined) throw new Error("Missing OPFS owner worker target");
  const { sessionId } = await cdp.send("Target.attachToTarget", {
    targetId: owner.targetId,
    flatten: false,
  });
  let nextId = 0;
  const pending = new Map<number, { resolve: () => void; reject: (error: Error) => void }>();
  let markPaused!: () => void;
  const paused = new Promise<void>((resolve) => {
    markPaused = resolve;
  });
  cdp.on("Target.receivedMessageFromTarget", (event) => {
    if (event.sessionId !== sessionId) return;
    const message = JSON.parse(event.message) as {
      id?: number;
      method?: string;
      error?: { message: string };
    };
    if (message.method === "Debugger.paused") markPaused();
    if (message.id === undefined) return;
    const call = pending.get(message.id);
    if (call === undefined) return;
    pending.delete(message.id);
    if (message.error !== undefined) call.reject(new Error(message.error.message));
    else call.resolve();
  });
  const command = (method: string, params = {}) => {
    const id = ++nextId;
    const response = new Promise<void>((resolve, reject) => {
      pending.set(id, { resolve, reject });
    });
    void cdp
      .send("Target.sendMessageToTarget", {
        sessionId,
        message: JSON.stringify({ id, method, params }),
      })
      .catch((error: unknown) => {
        pending.get(id)?.reject(error instanceof Error ? error : new Error(String(error)));
        pending.delete(id);
      });
    return response;
  };
  await open(second);
  await second.evaluate(async () => {
    const url = "/packages/core/browser/resilience.ts";
    await (
      (await import(url)) as typeof import("../browser/resilience.js")
    ).startRecoverySubscription();
  });
  const subscriptionState = () =>
    second.evaluate(async () => {
      const url = "/packages/core/browser/resilience.ts";
      return (
        (await import(url)) as typeof import("../browser/resilience.js")
      ).recoverySubscriptionState();
    });
  await command("Debugger.enable");
  const evaluation = command("Runtime.evaluate", { expression: "debugger;" });
  try {
    await paused;
    const failure = await second.evaluate(async () => {
      const url = "/packages/core/browser/resilience.ts";
      const scenario = (await import(url)) as typeof import("../browser/resilience.js");
      return scenario.recoveryMigrationFailure();
    });
    expect(failure).toEqual({
      transient: true,
      name: "OpfsCoordinationError",
      reason: "leader-unavailable",
    });
    expect(await subscriptionState()).toEqual({ updates: 1, rows: [{ qty: 1000 }], errors: [] });
    const deletion = await second.evaluate(async (name) => {
      const url = "/packages/core/browser/resilience.ts";
      return (
        (await import(url)) as typeof import("../browser/resilience.js")
      ).refuseRecoveryDeletion(name);
    }, name);
    expect(deletion).toBe(true);
  } finally {
    await command("Debugger.resume");
    await evaluation;
    await cdp.send("Target.detachFromTarget", { sessionId });
    await cdp.detach();
  }
  await first.evaluate(async () => {
    const url = "/packages/core/browser/resilience.ts";
    await ((await import(url)) as typeof import("../browser/resilience.js")).changeRecoveryStock();
  });
  await expect
    .poll(subscriptionState, { timeout: 15_000 })
    .toEqual({ updates: 2, rows: [{ qty: 999 }], errors: [] });
  await Promise.all(
    [first, second].map((page) =>
      page.evaluate(async () => {
        const url = "/packages/core/browser/resilience.ts";
        await ((await import(url)) as typeof import("../browser/resilience.js")).close();
      }),
    ),
  );
});

for (const kind of ["indexeddb", "opfs"] as const) {
  test(`strict ${kind}: recovers acknowledged sales after a browser process crash`, async ({
    browserName,
  }, info) => {
    test.skip(browserName !== "chromium", "Browser process crash control requires Chromium CDP");
    test.setTimeout(120_000);
    const profile = info.outputPath("crash-profile");
    const name = `process-crash-${crypto.randomUUID()}`;
    const launch = () =>
      chromium.launchPersistentContext(profile, { baseURL: info.project.use.baseURL ?? "" });
    let context = await launch();
    let page = await context.newPage();
    const open = async () => {
      await page.goto("/packages/core/browser/");
      await page.evaluate(
        async ({ name, kind }) => {
          const url = "/packages/core/browser/resilience.ts";
          await ((await import(url)) as typeof import("../browser/resilience.js")).open(
            name,
            kind,
            30_000,
          );
        },
        { name, kind },
      );
    };
    try {
      await open();
      await page.evaluate(async () => {
        const url = "/packages/core/browser/resilience.ts";
        const scenario = (await import(url)) as typeof import("../browser/resilience.js");
        await scenario.initialize();
        await scenario.startLoad(10_000);
      });
      const progress = () =>
        page.evaluate(async () => {
          const url = "/packages/core/browser/resilience.ts";
          return ((await import(url)) as typeof import("../browser/resilience.js")).progress();
        });
      await expect.poll(progress).toBeGreaterThanOrEqual(5);
      const acknowledged = await progress();
      expect(acknowledged).toBeLessThan(10_000);
      const browser = context.browser();
      if (browser === null) throw new Error("Missing owned browser");
      const cdp = await browser.newBrowserCDPSession();
      const processes = await cdp.send("SystemInfo.getProcessInfo");
      const owned = processes.processInfo.find((info) => info.type === "browser");
      if (owned === undefined || owned.id === process.pid || owned.id <= 0)
        throw new Error("Missing owned browser process");
      // Kill only the isolated browser this test launched, without any graceful cleanup.
      process.kill(owned.id, "SIGKILL");
      await expect.poll(() => browser.isConnected()).toBe(false);
      await context.close().catch(() => undefined);
      context = await launch();
      page = await context.newPage();
      await open();
      const recovered = await page.evaluate(async () => {
        const url = "/packages/core/browser/resilience.ts";
        const scenario = (await import(url)) as typeof import("../browser/resilience.js");
        return { counts: await scenario.counts(), receipts: await scenario.receipts() };
      });
      const sales = Number(recovered.counts.sales);
      expect(sales).toBeGreaterThanOrEqual(acknowledged);
      expect(sales).toBeLessThan(10_000);
      expect(recovered.counts).toEqual({ qty: 1000 - sales, sales, lines: sales, tickets: 0 });
      const ids = Array.from({ length: sales }, (_, index) => index + 10);
      expect(recovered.receipts.sales).toEqual(ids.map((id) => ({ id, total: 1000 })));
      expect(recovered.receipts.lines).toEqual(ids.map((id) => ({ id, sale_id: id, qty: 1 })));
      await page.evaluate(async () => {
        const url = "/packages/core/browser/resilience.ts";
        await ((await import(url)) as typeof import("../browser/resilience.js")).close();
      });
    } finally {
      await context.close().catch(() => undefined);
    }
  });
}

for (const kind of ["indexeddb", "opfs"] as const) {
  test(`strict ${kind}: a native quota failure preserves acknowledged rows and atomicity`, async ({
    browserName,
  }, info) => {
    test.skip(browserName !== "chromium", "Native quota override requires Chromium CDP");
    test.setTimeout(120_000);
    const baseURL = info.project.use.baseURL ?? "";
    const context = await chromium.launchPersistentContext(info.outputPath("quota-profile"), {
      baseURL,
    });
    try {
      const page = await context.newPage();
      await page.goto("/packages/core/browser/");
      const cdp = await context.newCDPSession(page);
      const origin = new URL(baseURL).origin;
      // Set the quota before the first IndexedDB connection can cache a larger allowance.
      await cdp.send("Storage.overrideQuotaForOrigin", { origin, quotaSize: 8 * 1024 * 1024 });
      await page.evaluate(async (kind) => {
        const url = "/packages/core/browser/quota.ts";
        await ((await import(url)) as typeof import("../browser/quota.js")).initialize(kind);
      }, kind);
      const result = await page.evaluate(async () => {
        const url = "/packages/core/browser/quota.ts";
        return ((await import(url)) as typeof import("../browser/quota.js")).fill();
      });
      expect(result.acknowledged).toBeGreaterThan(0);
      expect(result.acknowledged).toBeLessThan(128);
      expect(`${result.failure.name} ${result.failure.message}`).toMatch(/quota/i);
      await cdp.send("Storage.overrideQuotaForOrigin", { origin });
      expect(
        await page.evaluate(async () => {
          const url = "/packages/core/browser/quota.ts";
          return ((await import(url)) as typeof import("../browser/quota.js")).verify();
        }),
      ).toEqual({
        acknowledged: result.acknowledged,
        counter: result.acknowledged,
        rows: result.acknowledged,
        exact: true,
      });
      await cdp.detach();
    } finally {
      await context.close();
    }
  });
}
