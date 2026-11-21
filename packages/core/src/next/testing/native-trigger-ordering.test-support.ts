/** Consumer of B5b's verified concrete native graph. No model row/RI/body adapter. */
import { oracleRows } from "./differential-adapter.js";
import { commandRootFixture } from "./command-root.test-support.js";
import { orderingTrace, type TriggerOrderingFactory } from "./trigger-ordering.test-support.js";
export const nativeOrderingFactory: TriggerOrderingFactory = async (history, script) => {
  if (script.traceKind !== "accumulator")
    throw new TypeError("Native ordering needs the nonvolatile accumulator");
  const fixture = await commandRootFixture(
    [...script.nativeSetup, ...script.seed],
    script.mutation,
    history.published ?? false,
    64 << 20,
    true,
  );
  const readState = async (sql: string) => {
    const output = await fixture.core.execute(sql);
    if (!("batches" in output)) throw new TypeError("Missing native trigger query result");
    return oracleRows(output);
  };
  return {
    stateQueries: [
      "SELECT * FROM p ORDER BY id",
      "SELECT * FROM c ORDER BY id",
      "SELECT label,n FROM log ORDER BY label,n",
      "SELECT label,n FROM audit ORDER BY label,n",
    ],
    readState,
    async run() {
      const affected = await fixture.root.run({ slots: [] });
      if (affected !== history.rows) throw new Error("Native trigger root count mismatch");
      await fixture.commit();
      await fixture.retire();
      return orderingTrace(
        script,
        (await readState(script.traceSql)).map((row) => ({ s: row.s })),
      );
    },
    close: () => fixture.close(),
  };
};
