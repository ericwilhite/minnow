/**
 * Inline suspension choices: charged value copy vs retaining a 64 KiB page lease share.
 * Async-generator kernel, not I/O or whole export. CPU ns/value, five interleaved rounds.
 * A retained lease can in practice cover up to 16 MiB, not just this smallest pool class.
 * npx vite-node scripts/inline-stream-bench.mts
 */
const slab = new Uint8Array(65536).map((_, k) => k % 251);
const rounds = 5;
const count = 100_000;
let consumed = 0;
for (const width of [80, 1024, 8192]) {
  const value = slab.subarray(0, width);
  const samples: Record<string, number[]> = { copy: [], retain: [] };
  for (let round = -1; round < rounds; round++) {
    for (const mode of round % 2 === 0 ? ["copy", "retain"] : ["retain", "copy"]) {
      let charged = 0;
      let shares = 1;
      async function* stream() {
        if (mode === "copy") charged += width;
        else shares++;
        try {
          yield mode === "copy" ? value.slice() : value;
        } finally {
          if (mode === "copy") charged -= width;
          else shares--;
        }
      }
      const before = process.cpuUsage();
      for (let k = 0; k < count; k++) {
        const parts = stream();
        const first = await parts.next();
        if (first.done) throw new Error("no part");
        consumed += first.value[width - 1] ?? 0;
        await parts.return();
      }
      const cpu = process.cpuUsage(before);
      if (charged !== 0 || shares !== 1) throw new Error("leaked reservation");
      if (round >= 0) samples[mode]?.push(((cpu.user + cpu.system) * 1000) / count);
    }
  }
  console.log(
    JSON.stringify({
      width,
      count,
      rounds,
      samples,
      retainedBytes: { copy: width, retain: slab.length },
      summary: Object.fromEntries(
        Object.entries(samples).map(([mode, values]) => {
          values.sort((a, b) => a - b);
          return [mode, { median: values[2], min: values[0], max: values.at(-1) }];
        }),
      ),
      consumed,
    }),
  );
}
