/** Frozen 52722050 peak controls; generated choices vary nesting, key widths and NULL position. */
export const nestedPeakCases = [
  { widths: [43690, 512, 512], unicode: true, nullKey: 0, descending: true, peak: 14414023 },
  { widths: [43690, 512, 512], unicode: false, nullKey: 0, descending: false, peak: 14414643 },
  { widths: [512, 43690, 512], unicode: true, nullKey: 1, descending: true, peak: 14414023 },
  { widths: [512, 512, 43690], unicode: true, nullKey: 2, descending: false, peak: 14414023 },
  { widths: [4096, 4096, 512], unicode: true, nullKey: 0, descending: false, peak: 10925902 },
  { widths: [16384, 512, 64], unicode: false, nullKey: 1, descending: true, peak: 12481197 },
  { widths: [64, 16384, 256], unicode: false, nullKey: 2, descending: false, peak: 12437421 },
  { widths: [256, 512, 16384], unicode: false, nullKey: 0, descending: true, peak: 12524781 },
  { widths: [8192, 256, 8192], unicode: true, nullKey: 2, descending: true, peak: 12184408 },
  { widths: [2048, 2048, 2048], unicode: false, nullKey: 1, descending: false, peak: 10713270 },
  { widths: [32768, 64, 64], unicode: true, nullKey: 0, descending: false, peak: 14673595 },
  { widths: [512, 512, 512], unicode: false, nullKey: 2, descending: true, peak: 10651234 },
] as const;
export function nestedPeakSql(data: (typeof nestedPeakCases)[number]): string {
  const keys = data.widths.map((width, index) => {
    const text = data.unicode ? "汉".repeat(Math.floor(width / 3)) : "x".repeat(width);
    const doc = JSON.stringify({ a: [{ b: text }, [null, 1, 2]], c: { d: true } });
    return `CASE WHEN n=4 AND ${String(index)}=${String(data.nullKey)} THEN NULL ELSE '${doc}'::jsonb END`;
  });
  return `SELECT json_agg(n ORDER BY n ${data.descending ? "DESC" : "ASC"})::text j FROM (VALUES(1),(2),(3),(4))t(n) GROUP BY ${keys.join(",")}`;
}
