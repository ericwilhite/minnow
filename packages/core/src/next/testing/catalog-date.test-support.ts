/** Hostile instance methods must not change the Date's internal value. */
export const dateOverrides = ["none", "epoch", "throw"] as const;
export function catalogDate(milliseconds: number, override: (typeof dateOverrides)[number]): Date {
  const date = new Date(milliseconds);
  if (override !== "none")
    Object.defineProperties(date, {
      getTime: {
        value: () => {
          if (override === "throw") throw new Error("Caller getTime must not run");
          return 0;
        },
      },
      toISOString: {
        value: () => {
          if (override === "throw") throw new Error("Caller toISOString must not run");
          return "1970-01-01T00:00:00.000Z";
        },
      },
    });
  return date;
}
