import { describe, expect, it } from "vitest";
import { positionalToNumbered } from "./oracle.js";

describe("SQL oracle parameter translation", () => {
  it("numbers placeholders without rewriting SQL text, identifiers, or comments", () => {
    expect(
      positionalToNumbered(
        `SELECT '?', "why?", ? AS first, 'it''s ?' AS text, ? AS second
         -- a comment ?
         /* another ? */`,
      ),
    ).toBe(
      `SELECT '?', "why?", $1 AS first, 'it''s ?' AS text, $2 AS second
         -- a comment ?
         /* another ? */`,
    );
  });

  it("leaves PostgreSQL dollar-quoted bodies untouched", () => {
    expect(positionalToNumbered("SELECT $$?$$, $body$?$body$, ?")).toBe(
      "SELECT $$?$$, $body$?$body$, $1",
    );
  });

  it("honors escaped strings and nested block comments", () => {
    expect(
      positionalToNumbered(`SELECT E'quote \\'? still text', ? /* outer ? /* inner ? */ ? */`),
    ).toBe(`SELECT E'quote \\'? still text', $1 /* outer ? /* inner ? */ ? */`);
  });
});
