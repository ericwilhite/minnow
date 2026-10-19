import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const installer = fileURLToPath(new URL("./ci-install.sh", import.meta.url));

function install(failures: number, error: string) {
  const root = mkdtempSync(join(tmpdir(), "minnow-ci-install-"));
  try {
    writeFileSync(
      join(root, "npm"),
      `#!/usr/bin/env bash
set -eu
[ "$*" = ci ]
count=0
if [ -f "$INSTALL_COUNTER" ]; then read -r count < "$INSTALL_COUNTER"; fi
count=$((count + 1))
echo "$count" > "$INSTALL_COUNTER"
if [ "$count" -le "$INSTALL_FAILURES" ]; then
  echo "$INSTALL_ERROR" >&2
  exit 42
fi
echo "installed"
`,
      { mode: 0o755 },
    );
    writeFileSync(join(root, "sleep"), '#!/usr/bin/env bash\necho "$1" >> "$INSTALL_DELAYS"\n', {
      mode: 0o755,
    });
    const counter = join(root, "counter");
    const delays = join(root, "delays");
    writeFileSync(delays, "");
    const result = spawnSync("bash", [installer], {
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${root}${delimiter}${process.env.PATH ?? ""}`,
        INSTALL_COUNTER: counter,
        INSTALL_DELAYS: delays,
        INSTALL_FAILURES: String(failures),
        INSTALL_ERROR: error,
      },
    });
    return {
      status: result.status,
      output: result.stdout + result.stderr,
      attempts: Number(readFileSync(counter, "utf8")),
      delays: readFileSync(delays, "utf8"),
    };
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

describe("CI dependency installation", () => {
  it("installs successfully without retries", () => {
    expect(install(0, "")).toEqual({ status: 0, output: "installed\n", attempts: 1, delays: "" });
  });

  it.each(["npm error code ECONNRESET", "npm ERR! code EAI_AGAIN"])(
    "recovers from a transient network failure: %s",
    (error) => {
      const result = install(1, error);
      expect(result.status).toBe(0);
      expect(result.attempts).toBe(2);
      expect(result.delays).toBe("5\n");
      expect(result.output).toContain("installed");
    },
  );

  it("preserves a deterministic installation failure without retrying", () => {
    const result = install(9, "npm error code EUSAGE\nnpm error lockfile mismatch");
    expect(result.status).toBe(42);
    expect(result.attempts).toBe(1);
    expect(result.delays).toBe("");
  });

  it("fails after three interrupted downloads and preserves the error status", () => {
    const result = install(9, "npm error code ECONNRESET");
    expect(result.status).toBe(42);
    expect(result.attempts).toBe(3);
    expect(result.delays).toBe("5\n10\n");
  });
});
