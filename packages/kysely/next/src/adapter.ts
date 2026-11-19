import { DialectAdapterBase } from "kysely";
import type { NativeResultDecoding } from "./driver.js";
export class NativeKyselyAdapter extends DialectAdapterBase {
  constructor(readonly resultDecoding: NativeResultDecoding = {}) {
    super();
  }
  override get supportsMultipleConnections() {
    return true;
  }
  override get supportsReturning() {
    return true;
  }
  acquireMigrationLock(): Promise<void> {
    return Promise.resolve();
  }
  releaseMigrationLock(): Promise<void> {
    return Promise.resolve();
  }
}
