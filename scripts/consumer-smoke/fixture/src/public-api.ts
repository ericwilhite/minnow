/**
 * TypeScript resolves every documented package entry from the installed tarballs. This file is
 * deliberately not imported by the runtime smoke: optional testing and adapter-toolkit entries
 * should be checked without making an ordinary browser application download them.
 */
import type * as Core from "@minnowdb/core";
import type * as BlockFormat from "@minnowdb/core/block-format";
import type * as Client from "@minnowdb/core/client";
import type * as Live from "@minnowdb/core/live";
import type * as Plan from "@minnowdb/core/plan";
import type * as Query from "@minnowdb/core/query";
import type * as Schema from "@minnowdb/core/schema";
import type * as SchemaWire from "@minnowdb/core/schema-wire";
import type * as Storage from "@minnowdb/core/storage";
import type * as StorageContracts from "@minnowdb/core/storage/contracts";
import type * as IndexedDb from "@minnowdb/core/storage/indexeddb";
import type * as Memory from "@minnowdb/core/storage/memory";
import type * as Opfs from "@minnowdb/core/storage/opfs";
import type * as Persistence from "@minnowdb/core/storage/persistence";
import type * as Snapshots from "@minnowdb/core/storage/snapshots";
import type * as StorageToolkit from "@minnowdb/core/storage/toolkit";
import type * as Testing from "@minnowdb/core/testing";
import type * as Transactions from "@minnowdb/core/transactions";
import type * as Worker from "@minnowdb/core/worker";
import type * as WorkerHost from "@minnowdb/core/worker-host";
import type * as WorkerProtocol from "@minnowdb/core/worker-protocol";
import type * as Devtools from "@minnowdb/devtools";
import type * as Export from "@minnowdb/export";
import type * as Kysely from "@minnowdb/kysely";
import type * as ReactAdapter from "@minnowdb/react";
import postgresFeatureProfile from "@minnowdb/core/postgres-feature-profile.json" with { type: "json" };
import corePackage from "@minnowdb/core/package.json" with { type: "json" };
import sqlFeatureMatrix from "@minnowdb/core/sql-feature-matrix.json" with { type: "json" };

export type InstalledPublicApis = readonly [
  typeof Core,
  typeof BlockFormat,
  typeof Client,
  typeof Live,
  typeof Plan,
  typeof Query,
  typeof Schema,
  typeof SchemaWire,
  typeof Storage,
  typeof StorageContracts,
  typeof IndexedDb,
  typeof Memory,
  typeof Opfs,
  typeof Persistence,
  typeof Snapshots,
  typeof StorageToolkit,
  typeof Testing,
  typeof Transactions,
  typeof Worker,
  typeof WorkerHost,
  typeof WorkerProtocol,
  typeof Devtools,
  typeof Export,
  typeof Kysely,
  typeof ReactAdapter,
];

export type InstalledPublicJson = readonly [
  typeof postgresFeatureProfile,
  typeof corePackage,
  typeof sqlFeatureMatrix,
];
