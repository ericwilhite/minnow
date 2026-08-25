// This indirection makes the browser suites resolve the same package export applications use.
// Importing the source entry directly would let a stale or missing `./worker` export go unnoticed.
import "@minnowdb/core/worker";
