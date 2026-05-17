export * from "./create-app.js";

// Re-export commonly used types from zerithdb-core
export type {
  CollectionSchemaOptions,
  ValidationMode,
  SchemaLike,
} from "zerithdb-core";

export { ZerithDBError, ErrorCode, SchemaValidationError } from "zerithdb-errors";
