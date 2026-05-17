// zerithdb-sdk — public API
export { createApp } from "./create-app.js";
export type { ZerithDBApp } from "./create-app.js";
export {
  LocalCloudBackupAdapter,
  GoogleDriveBackupTarget,
  DropboxBackupTarget,
} from "./db-client.js";
export type {
  BackupExportOptions,
  BackupSnapshot,
  BackupUploadInput,
  BackupUploadResult,
  CloudBackupTarget,
  GoogleDriveBackupTargetOptions,
  DropboxBackupTargetOptions,
  LocalCloudBackupOptions,
} from "./db-client.js";

// Re-export core components with proper typing
export { AuthManager } from "./auth-manager.js";
export { SyncEngine } from "./sync-engine.js";
export { NetworkManager } from "./network-manager.js";

// Re-export commonly used types from zerithdb-core
export type {
  ZerithDBConfig,
  SyncConfig,
  AuthConfig,
  NetworkConfig,
  Document,
  DocumentId,
  CollectionName,
  QueryFilter,
  UpdateSpec,
  InsertResult,
  Identity,
  PeerInfo,
  SyncState,
  CollectionOptions,
  CollectionSchemaOptions,
  ValidationMode,
  SchemaLike,
} from "zerithdb-core";

export { ZerithDBError, ErrorCode, SchemaValidationError } from "zerithdb-errors";
