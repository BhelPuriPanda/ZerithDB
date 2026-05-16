// ─────────────────────────────────────────────────────────────────────────────
// zerithdb-core — Public API
// ─────────────────────────────────────────────────────────────────────────────

export { EventEmitter } from "./internal/event-emitter.js";
export { ZerithDBError, ErrorCode, SchemaValidationError } from "./internal/errors.js";
export { ValidatorRegistry } from "./internal/validator-registry.js";
export type { RegisteredValidator, ValidationResult } from "./internal/validator-registry.js";
export type { ZerithDBConfig, SyncConfig, AuthConfig, NetworkConfig } from "./types/config.js";
export type {
  Document,
  DocumentId,
  CollectionName,
  QueryFilter,
  UpdateSpec,
  InsertResult,
  FindResult,
  CollectionOptions,
} from "./types/db.js";
export type {
  SchemaLike,
  SafeParseResult,
  ValidationMode,
  CollectionSchemaOptions,
} from "./types/validation.js";
export type { PeerId, PeerInfo, RoomId, NetworkMessage } from "./types/network.js";
export type { Identity, PublicKey, Signature } from "./types/auth.js";
export type { SyncUpdate, SyncState, AwarenessState } from "./types/sync.js";
