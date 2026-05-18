// ─────────────────────────────────────────────────────────────────────────────
// zerithdb-core — Public API
// ─────────────────────────────────────────────────────────────────────────────
export { EventEmitter } from "./internal/event-emitter.js";
export { ZerithDBError, ErrorCode, SchemaValidationError } from "zerithdb-errors";
export { Logger } from "./internal/logger.js";

export { ValidatorRegistry } from "./internal/validator-registry.js";
export type { RegisteredValidator, ValidationResult } from "./internal/validator-registry.js";

export type {
  ZerithDBConfig,
  SyncConfig,
  AuthConfig,
  NetworkConfig,
  DebugConfig,
  ConflictResolverConfig,
} from "./types/config.js";

export type {
  Document,
  DocumentId,
  CollectionName,
  CollectionOptions,
  QueryFilter,
  QueryOptions,
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

export type {
  PeerId,
  PeerInfo,
  RoomId,
  NetworkMessage,
  MediaStreamKind,
  MediaTrackMetadata,
  MediaStreamMetadata,
} from "./types/network.js";

export type { Identity, PublicKey, Signature } from "./types/auth.js";
export type {
  SyncUpdate,
  SyncState,
  AwarenessState,
  SyncPlugin,
  MergePolicy,
  ConflictResolution,
  SyncLog,
  EphemeralPeerState,
  ActiveSpeakerState,
  VideoParticipantState,
} from "./types/sync.js";
