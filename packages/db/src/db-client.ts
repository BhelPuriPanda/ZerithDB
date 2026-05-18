import { Dexie, type Table, liveQuery } from "dexie";
import { v7 as uuidv7 } from "uuid";

import type {
  ZerithDBConfig,
  Document,
  QueryFilter,
  QueryOptions,
  InsertResult,
  UpdateSpec,
  ValidatorRegistry,
} from "zerithdb-core";

import {
  ZerithDBError,
  ErrorCode,
  SchemaValidationError,
} from "zerithdb-errors";

import { wrapIDBOperation } from "./internal/wrap-idb-operation.js";
import { EventEmitter } from "zerithdb-core";
import type { BackupExportOptions, BackupSnapshot } from "./backup.js";
import { GraphClient } from "./graph-client.js";
import type { GraphNode, GraphEdge } from "zerithdb-core";

/**
 * Internal Dexie subclass that manages dynamic collection creation.
 * Collections are added lazily via schema version upgrades.
 */
class ZerithDBDexie extends Dexie {
  private readonly tableMap = new Map<string, Table>();
  private _currentSchema: Record<string, string> = {};
  private _pendingVersion = 0;

  constructor(appId: string) {
    super(`zerithdb_${appId}`);
  }

  /**
   * Ensure a named collection exists, creating it via a Dexie version
   * upgrade if it has not been registered yet.
   *
   * @param name - The collection name to create or retrieve
   * @returns The Dexie Table handle for the collection
   */
  ensureCollection(name: string): Table {
    if (!this.tableMap.has(name)) {
      this._currentSchema[name] = "_id, _createdAt, _updatedAt";

      const nextVersion = Math.max(this.verno, this._pendingVersion) + 1;
      this._pendingVersion = nextVersion;

      if (this.isOpen()) {
        this.close();
      }

      this.version(nextVersion).stores(this._currentSchema);
      this.tableMap.set(name, this.table(name));
    }

    return this.tableMap.get(name)!;
  }

  ensureGraphTables(
    graphName: string
  ): { nodesTable: Table; edgesTable: Table } {
    const nodesKey = `__graph_nodes_${graphName}`;
    const edgesKey = `__graph_edges_${graphName}`;

    if (!this.tableMap.has(nodesKey) || !this.tableMap.has(edgesKey)) {
      this._currentSchema[nodesKey] = "_id, _createdAt, _updatedAt";
      this._currentSchema[edgesKey] =
        "_id, from, to, label, _createdAt";

      const nextVersion = Math.max(this.verno, this._pendingVersion) + 1;
      this._pendingVersion = nextVersion;

      if (this.isOpen()) {
        this.close();
      }

      this.version(nextVersion).stores(this._currentSchema);

      this.tableMap.set(nodesKey, this.table(nodesKey));
      this.tableMap.set(edgesKey, this.table(edgesKey));
    }

    return {
      nodesTable: this.tableMap.get(nodesKey)!,
      edgesTable: this.tableMap.get(edgesKey)!,
    };
  }
}

/**
 * Client for a specific collection.
 * Provides CRUD operations with optional schema validation.
 */
export class CollectionClient<
  T extends Record<string, any> = Record<string, any>
> {
  constructor(
    private readonly dexie: ZerithDBDexie,
    private readonly collectionName: string,
    private readonly validatorRegistry?: ValidatorRegistry,
    private readonly onValidationError?: (
      error: SchemaValidationError
    ) => void,
    private readonly auth?: any
  ) {}

  /**
   * Internal Dexie table accessor
   */
  private get table(): Table<Document<T>> {
    return this.dexie.table(this.collectionName);
  }

  private async checkBiometric(
    operationDescription: string
  ): Promise<void> {
    if (this.auth?.biometric?.isBiometricRequiredForDB()) {
      const authorized = await this.auth.biometric.promptBiometric(
        `Authorize sensitive database operation: ${operationDescription} in collection "${this.collectionName}"`
      );

      if (!authorized) {
        throw new ZerithDBError(
          ErrorCode.AUTH_SIGN_FAILED,
          "Database operation cancelled or biometric authentication failed."
        );
      }
    }
  }

  private runValidation(data: unknown, batchIndex?: number): void {
    if (!this.validatorRegistry) return;

    const result = this.validatorRegistry.validate(
      this.collectionName,
      data
    );

    if (result.valid) return;

    const prefix =
      batchIndex !== undefined
        ? `Batch validation failed at index ${batchIndex} in`
        : `Validation failed in`;

    const error = new SchemaValidationError(
      ErrorCode.DB_VALIDATION_FAILED,
      `${prefix} "${this.collectionName}": ${result.issues
        .map((i) => i.message)
        .join(", ")}`,
      result.issues
    );

    this.onValidationError?.(error);

    if (result.shouldThrow) {
      throw error;
    }
  }

  /**
   * Subscribe to changes in the collection.
   * Uses Dexie's liveQuery to reactively notify when documents change.
   */
  subscribe(callback: (documents: Document<T>[]) => void): () => void {
    const observable = liveQuery(() => this.find({}));

    const subscription = observable.subscribe({
      next: (docs) => callback(docs as Document<T>[]),
      error: (err) =>
        console.error(
          `Subscription error in "${this.collectionName}":`,
          err
        ),
    });

    return () => subscription.unsubscribe();
  }

  async insert(document: T): Promise<InsertResult> {
    if (document === null || document === undefined) {
      throw new ZerithDBError(
        ErrorCode.DB_WRITE_FAILED,
        "Document cannot be null or undefined"
      );
    }

    this.runValidation(document);
    await this.checkBiometric("Insert Document");

    const now = Date.now();
    const id = uuidv7();

    const doc: Document<T> = {
      ...docToInsert,
      _id: id,
      _createdAt: now,
      _updatedAt: now,
      _vclock: { [this.peerId]: 1 },
      _lamport: now,
      _deleted: false,
    };

    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to insert into collection "${this.collectionName}"`,
      async () => {
        await this.table.add(doc);
        this.emit("mutation", { collectionName: this.collectionName, doc, type: "insert" });
        return { id };
      }
    );
  }

  async insertMany(documents: T[]): Promise<InsertResult[]> {
    if (!Array.isArray(documents) || documents.length === 0) {
      throw new ZerithDBError(
        ErrorCode.DB_WRITE_FAILED,
        "Documents must be a non-empty array"
      );
    }

    await this.checkBiometric("Bulk Insert Documents");

    for (let i = 0; i < documents.length; i++) {
      const doc = documents[i];

      if (doc === null || doc === undefined) {
        throw new ZerithDBError(
          ErrorCode.DB_WRITE_FAILED,
          "Documents array cannot contain null or undefined"
        );
      }

      this.runValidation(doc, i);
    }

    const now = Date.now();

    const docs = documents.map((doc) => ({
      ...doc,
      _id: ids[i]!,
      _createdAt: now,
      _updatedAt: now,
      _vclock: { [this.peerId]: 1 },
      _lamport: now,
      _deleted: false,
    })) as Document<T>[];
    if (!documents || documents.length === 0) {
      throw new ZerithDBError(ErrorCode.DB_WRITE_FAILED, "insertMany requires a non-empty array");
    }
    if (documents.some((d) => (d as any) === null || (d as any) === undefined)) {
      throw new ZerithDBError(ErrorCode.DB_WRITE_FAILED, "insertMany array must not contain null or undefined");
    }
    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to bulk insert into collection "${this.collectionName}"`,
      async () => {
        await this.table.bulkAdd(docs);

        return docs.map((d) => ({
          id: d._id,
        }));
      }

      return results;
    } catch (err) {
      throw new ZerithDBError(
        ErrorCode.DB_WRITE_FAILED,
        `Failed to bulk insert into collection "${this.collectionName}"`,
        { cause: err }
      );
    }
  }

  /**
   * Find documents matching a filter.
   * All filter fields are ANDed together.
   */
  async find(
    filter: QueryFilter<T> = {},
    options: QueryOptions<T> = {}
  ): Promise<Document<T>[]> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to query collection "${this.collectionName}"`,
      async () => {
        const all = await this.table.toArray();
        const compiledFilter = this.precompileRegexes(filter);
        const results: Document<T>[] = [];

        await this.table.each((doc) => {
          if (this.matchesFilter(doc, compiledFilter)) {
            results.push(doc);
          }
        });

        if (options.sort) {
          const { field, order = "asc" } = options.sort;

          results.sort((a, b) => {
            const aValue = a[field];
            const bValue = b[field];

            if (aValue === bValue) return 0;

            if (aValue == null) return 1;
            if (bValue == null) return -1;

            const comparison = String(aValue).localeCompare(
              String(bValue),
              undefined,
              {
                numeric: true,
                sensitivity: "base",
              }
            );

            return order === "desc" ? -comparison : comparison;
          });
        }

        const skip = options.skip ?? options.offset ?? 0;
        const limit = options.limit ?? Number.POSITIVE_INFINITY;

        return results.slice(skip, skip + limit);
      }
    );
  }

  async findById(id: string): Promise<Document<T> | undefined> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to get document "${id}" from "${this.collectionName}"`,
      async () => {
        const doc = await this.table.get(id);
        return doc && !doc._deleted ? doc : undefined;
      }
    );
    if (!doc) return undefined;
    return this.restoreIpfsReferences(doc);
  }

  async update(
    filter: QueryFilter<T>,
    spec: UpdateSpec<T>
  ): Promise<number> {
    if (
      !spec ||
      Object.keys(spec).length === 0 ||
      ((!spec.$set || Object.keys(spec.$set).length === 0) &&
        (!spec.$unset || Object.keys(spec.$unset).length === 0))
    ) {
      throw new ZerithDBError(
        ErrorCode.DB_WRITE_FAILED,
        "Update spec cannot be empty. Must provide non-empty $set or $unset."
      );
    }

    await this.checkBiometric("Update Documents");

    try {
      const matches = await this.find(filter);
      const now = Date.now();

      const updatedDocs = matches.map((doc) =>
        this.applyUpdateSpec(doc, spec, now)
      );

      for (const doc of updatedDocs) {
        this.runValidation(doc);
      }

      await this.table.bulkPut(updatedDocs);

      return matches.length;
    } catch (err) {
      if (
        err instanceof SchemaValidationError ||
        (err instanceof Error &&
          err.name === "SchemaValidationError")
      ) {
        throw err;
      }

      throw new ZerithDBError(
        ErrorCode.DB_WRITE_FAILED,
        `Failed to update documents in "${this.collectionName}"`,
        { cause: err }
      );
    }
  }

  async delete(filter: QueryFilter<T>): Promise<number> {
    await this.checkBiometric("Delete Documents");

    return wrapIDBOperation(
      ErrorCode.DB_DELETE_FAILED,
      `Failed to delete documents from "${this.collectionName}"`,
      async () => {
        const matches = await this.find(filter);

        await this.table.bulkDelete(matches.map((d) => d._id));

        return matches.length;
      }

      return deletedCount;
    } catch (err) {
      throw new ZerithDBError(
        ErrorCode.DB_DELETE_FAILED,
        `Failed to delete documents from "${this.collectionName}"`,
        { cause: err }
      );
    }
  }

  async clearAll(): Promise<void> {
    await this.checkBiometric("Clear Collection");

    return wrapIDBOperation(
      ErrorCode.DB_DELETE_FAILED,
      `Failed to clear collection "${this.collectionName}"`,
      async () => {
        await this.table.clear();
        // Reset the integer sequence so IDs restart from 1 after a clear
        if (this.idStrategy === "autoincrement") {
          await this.seqTable.delete(this.collectionName);
        }
      }
    );
  }

  async clear(): Promise<void> {
    return this.clearAll();
  }

  async count(filter: QueryFilter<T> = {}): Promise<number> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to count documents in "${this.collectionName}"`,
      async () => {
        const compiledFilter = this.precompileRegexes(filter);

        let total = 0;

        await this.table.each((doc) => {
          if (this.matchesFilter(doc, compiledFilter)) {
            total++;
          }
        });

        return total;
      }
    );
  }

  private applyUpdateSpec(
    doc: Document<T>,
    spec: UpdateSpec<T>,
    updatedAt: number
  ): Document<T> {
    const next = {
      ...doc,
      ...(spec.$set ?? {}),
      _updatedAt: updatedAt,
    } as Record<string, any>;

    for (const key of Object.keys(spec.$unset ?? {})) {
      delete next[key];
    }

    next._id = doc._id;
    next._createdAt = doc._createdAt;
    next._updatedAt = updatedAt;

    return next as Document<T>;
  }

  private matchesFilter(
    doc: Document<T>,
    filter: QueryFilter<T>
  ): boolean {
    for (const [key, condition] of Object.entries(filter)) {
      const fieldValue = (doc as Record<string, any>)[key];

      if (condition === null || typeof condition !== "object") {
        if (fieldValue !== condition) return false;
        continue;
      }

      const conditions = condition as Record<string, any>;

      const isOperatorObject = Object.keys(conditions).some((k) =>
        k.startsWith("$")
      );

      if (!isOperatorObject) {
        if (
          JSON.stringify(fieldValue) !== JSON.stringify(condition)
        ) {
          return false;
        }

        continue;
      }

      if (
        "$eq" in conditions &&
        fieldValue !== conditions["$eq"]
      )
        return false;

      if (
        "$ne" in conditions &&
        fieldValue === conditions["$ne"]
      )
        return false;

      if (
        "$gt" in conditions &&
        !(fieldValue > conditions["$gt"])
      )
        return false;

      if (
        "$gte" in conditions &&
        !(fieldValue >= conditions["$gte"])
      )
        return false;

      if (
        "$lt" in conditions &&
        !(fieldValue < conditions["$lt"])
      )
        return false;

      if (
        "$lte" in conditions &&
        !(fieldValue <= conditions["$lte"])
      )
        return false;

      if (
        "$in" in conditions &&
        !(conditions["$in"] as unknown[]).includes(fieldValue)
      )
        return false;

      if (
        "$nin" in conditions &&
        (conditions["$nin"] as unknown[]).includes(fieldValue)
      )
        return false;

      if ("$exists" in conditions) {
        const exists = key in doc;

        if (conditions.$exists !== exists) {
          return false;
        }
      }

      if ("$regex" in conditions) {
        if (typeof fieldValue !== "string") {
          return false;
        }

        const regex = conditions.$regex;

        if (!(regex instanceof RegExp)) {
          return false;
        }

        regex.lastIndex = 0;

        if (!regex.test(fieldValue)) {
          return false;
        }
      }
    }

    return true;
  }

  private precompileRegexes(
    filter: QueryFilter<T>
  ): QueryFilter<T> {
    const compiled: Record<string, any> = {};

    for (const [key, condition] of Object.entries(filter)) {
      if (condition !== null && typeof condition === "object") {
        const conditions = {
          ...condition,
        } as Record<string, any>;

        const isOperatorObject = Object.keys(conditions).some((k) =>
          k.startsWith("$")
        );

        if (isOperatorObject && "$regex" in conditions) {
          conditions["$regex"] =
            this.compileRegexCondition(conditions);
        }

        compiled[key] = conditions;
      } else {
        compiled[key] = condition;
      }
    }

    return compiled as QueryFilter<T>;
  }

  private compileRegexCondition(
    conditions: Record<string, any>
  ): RegExp | null {
    const rawRegex = conditions.$regex;

    const rawFlags =
      typeof conditions.$flags === "string"
        ? conditions.$flags
        : typeof conditions.$options === "string"
          ? conditions.$options
          : undefined;

    try {
      if (rawRegex instanceof RegExp) {
        if (!rawFlags) {
          return rawRegex;
        }

        const mergedFlags = Array.from(
          new Set((rawRegex.flags + rawFlags).split(""))
        ).join("");

        return new RegExp(rawRegex.source, mergedFlags);
      }

      if (typeof rawRegex === "string") {
        return new RegExp(rawRegex, rawFlags);
      }

      return null;
    } catch {
      return null;
    }
  }
}

// ---------------------------------------------------------------------------
// Internal Dexie subclass
// ---------------------------------------------------------------------------

/**
 * Internal database client.
 * Wraps Dexie and manages collection instances.
 */
export class DbClient extends EventEmitter<{ "mutation": { collection: string } }> {
  private readonly dexie: ZerithDBDexie;
  private readonly appId: string;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly collections = new Map<
    string,
    CollectionClient<any>
  >();

  private readonly graphs = new Map<string, GraphClient<any>>();

  private validatorRegistry?: ValidatorRegistry;

  constructor(
    config: ZerithDBConfig,
    private readonly auth?: any
  ) {
    this.appId = config.appId;
    this.dexie = new ZerithDBDexie(config.appId);
    // Simplified peerId generation - in production this should be stable
    this.peerId = uuidv7();
  }

  setValidatorRegistry(registry: ValidatorRegistry): void {
    this.validatorRegistry = registry;
  }

  collection<T extends Record<string, any>>(
    name: string
  ): CollectionClient<T> {
    if (typeof name !== "string" || name.trim() === "") {
      throw new ZerithDBError(
        ErrorCode.DB_INIT_FAILED,
        "Collection name must be a non-empty string"
      );
    }

    if (!this.collections.has(name)) {
      this.dexie.ensureCollection(name);

      this.collections.set(
        name,
        new CollectionClient<T>(
          this.dexie,
          name,
          this.validatorRegistry,
          undefined,
          this.auth
        )
      );
    }

    return this.collections.get(name) as CollectionClient<T>;
  }

  graph<T extends Record<string, any> = Record<string, any>>(
    name: string
  ): GraphClient<T> {
    if (!this.graphs.has(name)) {
      const { nodesTable, edgesTable } =
        this.dexie.ensureGraphTables(name);

      this.graphs.set(
        name,
        new GraphClient<T>(
          nodesTable as Table<GraphNode<T>>,
          edgesTable as Table<GraphEdge>,
          name
        )
      );
    }

    return this.graphs.get(name) as GraphClient<T>;
  }

  async getMemoryStats(): Promise<{
    recordCount: number;
    collections: Record<string, number>;
  }> {
    const collections: Record<string, number> = {};
    let recordCount = 0;

    for (const [key, client] of this.collections) {
      // Strip the ":uuid" / ":autoincrement" suffix for the stat label
      const name = key.split(":")[0]!;
      const count = await client.count();

      collections[name] = count;
      recordCount += count;
    }

    return { recordCount, collections };
  }

  collectionNames(): string[] {
    // Deduplicate in case same collection opened with different strategies
    return [...new Set(Array.from(this.collections.keys()).map((k) => k.split(":")[0]!))];
  }

  allCollectionNames(): string[] {
    return this.dexie.tables.map((t) => t.name).filter((name) => !name.startsWith("_"));
  }

  async exportSnapshot(
    options: BackupExportOptions = {}
  ): Promise<BackupSnapshot> {
    if (this.auth?.biometric?.isBiometricRequiredForDB()) {
      const authorized =
        await this.auth.biometric.promptBiometric(
          "Authorize sensitive operation: Export full database backup snapshot"
        );

      if (!authorized) {
        throw new ZerithDBError(
          ErrorCode.AUTH_SIGN_FAILED,
          "Database export cancelled or biometric authentication failed."
        );
      }
    }

    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      "Failed to export local backup snapshot",
      async () => {
        const collectionNames =
          options.collections ?? this.allCollectionNames();

        const collections: BackupSnapshot["collections"] = {};

        for (const name of collectionNames) {
          const table = this.dexie.ensureCollection(name);

          collections[name] =
            (await table.toArray()) as Document<
              Record<string, any>
            >[];
        }

        return {
          format: "zerithdb.local-backup.v1",
          appId: this.appId,
          generatedAt: new Date().toISOString(),
          collections,
        };
      }
    );
  }

  async dispose(): Promise<void> {
    // Remove all EventEmitter listeners before closing to prevent memory leaks
    // from dangling references to this DbClient instance after disposal.
    this.removeAllListeners();
    this.dexie.close();
  }
}