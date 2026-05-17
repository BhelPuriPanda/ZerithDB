import Dexie, { type Table, liveQuery } from "dexie";
import { v7 as uuidv7 } from "uuid";

import type {
  ZerithDBConfig,
  Document,
  QueryFilter,
  InsertResult,
  UpdateSpec,
  ValidatorRegistry,
} from "zerithdb-core";

import { ZerithDBError, ErrorCode, SchemaValidationError } from "zerithdb-errors";

import { wrapIDBOperation } from "./internal/wrap-idb-operation.js";
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

  ensureCollection(name: string): Table {
    if (!this.tableMap.has(name)) {
      this._currentSchema[name] = "_id, _createdAt, _updatedAt";

      // We must increment the version for every new collection added dynamically
      const nextVersion = Math.max(this.verno, this._pendingVersion) + 1;
      this._pendingVersion = nextVersion;

      if (this.isOpen()) {
        this.close();
      }

      this.version(nextVersion).stores(this._currentSchema);
      this.tableMap.set(name, this.table(name));
    }

    return this.table(name);
  }

  ensureGraphTables(graphName: string): { nodesTable: Table; edgesTable: Table } {
    const nodesKey = `__graph_nodes_${graphName}`;
    const edgesKey = `__graph_edges_${graphName}`;

    if (!this.tableMap.has(nodesKey) || !this.tableMap.has(edgesKey)) {
      this._currentSchema[nodesKey] = "_id, _createdAt, _updatedAt";
      this._currentSchema[edgesKey] = "_id, from, to, label, _createdAt";

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
export class CollectionClient<T extends Record<string, any> = Record<string, any>> {
  constructor(
    private readonly dexie: ZerithDBDexie,
    private readonly collectionName: string,
    private readonly validatorRegistry?: ValidatorRegistry,
    private readonly onValidationError?: (error: SchemaValidationError) => void
  ) {}

  /**
   * Internal Dexie table accessor
   */
  private get table(): Table<Document<T>> {
    return this.dexie.table(this.collectionName);
  }

  /**
   * Subscribe to changes in the collection.
   * Uses Dexie's liveQuery to reactively notify when documents change.
   */
  subscribe(callback: (documents: Document<T>[]) => void): () => void {
    const observable = liveQuery(() => this.find({}));

    const subscription = observable.subscribe({
      next: (docs) => callback(docs as Document<T>[]),
      error: (err) => console.error(`Subscription error in "${this.collectionName}":`, err),
    });

    return () => subscription.unsubscribe();
  }

  async insert(document: T): Promise<InsertResult> {
    if (document === null || document === undefined) {
      throw new ZerithDBError(ErrorCode.DB_WRITE_FAILED, "Document cannot be null or undefined");
    }
    this.runValidation(document);
    const now = Date.now();
    const id = uuidv7();

    const doc: Document<T> = {
      ...document,
      _id: id,
      _createdAt: now,
      _updatedAt: now,
    };

    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to insert into collection "${this.collectionName}"`,
      async () => {
        await this.table.add(doc);
        return { id };
      }
    );
  }

  async insertMany(documents: T[]): Promise<InsertResult[]> {
    if (!Array.isArray(documents) || documents.length === 0) {
      throw new ZerithDBError(ErrorCode.DB_WRITE_FAILED, "Documents must be a non-empty array");
    }
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
      _id: uuidv7(),
      _createdAt: now,
      _updatedAt: now,
    })) as Document<T>[];

    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to bulk insert into collection "${this.collectionName}"`,
      async () => {
        await this.table.bulkAdd(docs);
        return docs.map((d) => ({ id: d._id }));
      }
    );
  }

  async find(filter: QueryFilter<T> = {}): Promise<Document<T>[]> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to query collection "${this.collectionName}"`,
      async () => {
        const all = await this.table.toArray();
        const compiledFilter = this.precompileRegexes(filter);
        return all.filter((doc) => this.matchesFilter(doc, compiledFilter));
      }
    );
  }

  async findById(id: string): Promise<Document<T> | undefined> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to get document "${id}" from "${this.collectionName}"`,
      () => this.table.get(id)
    );
  }

  async update(filter: QueryFilter<T>, spec: UpdateSpec<T>): Promise<number> {
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

    try {
      const matches = await this.find(filter);
      const now = Date.now();
      const updatedDocs = matches.map((doc) => this.applyUpdateSpec(doc, spec, now));

      for (const doc of updatedDocs) {
        this.runValidation(doc);
      }

      await this.table.bulkPut(updatedDocs);
      return matches.length;
    } catch (err) {
      if (
        err instanceof SchemaValidationError ||
        (err instanceof Error && err.name === "SchemaValidationError")
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
    return wrapIDBOperation(
      ErrorCode.DB_DELETE_FAILED,
      `Failed to delete documents from "${this.collectionName}"`,
      async () => {
        const matches = await this.find(filter);
        await this.table.bulkDelete(matches.map((d) => d._id));
        return matches.length;
      }
    );
  }

  async clearAll(): Promise<void> {
    return wrapIDBOperation(
      ErrorCode.DB_DELETE_FAILED,
      `Failed to clear collection "${this.collectionName}"`,
      () => this.table.clear()
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

  private applyUpdateSpec(doc: Document<T>, spec: UpdateSpec<T>, updatedAt: number): Document<T> {
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

    return next as Document<T>;
  }

  private matchesFilter(doc: Document<T>, filter: QueryFilter<T>): boolean {
    for (const [key, condition] of Object.entries(filter)) {
      const fieldValue = (doc as Record<string, any>)[key];

      if (condition === null || typeof condition !== "object") {
        if (fieldValue !== condition) return false;
        continue;
      }

      const conditions = condition as Record<string, any>;
      const isOperatorObject = Object.keys(conditions).some((k) => k.startsWith("$"));

      if (!isOperatorObject) {
        if (JSON.stringify(fieldValue) !== JSON.stringify(condition)) {
          return false;
        }
        continue;
      }

      if ("$eq" in conditions && fieldValue !== conditions["$eq"]) return false;
      if ("$ne" in conditions && fieldValue === conditions["$ne"]) return false;
      if ("$gt" in conditions && !(fieldValue > conditions["$gt"])) return false;
      if ("$gte" in conditions && !(fieldValue >= conditions["$gte"])) return false;
      if ("$lt" in conditions && !(fieldValue < conditions["$lt"])) return false;
      if ("$lte" in conditions && !(fieldValue <= conditions["$lte"])) return false;
      if ("$in" in conditions && !(conditions["$in"] as unknown[]).includes(fieldValue))
        return false;
      if ("$nin" in conditions && (conditions["$nin"] as unknown[]).includes(fieldValue))
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

        const regex =
          conditions.$regex instanceof RegExp ? conditions.$regex : new RegExp(conditions.$regex);

        regex.lastIndex = 0;

        if (!regex.test(fieldValue)) {
          return false;
        }
      }
    }

    return true;
  }

  private precompileRegexes(filter: QueryFilter<T>): QueryFilter<T> {
    const compiled: Record<string, any> = {};
    for (const [key, condition] of Object.entries(filter)) {
      if (condition !== null && typeof condition === "object") {
        const conditions = { ...condition } as Record<string, any>;
        const isOperatorObject = Object.keys(conditions).some((k) => k.startsWith("$"));
        if (isOperatorObject && "$regex" in conditions) {
          const regex = conditions["$regex"];
          conditions["$regex"] = regex instanceof RegExp ? regex : new RegExp(regex);
        }
        compiled[key] = conditions;
      } else {
        compiled[key] = condition;
      }
    }
    return compiled as QueryFilter<T>;
  }

  private runValidation(data: unknown, batchIndex?: number): void {
    if (!this.validatorRegistry) return;

    const result = this.validatorRegistry.validate(this.collectionName, data);

    if (result.valid) return;

    const prefix =
      batchIndex !== undefined
        ? `Batch validation failed at index ${batchIndex} in`
        : `Validation failed in`;

    const error = new SchemaValidationError(
      ErrorCode.DB_VALIDATION_FAILED,
      `${prefix} "${this.collectionName}": ${result.issues.map((i) => i.message).join(", ")}`,
      result.issues
    );

    this.onValidationError?.(error);

    if (result.shouldThrow) {
      throw error;
    }
  }
}
/**
 * Internal database client.
 * Wraps Dexie and manages collection instances.
 */
export class DbClient {
  private readonly dexie: ZerithDBDexie;
  private readonly appId: string;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly collections = new Map<string, CollectionClient<any>>();

  private validatorRegistry?: ValidatorRegistry;
  private readonly graphs = new Map<string, GraphClient<any>>();

  constructor(config: ZerithDBConfig) {
    this.appId = config.appId;
    this.dexie = new ZerithDBDexie(config.appId);
  }

  setValidatorRegistry(registry: ValidatorRegistry): void {
    this.validatorRegistry = registry;
  }

  collection<T extends Record<string, any>>(name: string): CollectionClient<T> {
    if (typeof name !== "string" || name.trim() === "") {
      throw new ZerithDBError(
        ErrorCode.DB_INIT_FAILED,
        "Collection name must be a non-empty string"
      );
    }
    if (!this.collections.has(name)) {
      this.dexie.ensureCollection(name);

      this.collections.set(name, new CollectionClient<T>(this.dexie, name, this.validatorRegistry));
    }

    return this.collections.get(name) as CollectionClient<T>;
  }

  graph<T extends Record<string, any> = Record<string, any>>(name: string): GraphClient<T> {
    if (!this.graphs.has(name)) {
      const { nodesTable, edgesTable } = this.dexie.ensureGraphTables(name);
      this.graphs.set(
        name,
        new GraphClient<T>(nodesTable as Table<GraphNode<T>>, edgesTable as Table<GraphEdge>, name)
      );
    }
    return this.graphs.get(name) as GraphClient<T>;
  }

  async getMemoryStats(): Promise<{ recordCount: number; collections: Record<string, number> }> {
    const collections: Record<string, number> = {};
    let recordCount = 0;

    for (const [name, client] of this.collections) {
      const count = await client.count();
      collections[name] = count;
      recordCount += count;
    }

    return { recordCount, collections };
  }

  collectionNames(): string[] {
    return Array.from(this.collections.keys());
  }

  allCollectionNames(): string[] {
    return this.dexie.tables.map((t) => t.name);
  }

  async exportSnapshot(options: BackupExportOptions = {}): Promise<BackupSnapshot> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      "Failed to export local backup snapshot",
      async () => {
        const collectionNames = options.collections ?? this.allCollectionNames();

        const collections: BackupSnapshot["collections"] = {};

        for (const name of collectionNames) {
          const table = this.dexie.ensureCollection(name);

          collections[name] = (await table.toArray()) as Document<Record<string, any>>[];
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
    this.dexie.close();
  }
}
