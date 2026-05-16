import Dexie, { type Table } from "dexie";
import { v7 as uuidv7 } from "uuid";
import type {
  ZerithDBConfig,
  Document,
  QueryFilter,
  InsertResult,
  UpdateSpec,
  ValidatorRegistry,
  ValidationResult,
} from "zerithdb-core";
import { ZerithDBError, ErrorCode, SchemaValidationError } from "zerithdb-core";

class ZerithDBDexie extends Dexie {
  private readonly insuredTableNames = new Set<string>();

  constructor(appId: string) {
    super(`zerithdb_${appId}`);
  }

  ensureCollection(name: string): Table {
    if (this.insuredTableNames.has(name)) {
      return this.table(name);
    }

    this.insuredTableNames.add(name);
    const version = (this.verno || 0) + this.insuredTableNames.size;
    const schema: Record<string, string> = {};
    for (const tableName of this.insuredTableNames) {
      schema[tableName] = "_id, _createdAt, _updatedAt";
    }
    
    this.version(version).stores(schema);
    return this.table(name);
  }
}

/**
 * Client for a specific collection.
 * Provides CRUD operations with optional schema validation.
 *
 * @example
 * ```typescript
 * const todos = db.collection<Todo>("todos");
 * await todos.insert({ text: "Buy milk", done: false });
 * ```
 */
export class CollectionClient<T extends Record<string, any> = Record<string, any>> {
  constructor(
    private readonly dexie: ZerithDBDexie,
    private readonly collectionName: string,
    private readonly validatorRegistry?: ValidatorRegistry,
    private readonly onValidationError?: (error: SchemaValidationError) => void
  ) { }

  private get table(): Table<Document<T>> {
    return this.dexie.table(this.collectionName);
  }

  /**
   * Insert a new document into the collection.
   * Automatically assigns `_id`, `_createdAt`, and `_updatedAt`.
   */
  async insert(document: T): Promise<InsertResult> {
    this.runValidation(document);
    const now = Date.now();
    const id = uuidv7();
    const doc: Document<T> = {
      ...document,
      _id: id,
      _createdAt: now,
      _updatedAt: now,
    };

    try {
      await this.table.add(doc);
      return { id };
    } catch (err) {
      throw new ZerithDBError(
        ErrorCode.DB_WRITE_FAILED,
        `Failed to insert into collection "${this.collectionName}"`,
        { cause: err }
      );
    }
  }

  /**
   * Insert multiple documents in a single atomic operation.
   */
  async insertMany(documents: T[]): Promise<InsertResult[]> {
    // Atomic: validate ALL documents before writing any
    for (let i = 0; i < documents.length; i++) {
      this.runValidation(documents[i], i);
    }
    const now = Date.now();
    const docs = documents.map((doc) => ({
      ...doc,
      _id: uuidv7(),
      _createdAt: now,
      _updatedAt: now,
    })) as Document<T>[];

    try {
      await this.table.bulkAdd(docs);
      return docs.map((d) => ({ id: d._id }));
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
   *
   * @example
   * ```typescript
   * const active = await todos.find({ done: false });
   * const high = await todos.find({ priority: { $gte: 3 } });
   * ```
   */
  async find(filter: QueryFilter<T> = {}): Promise<Document<T>[]> {
    try {
      const all = await this.table.toArray();
      return all.filter((doc) => this.matchesFilter(doc, filter));
    } catch (err) {
      throw new ZerithDBError(
        ErrorCode.DB_READ_FAILED,
        `Failed to query collection "${this.collectionName}"`,
        { cause: err }
      );
    }
  }

  /**
   * Find a single document by its `_id`.
   */
  async findById(id: string): Promise<Document<T> | undefined> {
    try {
      return await this.table.get(id);
    } catch (err) {
      throw new ZerithDBError(
        ErrorCode.DB_READ_FAILED,
        `Failed to get document "${id}" from "${this.collectionName}"`,
        { cause: err }
      );
    }
  }

  /**
   * Update documents matching a filter.
   * Returns the number of updated documents.
   */
  async update(filter: QueryFilter<T>, spec: UpdateSpec<T>): Promise<number> {
    try {
      const matches = await this.find(filter);
      const now = Date.now();

<<<<<<< HEAD
      await this.table.bulkPut(matches.map((doc) => this.applyUpdateSpec(doc, spec, now)));
=======
      // Validate each merged document before writing.
      // NOTE: Currently only previews $set merges. Future operators ($unset,
      // $push, $pull, nested dot-path updates) will need their own merge
      // preview logic to produce an accurate validation candidate.
      for (const doc of matches) {
        const merged = { ...doc, ...(spec.$set ?? {}), _updatedAt: now };
        this.runValidation(merged);
      }

      await this.table.bulkPut(
        matches.map((doc) => ({
          ...doc,
          ...(spec.$set ?? {}),
          _updatedAt: now,
        }))
      );
>>>>>>> f59b043 (feat(validation): add centralized schema validation system)

      return matches.length;
    } catch (err) {
      if (err instanceof SchemaValidationError) throw err;
      throw new ZerithDBError(
        ErrorCode.DB_WRITE_FAILED,
        `Failed to update documents in "${this.collectionName}"`,
        { cause: err }
      );
    }
  }

  /**
   * Delete documents matching a filter.
   * Returns the number of deleted documents.
   */
  async delete(filter: QueryFilter<T>): Promise<number> {
    try {
      const matches = await this.find(filter);
      await this.table.bulkDelete(matches.map((d) => d._id));
      return matches.length;
    } catch (err) {
      throw new ZerithDBError(
        ErrorCode.DB_DELETE_FAILED,
        `Failed to delete documents from "${this.collectionName}"`,
        { cause: err }
      );
    }
  }

  /**
   * Delete every document in the collection.
   */
  async clearAll(): Promise<void> {
    try {
      await this.table.clear();
    } catch (err) {
      throw new ZerithDBError(
        ErrorCode.DB_DELETE_FAILED,
        `Failed to clear collection "${this.collectionName}"`,
        { cause: err }
      );
    }
  }

  /**
   * Count documents matching a filter.
   */
  async count(filter: QueryFilter<T> = {}): Promise<number> {
    const docs = await this.find(filter);
    return docs.length;
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
    next._updatedAt = updatedAt;

    return next as Document<T>;
  }

  private matchesFilter(doc: Document<T>, filter: QueryFilter<T>): boolean {
    for (const [key, condition] of Object.entries(filter)) {
      const fieldValue = (doc as Record<string, any>)[key];

      if (condition === null || typeof condition !== "object") {
        if (fieldValue !== condition) return false;
        continue;
      }

      const ops = condition as Record<string, any>;
      if ("$eq" in ops && fieldValue !== ops["$eq"]) return false;
      if ("$ne" in ops && fieldValue === ops["$ne"]) return false;
      if ("$gt" in ops && !((fieldValue as any) > (ops["$gt"] as never))) return false;
      if ("$gte" in ops && !((fieldValue as any) >= (ops["$gte"] as never))) return false;
      if ("$lt" in ops && !((fieldValue as any) < (ops["$lt"] as never))) return false;
      if ("$lte" in ops && !((fieldValue as any) <= (ops["$lte"] as never))) return false;
      if ("$in" in ops && !(ops["$in"] as unknown[]).includes(fieldValue)) return false;
      if ("$nin" in ops && (ops["$nin"] as unknown[]).includes(fieldValue)) return false;
    }
    return true;
  }

  /**
   * Validate a document against the collection's registered schema.
   * Behavior depends on the validation mode:
   *  - "strict": throws SchemaValidationError
   *  - "warn": calls onValidationError callback, does NOT throw
   *  - "off" / no schema: no-op
   */
  private runValidation(data: unknown, batchIndex?: number): void {
    if (!this.validatorRegistry) return;

    const result = this.validatorRegistry.validate(this.collectionName, data);
    if (result.valid) return;

    const prefix = batchIndex !== undefined
      ? `Batch validation failed at index ${batchIndex} in`
      : `Validation failed in`;

    const error = new SchemaValidationError(
      ErrorCode.DB_VALIDATION_FAILED,
      `${prefix} "${this.collectionName}": ${result.issues.map(i => i.message).join(", ")}`,
      result.issues
    );

    // Always notify the callback (for both "strict" and "warn")
    this.onValidationError?.(error);

    // Only throw in "strict" mode
    if (result.shouldThrow) {
      throw error;
    }
  }
}


/**
 * Internal database client. Wraps Dexie and manages collection instances.
 * Use via {@link ZerithDBApp.db} — not instantiated directly.
 */
export class DbClient {
  private readonly dexie: ZerithDBDexie;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly collections = new Map<string, CollectionClient<any>>();
  private validatorRegistry?: ValidatorRegistry;

  constructor(config: ZerithDBConfig) {
    this.dexie = new ZerithDBDexie(config.appId);
  }

  /** Set the shared validator registry. Called by the SDK during initialization. */
  setValidatorRegistry(registry: ValidatorRegistry): void {
    this.validatorRegistry = registry;
  }

  collection<T extends Record<string, any>>(name: string): CollectionClient<T> {
    if (!this.collections.has(name)) {
      this.dexie.ensureCollection(name);
      this.collections.set(
        name,
        new CollectionClient<T>(this.dexie, name, this.validatorRegistry)
      );
    }
    return this.collections.get(name) as CollectionClient<T>;
  }

  async dispose(): Promise<void> {
    this.dexie.close();
  }
}
