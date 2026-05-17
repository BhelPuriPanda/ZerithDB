import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import "fake-indexeddb/auto";
import { z } from "zod";
import * as Y from "yjs";
import { ValidatorRegistry, SchemaValidationError, ErrorCode } from "zerithdb-core";
import { DbClient } from "zerithdb-db";
import { SyncEngine } from "zerithdb-sync";
import { NetworkManager } from "zerithdb-network";
import { AuthManager } from "zerithdb-auth";
import type { ZerithDBConfig } from "zerithdb-core";

function createTestConfig(): ZerithDBConfig {
  return {
    appId: "test-schema-" + Math.random().toString(36).slice(2),
  };
}

describe("Schema Validation Infrastructure", () => {
  describe("ValidatorRegistry", () => {
    it("should allow registering a schema and retrieving it", () => {
      const registry = new ValidatorRegistry();
      const schema = z.object({ name: z.string() });
      registry.register("users", schema, "strict");

      const reg = registry.get("users");
      expect(reg?.schema).toBe(schema);
      expect(reg?.mode).toBe("strict");
    });

    it("should allow re-registering the EXACT SAME schema instance (idempotency)", () => {
      const registry = new ValidatorRegistry();
      const schema = z.object({ name: z.string() });

      registry.register("users", schema, "strict");
      expect(() => registry.register("users", schema, "strict")).not.toThrow();
    });

    it("should THROW when registering a different schema instance for the same collection", () => {
      const registry = new ValidatorRegistry();
      const schema1 = z.object({ name: z.string() });
      const schema2 = z.object({ name: z.string() });

      registry.register("users", schema1, "strict");
      expect(() => registry.register("users", schema2, "strict")).toThrow(/Schema conflict/);
    });

    it("should allow updating a schema reference and mode for a collection", () => {
      const registry = new ValidatorRegistry();
      const schema1 = z.object({ name: z.string() });
      const schema2 = z.object({ age: z.number() });

      registry.register("users", schema1, "strict");
      expect(() => registry.update("users", schema2, "warn")).not.toThrow();

      const reg = registry.get("users");
      expect(reg?.schema).toBe(schema2);
      expect(reg?.mode).toBe("warn");
    });

    it("should default validation mode to strict when updating/registering and mode is omitted", () => {
      const registry = new ValidatorRegistry();
      const schema1 = z.object({ name: z.string() });
      const schema2 = z.object({ age: z.number() });

      registry.register("users", schema1);
      expect(registry.get("users")?.mode).toBe("strict");

      registry.update("users", schema2);
      expect(registry.get("users")?.mode).toBe("strict");
    });

    it("should allow removing a schema for a collection", () => {
      const registry = new ValidatorRegistry();
      const schema = z.object({ name: z.string() });

      registry.register("users", schema, "strict");
      expect(registry.has("users")).toBe(true);

      const removed = registry.remove("users");
      expect(removed).toBe(true);
      expect(registry.has("users")).toBe(false);
      expect(registry.get("users")).toBeUndefined();

      const removedAgain = registry.remove("users");
      expect(removedAgain).toBe(false);
    });
  });

  describe("Local Writes (DbClient)", () => {
    let db: DbClient;
    let registry: ValidatorRegistry;
    const TodoSchema = z.object({
      text: z.string().min(3),
      done: z.boolean(),
    });

    beforeEach(() => {
      registry = new ValidatorRegistry();
      db = new DbClient(createTestConfig());
      db.setValidatorRegistry(registry);
    });

    afterEach(async () => {
      await db.dispose();
    });

    it("should allow valid inserts", async () => {
      registry.register("todos", TodoSchema, "strict");
      const todos = db.collection("todos");

      await expect(todos.insert({ text: "Buy milk", done: false })).resolves.toBeDefined();
    });

    it("should throw SchemaValidationError on invalid insert (strict mode)", async () => {
      registry.register("todos", TodoSchema, "strict");
      const todos = db.collection("todos");

      try {
        await todos.insert({ text: "hi", done: "maybe" } as any);
        expect.fail("Should have thrown");
      } catch (err: any) {
        expect(err.name).toBe("SchemaValidationError");
        expect(err.code).toBe(ErrorCode.DB_VALIDATION_FAILED);
        expect(err.issues).toHaveLength(2);
      }
    });

    it("should NOT throw but call onValidationError in warn mode", async () => {
      const onWarn = vi.fn();
      registry.register("todos", TodoSchema, "warn");

      const todos = db.collection("todos");
      (todos as any).onValidationError = onWarn;

      await todos.insert({ text: "hi", done: false });

      expect(onWarn).toHaveBeenCalled();
      const error = onWarn.mock.calls[0]![0];
      expect(error.name).toBe("SchemaValidationError");

      const docs = await todos.find({});
      expect(docs).toHaveLength(1);
    });

    it("should validate all documents in insertMany and reject atomically (strict)", async () => {
      registry.register("todos", TodoSchema, "strict");
      const todos = db.collection("todos");

      const batch = [
        { text: "No", done: false }, // invalid first to ensure nothing added
        { text: "Valid 2", done: false },
      ];

      await expect(todos.insertMany(batch as any)).rejects.toThrow();

      const count = await todos.count();
      expect(count).toBe(0);
    });

    it("should validate merged state during update()", async () => {
      registry.register("todos", TodoSchema, "strict");
      const todos = db.collection("todos");

      await todos.insert({ text: "Original", done: false });

      try {
        await todos.update({ text: "Original" }, { $set: { text: "x" } as any });
        expect.fail("Should have thrown");
      } catch (err: any) {
        expect(err.name).toBe("SchemaValidationError");
      }

      const [doc] = await todos.find({});
      expect(doc?.text).toBe("Original");
    });

    it("should throw SchemaValidationError on update() failure in strict mode", async () => {
      registry.register("todos", TodoSchema, "strict");
      const todos = db.collection("todos");

      await todos.insert({ text: "Original", done: false });

      try {
        await todos.update({ text: "Original" }, { $set: { text: "ab" } });
        expect.fail("Should have thrown");
      } catch (err: any) {
        expect(err.name).toBe("SchemaValidationError");
        expect(err.code).toBe(ErrorCode.DB_VALIDATION_FAILED);
      }
    });

    it("should preserve _id and _createdAt metadata fields during update()", async () => {
      registry.register("todos", TodoSchema, "strict");
      const todos = db.collection("todos");

      const insertResult = await todos.insert({ text: "Original", done: false });
      const initialDoc = await todos.findById(insertResult.id);
      expect(initialDoc).toBeDefined();

      const initialCreatedAt = initialDoc!._createdAt;
      const initialId = initialDoc!._id;

      // Perform a valid update
      await todos.update({ text: "Original" }, { $set: { text: "Updated" } });

      const updatedDoc = await todos.findById(insertResult.id);
      expect(updatedDoc).toBeDefined();
      expect(updatedDoc!._id).toBe(initialId);
      expect(updatedDoc!._createdAt).toBe(initialCreatedAt);
      expect(updatedDoc!._updatedAt).toBeGreaterThanOrEqual(initialCreatedAt);
    });
  });

  describe("Remote Sync (SyncEngine)", () => {
    let registry: ValidatorRegistry;
    let sync: SyncEngine;
    let db: DbClient;
    let network: NetworkManager;
    let auth: AuthManager;

    const UserSchema = z.object({
      username: z.string().min(3),
      age: z.number(),
    });

    beforeEach(() => {
      registry = new ValidatorRegistry();
      registry.register("users", UserSchema, "strict");

      const config = createTestConfig();
      db = new DbClient(config);
      auth = new AuthManager(config);
      network = new NetworkManager(config, auth);
      sync = new SyncEngine(config, db, network, registry);
    });

    afterEach(async () => {
      await db.dispose();
      await sync.dispose();
      await network.dispose();
    });

    it("should validate remote updates and emit validation:error without blocking merge", async () => {
      const errorSpy = vi.fn();
      sync.on("validation:error", errorSpy);

      const doc = sync.getDoc("users");
      const dataMap = doc.getMap("users");

      // Use Y.applyUpdate with a simple set operation
      // Instead of remote doc, just manually call applyRemoteUpdate with an update
      // that we know will change a key.
      const tempDoc = new Y.Doc();
      const tempMap = tempDoc.getMap("users");
      const invalidData = { username: "bo", age: "young" };
      tempMap.set("user-remote", invalidData);
      const update = Y.encodeStateAsUpdate(tempDoc);

      await sync.applyRemoteUpdate("users", update, "peer-123");

      // Convergence
      expect(dataMap.get("user-remote")).toEqual(invalidData);

      // Validation error
      expect(errorSpy).toHaveBeenCalled();
    });

    it("should only validate CHANGED keys in a multi-document update", async () => {
      const doc = sync.getDoc("users");
      const dataMap = doc.getMap("users");

      // Initial valid data
      dataMap.set("user-1", { username: "alice", age: 30 });

      const validateSpy = vi.spyOn(registry, "validateRemote");

      // Create update that adds user-2 but NOT user-1
      const tempDoc = new Y.Doc();
      const tempMap = tempDoc.getMap("users");
      tempMap.set("user-2", { username: "bob", age: 40 });
      const update = Y.encodeStateAsUpdate(tempDoc);

      await sync.applyRemoteUpdate("users", update, "peer-1");

      // Should have only validated the newly added user-2
      expect(validateSpy).toHaveBeenCalledTimes(1);
      expect(validateSpy).toHaveBeenCalledWith(
        "users",
        expect.objectContaining({ username: "bob" })
      );
    });
  });
});
