import { describe, beforeEach, test, afterEach, expect } from "vitest";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";

import { Document } from "@/storage";
import { createJsonDatabase } from "@/storage/json";

type User = Document & {
  name: string;
  email: string;
};

async function readJSON(path: string) {
  const content = await fs.readFile(path, "utf-8");
  return JSON.parse(content);
}

describe("JsonDatabase", () => {
  let dbDir: string;

  function createDatabase() {
    return createJsonDatabase(dbDir, { writeDelay: 0 });
  }

  beforeEach(async () => {
    dbDir = await fs.mkdtemp(path.join(os.tmpdir(), "db-"));
  });

  afterEach(async () => {
    await fs.rm(dbDir, { recursive: true });
  }, 500);

  test("insert and findById", async () => {
    const db = await createDatabase();
    const users = db.collection<User>("users");

    await users.insert({ id: "1", name: "Alice", email: "alice@example.com" });
    await users.insert({ id: "2", name: "Bob", email: "bob@example.com" });

    const user = await users.findById("1");

    expect(user).toEqual({
      id: "1",
      name: "Alice",
      email: "alice@example.com",
    });
  });

  test("updateById", async () => {
    const db = await createDatabase();
    const users = db.collection<User>("users");

    await users.insert({ id: "1", name: "Alice", email: "alice@example.com" });

    const updatedUser = await users.updateById("1", (user) => ({
      ...user,
      name: "Alicia",
    }));

    expect(updatedUser).toEqual({
      id: "1",
      name: "Alicia",
      email: "alice@example.com",
    });
  });

  test("upsertById", async () => {
    const db = await createDatabase();
    const users = db.collection<User>("users");

    const isNew = await users.upsertById("1", (user) => {
      expect(user).toBeNull();

      return {
        id: "1",
        name: "Alice",
        email: "alice@example.com",
      };
    });

    expect(isNew).toBe(true);

    const isNew2 = await users.upsertById("1", (user) => {
      expect(user).not.toBeNull();

      return {
        name: "Alice",
        email: "alice@example.com",
      };
    });

    expect(isNew2).toBe(false);

    const user = await users.findById("1");

    expect(user).toEqual({
      id: "1",
      name: "Alice",
      email: "alice@example.com",
    });
  });

  test("all", async () => {
    const db = await createDatabase();
    const users = db.collection<User>("users");

    await users.insert({ id: "1", name: "Alice", email: "alice@example.com" });
    await users.insert({ id: "2", name: "Bob", email: "bob@example.com" });

    const allUsers = await users.all();
    expect(allUsers).toEqual([
      { id: "1", name: "Alice", email: "alice@example.com" },
      { id: "2", name: "Bob", email: "bob@example.com" },
    ]);
  });

  test("writes JSON files", async () => {
    // test write delay code path
    const db = await createJsonDatabase(dbDir, { writeDelay: 500 });
    const users = db.collection<User>("sub/users");

    await users.insert({ id: "1", name: "Alice", email: "alice@example.com" });

    // wait for the write delay
    await new Promise((resolve) => setTimeout(resolve, 500));

    const dbData = await readJSON(path.join(dbDir, "sub/users.json"));

    expect(dbData).toEqual([
      {
        id: "1",
        name: "Alice",
        email: "alice@example.com",
      },
    ]);
  });
});
