import BetterSqlite3 from "better-sqlite3";
import { Collection } from "@/storage";

export type Document = { id: string; [key: string]: unknown };

export interface Database {
  collection<T extends Document>(name: string): Collection<T>;
}

type Row = { id: string; data: string };

class SqliteCollection<T extends Document> implements Collection<T> {
  private db: BetterSqlite3.Database;
  private tableName: string;

  constructor(db: BetterSqlite3.Database, tableName: string) {
    this.db = db;
    this.tableName = tableName;

    // Create the table if it doesn't exist
    this.db.exec(
      `CREATE TABLE IF NOT EXISTS ${tableName} (id TEXT PRIMARY KEY, data JSON);`
    );
  }

  insert(document: T): Promise<T> {
    const stmt = this.db.prepare(
      `INSERT INTO ${this.tableName} (id, data) VALUES (?, ?)`
    );
    stmt.run(document.id, JSON.stringify(document));
    return Promise.resolve(document);
  }

  findById(id: string): Promise<T | null> {
    const stmt = this.db.prepare(
      `SELECT data FROM ${this.tableName} WHERE id = ?`
    );
    const row = stmt.get(id) as Row | undefined;
    return Promise.resolve(row ? JSON.parse(row.data) : null);
  }

  async updateById(id: string, fun: (doc: T) => T): Promise<T | null> {
    const existingDoc = await this.findById(id);
    if (!existingDoc) {
      return Promise.resolve(null);
    }
    const updatedDoc = fun(existingDoc);
    const stmt = this.db.prepare(
      `UPDATE ${this.tableName} SET data = ? WHERE id = ?`
    );
    stmt.run(JSON.stringify(updatedDoc), id);
    return Promise.resolve(updatedDoc);
  }

  async upsertById(id: string, fun: (doc: T | null) => T): Promise<boolean> {
    let isNewDocument = false;
    let existingDoc = await this.findById(id);

    if (!existingDoc) {
      isNewDocument = true;
      existingDoc = null;
    }

    const upsertedDoc = fun(existingDoc);
    const stmt = this.db.prepare(
      `INSERT OR REPLACE INTO ${this.tableName} (id, data) VALUES (?, ?)`
    );
    stmt.run(id, JSON.stringify(upsertedDoc));

    return Promise.resolve(isNewDocument);
  }

  async all(): Promise<T[]> {
    const stmt = this.db.prepare(`SELECT data FROM ${this.tableName}`);
    const rows = stmt.all() as Row[];
    return rows.map((row) => JSON.parse(row.data));
  }
}

export async function createSqliteDatabase(opts: {
  dbPath: string;
}): Promise<Database> {
  const db = new BetterSqlite3(opts.dbPath);
  const collections: Record<string, Collection<Document>> = {};

  return {
    collection<T extends Document>(name: string): Collection<T> {
      if (!collections[name]) {
        collections[name] = new SqliteCollection<T>(db, name);
      }
      return collections[name] as Collection<T>;
    },
  };
}
