import { Hex, ToBlock } from "@/types";
import Database from "better-sqlite3";

export type Subscription = {
  id: string;
  contractName: string;
  contractAddress: Hex;
  indexedToBlock: bigint;
  indexedToLogIndex: number;
  fromBlock: bigint;
  toBlock: ToBlock;
};

export interface SubscriptionStore {
  save(subscription: Subscription): Promise<void>;
  get(id: string): Promise<Subscription | null>;
  delete(id: string): Promise<void>;
  all(): Promise<Subscription[]>;
}

type SubscriptionRow = {
  id: string;
  contractName: string;
  contractAddress: Hex;
  indexedToBlock: string;
  fromBlock: string;
  indexedToLogIndex: number;
  toBlock: string;
};

export function createSqliteSubscriptionStore(
  dbPath: string
): SubscriptionStore {
  const db = new Database(dbPath);

  db.prepare(
    `
    CREATE TABLE IF NOT EXISTS subscriptions (
      id TEXT PRIMARY KEY,
      contractName TEXT,
      contractAddress TEXT,
      fromBlock INTEGER,
      toBlock TEXT,
      indexedToBlock INTEGER,
      indexedToLogIndex INTEGER
    )
  `
  ).run();

  function fromRow(row: SubscriptionRow): Subscription {
    return {
      ...row,
      indexedToBlock: BigInt(row.indexedToBlock),
      fromBlock: BigInt(row.fromBlock),
      toBlock: row.toBlock === "latest" ? "latest" : BigInt(row.toBlock),
    } as Subscription;
  }

  return {
    async save(subscription: Subscription): Promise<void> {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO subscriptions (id, contractName, contractAddress, fromBlock, indexedToBlock, indexedToLogIndex, toBlock)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `);
      stmt.run(
        subscription.id,
        subscription.contractName,
        subscription.contractAddress,
        subscription.fromBlock.toString(),
        subscription.indexedToBlock.toString(),
        subscription.indexedToLogIndex,
        subscription.toBlock
      );
    },

    async get(id: string): Promise<Subscription | null> {
      const stmt = db.prepare("SELECT * FROM subscriptions WHERE id = ?");
      const row = stmt.get(id) as SubscriptionRow | undefined;
      return row ? fromRow(row) : null;
    },

    async delete(id: string): Promise<void> {
      const stmt = db.prepare("DELETE FROM subscriptions WHERE id = ?");
      stmt.run(id);
    },

    async all(): Promise<Subscription[]> {
      const stmt = db.prepare("SELECT * FROM subscriptions");
      return (stmt.all() as SubscriptionRow[]).map(fromRow);
    },
  };
}
