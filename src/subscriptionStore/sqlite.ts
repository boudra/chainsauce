import Database from "better-sqlite3";
import { Hex } from "@/types";
import { SubscriptionStore, Subscription } from "@/subscriptionStore";

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

  function fromRow(row: SubscriptionRow): Subscription {
    return {
      ...row,
      indexedToBlock: BigInt(row.indexedToBlock),
      fromBlock: BigInt(row.fromBlock),
      toBlock: row.toBlock === "latest" ? "latest" : BigInt(row.toBlock),
    } as Subscription;
  }

  return {
    async init(): Promise<void> {
      db.prepare(
        `
        CREATE TABLE IF NOT EXISTS subscriptions (
          id TEXT PRIMARY KEY,
          chainId INTEGER,
          contractName TEXT,
          contractAddress TEXT,
          fromBlock INTEGER,
          toBlock TEXT,
          indexedToBlock INTEGER,
          indexedToLogIndex INTEGER
        )
        `
      ).run();
    },
    async save(subscription: Subscription): Promise<void> {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO subscriptions (id, chainId, contractName, contractAddress, fromBlock, indexedToBlock, indexedToLogIndex, toBlock)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);
      stmt.run(
        subscription.id,
        subscription.chainId,
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

    async all(chainId: number): Promise<Subscription[]> {
      const stmt = db.prepare("SELECT * FROM subscriptions WHERE chainId = ?");
      return (stmt.all(chainId) as SubscriptionRow[]).map(fromRow);
    },
  };
}
