import { Pool } from "pg";
import { Subscription, SubscriptionStore } from "@/subscriptionStore";
import { Hex } from "@/types";

type SubscriptionRow = {
  id: string;
  chain_id: number;
  contract_name: string;
  contract_address: Hex;
  indexed_to_block: string;
  from_block: string;
  indexed_to_log_index: number;
  to_block: string;
};

export function createPostgresSubscriptionStore(args: {
  pool: Pool;
  schema: string;
}): SubscriptionStore {
  const { pool, schema } = args;
  const schemaPrefix = `"${schema}".`;

  async function runQuery(query: string, params: unknown[] = []) {
    const client = await pool.connect();
    try {
      return await client.query(query, params);
    } finally {
      client.release();
    }
  }

  function fromRow(row: SubscriptionRow): Subscription {
    return {
      chainId: row.chain_id,
      id: row.id,
      contractName: row.contract_name,
      contractAddress: row.contract_address,
      indexedToLogIndex: row.indexed_to_log_index,
      indexedToBlock: BigInt(row.indexed_to_block),
      fromBlock: BigInt(row.from_block),
      toBlock: row.to_block === "latest" ? "latest" : BigInt(row.to_block),
    };
  }

  return {
    async init(): Promise<void> {
      await runQuery(
        `
        CREATE TABLE IF NOT EXISTS ${schemaPrefix}subscriptions (
          id TEXT PRIMARY KEY,
          chain_id INTEGER,
          contract_name TEXT,
          contract_address TEXT,
          from_block TEXT,
          to_block TEXT,
          indexed_to_block TEXT,
          indexed_to_log_index INTEGER
        )
        `
      );
    },

    async save(subscription: Subscription): Promise<void> {
      const query = `
        INSERT INTO ${schemaPrefix}subscriptions (id, contract_name, contract_address, from_block, indexed_to_block, indexed_to_log_index, to_block, chain_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO UPDATE SET
          contract_name = EXCLUDED.contract_name,
          contract_address = EXCLUDED.contract_address,
          from_block = EXCLUDED.from_block,
          indexed_to_block = EXCLUDED.indexed_to_block,
          indexed_to_log_index = EXCLUDED.indexed_to_log_index,
          to_block = EXCLUDED.to_block,
          chain_id = EXCLUDED.chain_id
      `;
      await runQuery(query, [
        subscription.id,
        subscription.contractName,
        subscription.contractAddress,
        subscription.fromBlock.toString(),
        subscription.indexedToBlock.toString(),
        subscription.indexedToLogIndex,
        subscription.toBlock,
        subscription.chainId,
      ]);
    },

    async update(
      id: string,
      update: Pick<Subscription, "indexedToBlock" | "indexedToLogIndex">
    ): Promise<void> {
      const query = `
      UPDATE ${schemaPrefix}subscriptions
      SET
        indexed_to_block = $1,
        indexed_to_log_index = $2
      WHERE id = $3
      `;

      await runQuery(query, [
        update.indexedToBlock.toString(),
        update.indexedToLogIndex,
        id,
      ]);
    },

    async get(id: string): Promise<Subscription | null> {
      const query = `SELECT * FROM ${schemaPrefix}subscriptions WHERE id = $1`;
      const result = await runQuery(query, [id]);
      return result.rows.length > 0 ? fromRow(result.rows[0]) : null;
    },

    async delete(id: string): Promise<void> {
      const query = `DELETE FROM ${schemaPrefix}subscriptions WHERE id = $1`;
      await runQuery(query, [id]);
    },

    async all(chain_id: number): Promise<Subscription[]> {
      const query = `SELECT * FROM ${schemaPrefix}subscriptions WHERE chain_id = $1`;
      const result = await runQuery(query, [chain_id]);
      return result.rows.map(fromRow);
    },
  };
}
