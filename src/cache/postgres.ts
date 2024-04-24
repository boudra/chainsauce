import { Pool } from "pg";

import { Event, Hex } from "@/types";
import { Block, Cache } from "@/cache";
import { encodeJsonWithBigInts, decodeJsonWithBigInts } from "@/utils";

type BlockRow = {
  chainid: number;
  blocknumber: string;
  blockhash: Hex;
  timestamp: number;
};

const migration = `
CREATE TABLE IF NOT EXISTS "$1"."events" (
  chainId INTEGER,
  name TEXT,
  params TEXT,
  address TEXT,
  topic0 TEXT,
  transactionHash TEXT,
  blockNumber INTEGER,
  logIndex INTEGER,
  PRIMARY KEY (chainId, blockNumber, logIndex)
);
CREATE TABLE IF NOT EXISTS "$1"."logRanges" (
  chainId INTEGER,
  address TEXT,
  fromBlock INTEGER,
  toBlock INTEGER
);
CREATE INDEX IF NOT EXISTS idx_events ON "$1"."events" (chainId, address, blockNumber, name, params, transactionHash, logIndex);
CREATE INDEX IF NOT EXISTS idx_logranges_search ON "$1"."logRanges" (chainId, address, fromBlock, toBlock);
CREATE TABLE IF NOT EXISTS "$1"."contractReads" (
  chainId INTEGER,
  address TEXT,
  data TEXT,
  functionName TEXT,
  blockNumber INTEGER,
  result TEXT,
  PRIMARY KEY (chainId, address, data, functionName, blockNumber)
);
CREATE TABLE IF NOT EXISTS "$1".blocks (
  chainId INTEGER,
  blockNumber TEXT,
  blockHash TEXT,
  timestamp INTEGER,
  PRIMARY KEY (chainId, blockHash)
);
CREATE INDEX IF NOT EXISTS idx_blocks ON "$1".blocks (chainId, blockNumber);
`;

export function createPostgresCache(args: {
  connectionPool: Pool;
  schemaName?: string;
}): Cache {
  const pool = args.connectionPool;
  const schema = args.schemaName ?? "public";

  const tableName = (name: string) => `"${schema}"."${name}"`;

  return {
    migrate: async () => {
      await pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
      await pool.query(migration.replaceAll(/\$1/g, schema));
    },

    async getBlockByNumber(args: {
      chainId: number;
      blockNumber: bigint;
    }): Promise<Block | null> {
      const client = await pool.connect();
      try {
        const res = await client.query<BlockRow>(
          `SELECT * FROM ${tableName(
            "blocks"
          )} WHERE chainId = $1 AND blockNumber = $2`,
          [args.chainId, args.blockNumber.toString()]
        );

        if (res.rows.length === 0) {
          return null;
        }

        return {
          chainId: res.rows[0].chainid,
          blockNumber: BigInt(res.rows[0].blocknumber),
          blockHash: res.rows[0].blockhash,
          timestamp: res.rows[0].timestamp,
        };
      } finally {
        client.release();
      }
    },

    async insertBlock(args: Block) {
      const client = await pool.connect();
      try {
        await client.query(
          `INSERT INTO ${tableName(
            "blocks"
          )} (chainId, blockNumber, blockHash, timestamp) VALUES ($1, $2, $3, $4)
            ON CONFLICT (chainId, blockhash) DO UPDATE SET
              blockHash = EXCLUDED.blockHash,
              timestamp = EXCLUDED.timestamp,
              blockNumber = EXCLUDED.blocknumber,
              chainId = EXCLUDED.chainId
          `,
          [
            args.chainId,
            args.blockNumber.toString(),
            args.blockHash,
            args.timestamp,
          ]
        );
      } finally {
        client.release();
      }
    },

    async insertEvents(args: {
      chainId: number;
      events: Event[];
      address: Hex;
      topics: Hex[];
      fromBlock: bigint;
      toBlock: bigint;
    }) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        for (const event of args.events) {
          await client.query(
            `INSERT INTO ${tableName(
              "events"
            )} (chainId, name, params, address, topic0, transactionHash, blockNumber, logIndex)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
              ON CONFLICT (chainId, blockNumber, logIndex) DO UPDATE SET
                name = EXCLUDED.name,
                params = EXCLUDED.params,
                address = EXCLUDED.address,
                topic0 = EXCLUDED.topic0,
                transactionHash = EXCLUDED.transactionHash,
                blockNumber = EXCLUDED.blockNumber,
                logIndex = EXCLUDED.logIndex
             `,
            [
              args.chainId,
              event.name,
              encodeJsonWithBigInts(event.params),
              event.address,
              event.topic,
              event.transactionHash,
              event.blockNumber.toString(),
              event.logIndex,
            ]
          );
        }

        // Check for adjacent ranges in logRanges
        const adjacentRanges = await client.query(
          `SELECT fromBlock, toBlock FROM ${tableName(
            "logRanges"
          )} WHERE chainId = $1 AND address = $2 AND toBlock >= $3 - 1 AND fromBlock <= $4 + 1`,
          [
            args.chainId,
            args.address,
            Number(args.fromBlock),
            Number(args.toBlock),
          ]
        );

        let newFrom = Number(args.fromBlock);
        let newTo = Number(args.toBlock);

        for (const range of adjacentRanges.rows) {
          newFrom = Math.min(newFrom, range.fromblock);
          newTo = Math.max(newTo, range.toblock);
        }

        // Remove old overlapping ranges
        await client.query(
          `DELETE FROM ${tableName(
            "logRanges"
          )} WHERE chainId = $1 AND address = $2 AND fromBlock >= $3 AND toBlock <= $4`,
          [args.chainId, args.address, newFrom, newTo]
        );

        // Insert the new merged range
        await client.query(
          `INSERT INTO ${tableName(
            "logRanges"
          )} (chainId, address, fromBlock, toBlock) VALUES ($1, $2, $3, $4)`,
          [args.chainId, args.address, newFrom, newTo]
        );

        await client.query("COMMIT");
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        client.release();
      }
    },

    async getEvents(args: {
      chainId: number;
      address: Hex;
      topic0: Hex;
      fromBlock: bigint;
      toBlock: bigint;
    }): Promise<{
      fromBlock: bigint;
      toBlock: bigint;
      events: Event[];
    } | null> {
      const client = await pool.connect();

      try {
        const range = await client.query(
          `SELECT fromBlock, toBlock
          FROM ${tableName("logRanges")}
          WHERE chainId = $1 AND address = $2
          AND ((fromBlock <= $3 AND toBlock >= $4) OR (fromBlock <= $5 AND toBlock >= $6))
          LIMIT 1`,
          [
            args.chainId,
            args.address,
            Number(args.toBlock),
            Number(args.fromBlock),
            Number(args.toBlock),
            Number(args.fromBlock),
          ]
        );

        if (range.rows.length > 0) {
          const fromBlock = Math.max(
            range.rows[0].fromblock,
            Number(args.fromBlock)
          );

          const toBlock = Math.min(range.rows[0].toblock, Number(args.toBlock));

          const rows = await client.query(
            `SELECT * FROM ${tableName(
              "events"
            )} WHERE chainId = $1 AND address = $2 AND blockNumber >= $3 AND blockNumber <= $4`,
            [args.chainId, args.address, fromBlock, toBlock]
          );

          return {
            fromBlock: BigInt(fromBlock),
            toBlock: BigInt(toBlock),
            events: rows.rows.map((row) => ({
              name: row.name,
              params: decodeJsonWithBigInts(row.params),
              address: row.address,
              topic: row.topic0,
              transactionHash: row.transactionhash,
              blockNumber: BigInt(row.blocknumber),
              logIndex: row.logindex,
            })),
          };
        } else {
          return null;
        }
      } finally {
        client.release();
      }
    },

    async getContractRead(args: {
      chainId: number;
      address: Hex;
      data: Hex;
      functionName: string;
      blockNumber: bigint;
    }): Promise<Hex | null> {
      const client = await pool.connect();
      try {
        const res = await client.query(
          `SELECT result FROM ${tableName(
            "contractReads"
          )} WHERE chainId = $1 AND address = $2 AND data = $3 AND functionName = $4 AND blockNumber = $5`,
          [
            args.chainId,
            args.address,
            args.data,
            args.functionName,
            args.blockNumber.toString(),
          ]
        );
        return res.rows.length > 0 ? res.rows[0].result : null;
      } finally {
        client.release();
      }
    },

    async insertContractRead(args: {
      chainId: number;
      address: Hex;
      data: Hex;
      functionName: string;
      blockNumber: bigint;
      result: Hex;
    }): Promise<void> {
      const client = await pool.connect();
      try {
        await client.query(
          `INSERT INTO ${tableName(
            "contractReads"
          )} (chainId, address, data, functionName, blockNumber, result)
          VALUES ($1, $2, $3, $4, $5, $6)`,
          [
            args.chainId,
            args.address,
            args.data,
            args.functionName,
            args.blockNumber.toString(),
            args.result,
          ]
        );
      } finally {
        client.release();
      }
    },
  };
}
