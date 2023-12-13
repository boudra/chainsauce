import Sqlite from "better-sqlite3";

import { Event, Hex } from "@/types";
import { Block, Cache } from "@/cache";
import { encodeJsonWithBigInts, decodeJsonWithBigInts } from "@/utils";

type EventRow = {
  chainId: number;
  name: string;
  params: string;
  address: Hex;
  topic0: Hex;
  transactionHash: Hex;
  blockNumber: string;
  logIndex: number;
};

type BlockRow = {
  chainId: number;
  blockNumber: number;
  blockHash: Hex;
  timestamp: number;
};

function initSqliteConnection(dbPath: string) {
  const db = new Sqlite(dbPath);

  db.exec("PRAGMA journal_mode = WAL");

  db.exec(`
  CREATE TABLE IF NOT EXISTS events (
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
`);

  db.exec(`
  CREATE TABLE IF NOT EXISTS logRanges (
    chainId INTEGER,
    address TEXT,
    fromBlock INTEGER,
    toBlock INTEGER
  );
`);

  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_events
     ON events (chainId, address, blockNumber, name, params, transactionHash, logIndex);`
  );

  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_logranges_search
     ON logRanges (chainId, address, fromBlock, toBlock);`
  );

  db.exec(`
  CREATE TABLE IF NOT EXISTS contractReads (
    chainId INTEGER,
    address TEXT,
    data TEXT,
    functionName TEXT,
    blockNumber INTEGER,
    result TEXT,
    PRIMARY KEY (chainId, address, data, functionName, blockNumber)
  );
`);

  db.exec(`
  CREATE TABLE IF NOT EXISTS blocks (
    chainId INTEGER,
    blockNumber INTEGER,
    blockHash INTEGER,
    timestamp INTEGER,
    PRIMARY KEY (chainId, blockHash)
  );
`);

  const insertEventStmt = db.prepare(
    `INSERT OR REPLACE INTO events (chainId, name, params, address, topic0, transactionHash, blockNumber, logIndex)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  );

  const findAdjacentRangesStmt = db.prepare(
    `SELECT fromBlock, toBlock
     FROM logRanges
     WHERE chainId = ? AND address = ?
     AND toBlock >= ? - 1 AND fromBlock <= ? + 1`
  );

  const findRangesStmt = db.prepare(
    `SELECT fromBlock, toBlock FROM logRanges
     WHERE chainId = ? AND address = ?
     AND ((fromBlock <= ? AND toBlock >= ?) OR (fromBlock <= ? AND toBlock >= ?))
     LIMIT 1`
  );

  const findEventsStmt = db.prepare(`
    SELECT * FROM events
    WHERE chainId = ? AND address = ? AND blockNumber >= ? AND blockNumber <= ?
    `);

  const findBlockByNumberStmt = db.prepare(`
    SELECT * FROM blocks
    WHERE chainId = ? AND blockNumber = ?
    `);

  const insertBlockStmt = db.prepare(`
    INSERT OR REPLACE INTO blocks (chainId, blockNumber, blockHash, timestamp)
    VALUES (?, ?, ?, ?)
    `);

  return {
    db,
    findRangesStmt,
    findEventsStmt,
    insertEventStmt,
    findAdjacentRangesStmt,
    findBlockByNumberStmt,
    insertBlockStmt,
  };
}

export function createSqliteCache(dbPath: string): Cache {
  let conn: ReturnType<typeof initSqliteConnection> | null = null;

  function getConnection() {
    if (conn) {
      return conn;
    }

    conn = initSqliteConnection(dbPath);

    return conn;
  }

  return {
    async insertEvents(args: {
      chainId: number;
      events: Event[];
      address: Hex;
      topics: Hex[];
      fromBlock: bigint;
      toBlock: bigint;
    }) {
      const { db, insertEventStmt, findAdjacentRangesStmt } = getConnection();

      const { chainId, events, address, fromBlock, toBlock } = args;

      if (args.toBlock < args.fromBlock) {
        throw new Error("toBlock must be greater than or equal to fromBlock");
      }

      const transaction = db.transaction(() => {
        for (const event of events) {
          insertEventStmt.run(
            chainId,
            event.name,
            encodeJsonWithBigInts(event.params),
            event.address,
            event.topic,
            event.transactionHash,
            event.blockNumber.toString(),
            event.logIndex
          );
        }

        const adjacentRanges = findAdjacentRangesStmt.all(
          chainId,
          address,
          Number(fromBlock),
          Number(toBlock)
        ) as {
          fromBlock: number;
          toBlock: number;
        }[];

        let newFrom = Number(fromBlock);
        let newTo = Number(toBlock);

        for (const range of adjacentRanges) {
          newFrom = Math.min(newFrom, range.fromBlock);
          newTo = Math.max(newTo, range.toBlock);
        }

        // Remove old overlapping ranges
        db.prepare(
          `DELETE FROM logRanges WHERE chainId = ? AND address = ? AND fromBlock >= ? AND toBlock <= ?`
        ).run(chainId, address, newFrom, newTo);

        // Insert the new merged range
        db.prepare(
          `INSERT INTO logRanges (chainId, address, fromBlock, toBlock)
             VALUES (?, ?, ?, ?)`
        ).run(chainId, address, newFrom, newTo);
      });

      transaction();
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
      const { findEventsStmt, findRangesStmt } = getConnection();

      // find a range that overlaps with the requested range
      const range = findRangesStmt.get(
        args.chainId,
        args.address,
        Number(args.toBlock),
        Number(args.fromBlock),
        Number(args.toBlock),
        Number(args.fromBlock)
      ) as { fromBlock: number; toBlock: number } | undefined;

      if (
        range !== undefined &&
        range.fromBlock !== null &&
        range.toBlock !== null
      ) {
        const fromBlock = Math.max(range.fromBlock, Number(args.fromBlock));
        const toBlock = Math.min(range.toBlock, Number(args.toBlock));

        const rows = findEventsStmt.all(
          args.chainId,
          args.address,
          fromBlock,
          toBlock
        ) as EventRow[];

        return {
          fromBlock: BigInt(fromBlock),
          toBlock: BigInt(toBlock),
          events: rows.map((row) => ({
            name: row.name,
            params: decodeJsonWithBigInts(row.params),
            address: row.address,
            topic: row.topic0,
            transactionHash: row.transactionHash,
            blockNumber: BigInt(row.blockNumber),
            logIndex: row.logIndex,
          })),
        };
      } else {
        return null;
      }
    },

    async getContractRead(args: {
      chainId: number;
      address: Hex;
      data: Hex;
      functionName: string;
      blockNumber: bigint;
    }): Promise<Hex | null> {
      const { db } = getConnection();

      const row = db
        .prepare(
          `SELECT result FROM contractReads WHERE chainId = ? AND address = ? AND data = ? AND functionName = ? AND blockNumber = ?`
        )
        .get(
          args.chainId,
          args.address,
          args.data,
          args.functionName,
          args.blockNumber.toString()
        ) as { result: Hex } | undefined;

      return row ? row.result : null;
    },

    async insertContractRead(args: {
      chainId: number;
      address: Hex;
      data: Hex;
      functionName: string;
      blockNumber: bigint;
      result: Hex;
    }): Promise<void> {
      const { db } = getConnection();

      db.prepare(
        `INSERT OR REPLACE INTO contractReads (chainId, address, data, functionName, blockNumber, result) VALUES (?, ?, ?, ?, ?, ?)`
      ).run(
        args.chainId,
        args.address,
        args.data,
        args.functionName,
        args.blockNumber.toString(),
        args.result
      );
    },

    async insertBlock(args: Block): Promise<void> {
      const { insertBlockStmt } = getConnection();

      const { chainId, blockNumber, blockHash, timestamp } = args;

      insertBlockStmt.run(
        chainId,
        blockNumber.toString(),
        blockHash,
        timestamp
      );
    },

    async getBlockByNumber(args: {
      chainId: number;
      blockNumber: bigint;
    }): Promise<Block | null> {
      const { findBlockByNumberStmt } = getConnection();

      const row = findBlockByNumberStmt.get(
        args.chainId,
        args.blockNumber.toString()
      ) as BlockRow | undefined;

      return row
        ? {
            chainId: row.chainId,
            blockNumber: BigInt(row.blockNumber),
            blockHash: row.blockHash,
            timestamp: row.timestamp,
          }
        : null;
    },
  };
}
