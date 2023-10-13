import Sqlite from "better-sqlite3";

import { Event, Hex } from "@/types";
import { Cache } from "@/cache";
import { encodeJsonWithBigInts, decodeJsonWithBigInts } from "@/utils";

type EventRow = {
  chainId: number;
  name: string;
  params: string;
  address: Hex;
  topic: Hex;
  transactionHash: Hex;
  blockNumber: string;
  logIndex: number;
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
    topic TEXT,
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
    topic TEXT,
    fromBlock INTEGER,
    toBlock INTEGER
  );
`);

  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_events
     ON events (chainId, address, topic, blockNumber, name, params, transactionHash, logIndex);`
  );

  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_logranges_search
     ON logRanges (chainId, address, topic, fromBlock, toBlock);`
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

  const insertEventStmt = db.prepare(
    `INSERT OR REPLACE INTO events (chainId, name, params, address, topic, transactionHash, blockNumber, logIndex)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  );

  const findAdjacentRangesStmt = db.prepare(
    `SELECT fromBlock, toBlock
     FROM logRanges
     WHERE chainId = ? AND address = ? AND topic = ?
     AND toBlock >= ? - 1 AND fromBlock <= ? + 1`
  );

  const findRangesStmt = db.prepare(
    `SELECT fromBlock, toBlock FROM logRanges
     WHERE chainId = ? AND address = ? AND topic = ?
     AND ((fromBlock <= ? AND toBlock >= ?) OR (fromBlock <= ? AND toBlock >= ?))
     LIMIT 1`
  );

  const findEventsStmt = db.prepare(`
    SELECT * FROM events
    WHERE chainId = ? AND address = ? AND topic = ? AND blockNumber >= ? AND blockNumber <= ?
    `);

  return {
    db,
    findRangesStmt,
    findEventsStmt,
    insertEventStmt,
    findAdjacentRangesStmt,
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

      const { chainId, events, address, topics, fromBlock, toBlock } = args;

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

        for (const topic of topics) {
          const adjacentRanges = findAdjacentRangesStmt.all(
            chainId,
            address,
            topic,
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
            `DELETE FROM logRanges WHERE chainId = ? AND address = ? AND topic = ? AND fromBlock >= ? AND toBlock <= ?`
          ).run(chainId, address, topic, newFrom, newTo);

          // Insert the new merged range
          db.prepare(
            `INSERT INTO logRanges (chainId, address, topic, fromBlock, toBlock)
             VALUES (?, ?, ?, ?, ?)`
          ).run(chainId, address, topic, newFrom, newTo);
        }
      });

      transaction();
    },

    async getEvents(args: {
      chainId: number;
      address: Hex;
      topic: Hex;
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
        args.topic,
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
          args.topic,
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
            topic: row.topic,
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
  };
}
