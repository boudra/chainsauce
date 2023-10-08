import SqliteDatabase from "better-sqlite3";
import { Event, Hex } from "@/types";

export interface Cache {
  insertEvents(args: {
    events: Event[];
    address: Hex;
    topics: Hex[];
    fromBlock: bigint;
    toBlock: bigint;
  }): Promise<void>;
  getEvents(args: {
    address: Hex;
    topic: Hex;
    fromBlock: bigint;
    toBlock: bigint;
  }): Promise<{ fromBlock: bigint; toBlock: bigint; events: Event[] } | null>;
  getContractRead(args: {
    address: Hex;
    data: Hex;
    functionName: string;
    blockNumber: bigint;
  }): Promise<Hex | null>;
  insertContractRead(args: {
    address: Hex;
    data: Hex;
    functionName: string;
    blockNumber: bigint;
    result: Hex;
  }): Promise<void>;
}

type EventRow = {
  name: string;
  params: string;
  address: Hex;
  topic: Hex;
  transactionHash: Hex;
  blockNumber: string;
  logIndex: number;
};

export function createInMemoryCache(): Cache {
  return createSqliteCache(":memory:");
}

export function createSqliteCache(dbPath: string): Cache {
  function stringify(_key: string, value: unknown) {
    if (typeof value === "bigint") {
      return { type: "bigint", value: value.toString() };
    }
    return value;
  }

  function parse(_key: string, value: unknown) {
    if (
      typeof value === "object" &&
      value !== null &&
      "type" in value &&
      value.type === "bigint" &&
      "value" in value &&
      typeof value.value === "string"
    ) {
      return BigInt(value.value);
    }
    return value;
  }

  const db = new SqliteDatabase(dbPath);

  db.exec(`
  CREATE TABLE IF NOT EXISTS events (
    name TEXT,
    params TEXT,
    address TEXT,
    topic TEXT,
    transactionHash TEXT,
    blockNumber INTEGER,
    logIndex INTEGER,
    PRIMARY KEY (blockNumber, logIndex)
  );
`);

  db.exec(`
  CREATE TABLE IF NOT EXISTS logRanges (
    address TEXT,
    topic TEXT,
    fromBlock INTEGER,
    toBlock INTEGER
  );
`);

  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_address_topic_block
   ON events (address, topic, blockNumber);`
  );

  db.exec(`
  CREATE TABLE IF NOT EXISTS contractReads (
    address TEXT,
    data TEXT,
    functionName TEXT,
    blockNumber INTEGER,
    result TEXT,
    PRIMARY KEY (address, data, functionName, blockNumber)
  );
`);

  return {
    async insertEvents(args: {
      events: Event[];
      address: Hex;
      topics: Hex[];
      fromBlock: bigint;
      toBlock: bigint;
    }) {
      const { events, address, topics, fromBlock, toBlock } = args;

      if (args.toBlock < args.fromBlock) {
        throw new Error("toBlock must be greater than or equal to fromBlock");
      }

      const transaction = db.transaction(() => {
        const insertStmt = db.prepare(
          `INSERT OR REPLACE INTO events (name, params, address, topic, transactionHash, blockNumber, logIndex)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
        );

        for (const event of events) {
          insertStmt.run(
            event.name,
            JSON.stringify(event.params, stringify),
            event.address,
            event.topic,
            event.transactionHash,
            event.blockNumber.toString(),
            event.logIndex
          );
        }

        for (const topic of topics) {
          const adjacentRanges = db
            .prepare(
              `SELECT fromBlock, toBlock
               FROM logRanges
               WHERE address = ? AND topic = ?
               AND toBlock >= ? - 1 AND fromBlock <= ? + 1`
            )
            .all(address, topic, Number(fromBlock), Number(toBlock)) as {
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
            `DELETE FROM logRanges WHERE address = ? AND topic = ? AND fromBlock >= ? AND toBlock <= ?`
          ).run(address, topic, newFrom, newTo);

          // Insert the new merged range
          db.prepare(
            `INSERT INTO logRanges (address, topic, fromBlock, toBlock)
             VALUES (?, ?, ?, ?)`
          ).run(address, topic, newFrom, newTo);
        }
      });

      transaction();
    },

    async getEvents(args: {
      address: Hex;
      topic: Hex;
      fromBlock: bigint;
      toBlock: bigint;
    }): Promise<{
      fromBlock: bigint;
      toBlock: bigint;
      events: Event[];
    } | null> {
      // find a range that overlaps with the requested range
      const range = db
        .prepare(
          `SELECT fromBlock, toBlock FROM logRanges
             WHERE address = ? AND topic = ? 
               AND ((fromBlock <= ? AND toBlock >= ?) OR (fromBlock <= ? AND toBlock >= ?))
             LIMIT 1`
        )
        .get(
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

        const query = db.prepare(`
        SELECT * FROM events
        WHERE address = ? AND topic = ? AND blockNumber >= ? AND blockNumber <= ?
        `);

        const rows = query.all(
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
            params: JSON.parse(row.params, parse),
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
      address: Hex;
      data: Hex;
      functionName: string;
      blockNumber: bigint;
    }): Promise<Hex | null> {
      const row = db
        .prepare(
          `SELECT result FROM contractReads WHERE address = ? AND data = ? AND functionName = ? AND blockNumber = ?`
        )
        .get(
          args.address,
          args.data,
          args.functionName,
          args.blockNumber.toString()
        ) as { result: Hex } | undefined;

      return row ? row.result : null;
    },

    async insertContractRead(args: {
      address: Hex;
      data: Hex;
      functionName: string;
      blockNumber: bigint;
      result: Hex;
    }): Promise<void> {
      db.prepare(
        `INSERT OR REPLACE INTO contractReads (address, data, functionName, blockNumber, result) VALUES (?, ?, ?, ?, ?)`
      ).run(
        args.address,
        args.data,
        args.functionName,
        args.blockNumber.toString(),
        args.result
      );
    },
  };
}
