import SqliteDatabase from "better-sqlite3";
import { BaseEvent, ToBlock, Hex } from "@/types";

export interface EventStore {
  insert(event: BaseEvent): Promise<void>;
  getEvents(args: {
    address: Hex;
    topic: Hex;
    fromBlock: bigint;
    toBlock: ToBlock;
  }): Promise<BaseEvent[]>;
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

export function createMemoryEventStore(): EventStore {
  return createSqliteEventStore(":memory:");
}

export function createSqliteEventStore(dbPath: string): EventStore {
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

  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_address_topic_block
     ON events (address, topic, blockNumber);`
  );

  return {
    async insert(event: BaseEvent) {
      const insertQuery = db.prepare(
        `INSERT INTO events (name, params, address, topic, transactionHash, blockNumber, logIndex)
         VALUES (?, ?, ?, ?, ?, ?, ?)
        `
      );
      insertQuery.run(
        event.name,
        JSON.stringify(event.params, stringify),
        event.address,
        event.topic,
        event.transactionHash,
        event.blockNumber.toString(),
        event.logIndex
      );
    },

    async getEvents(args: {
      address: Hex;
      topic: Hex;
      fromBlock: bigint;
      toBlock: ToBlock;
    }): Promise<BaseEvent[]> {
      const query = db.prepare(`
      SELECT * FROM events
      WHERE address = ? AND topic = ? AND blockNumber >= ?
        AND (blockNumber <= ? OR ? = 'latest')
      `);

      const rows = query.all(
        args.address,
        args.topic,
        args.fromBlock,
        args.toBlock,
        args.toBlock
      ) as EventRow[];

      return rows.map((row) => ({
        name: row.name,
        params: JSON.parse(row.params, parse),
        address: row.address,
        topic: row.topic,
        transactionHash: row.transactionHash,
        blockNumber: BigInt(row.blockNumber),
        logIndex: row.logIndex,
      }));
    },
  };
}
