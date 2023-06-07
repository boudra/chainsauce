import type { Storage, Subscription } from "../index";
import { ethers } from "ethers";
import { Database } from "better-sqlite3";

type SubConfig = {
  address: string;
  abi: string;
  fromBlock: number;
};

export default class SqliteStorage implements Storage {
  db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  async init() {
    this.db
      .prepare(
        `CREATE TABLE IF NOT EXISTS "__subscriptions" (
        "address" TEXT NOT NULL PRIMARY KEY,
        "abi" TEXT NOT NULL,
        "fromBlock" INTEGER NOT NULL
      )`
      )
      .run();
  }

  async getSubscriptions(): Promise<Subscription[]> {
    return this.db
      .prepare("SELECT * FROM __subscriptions")
      .all()
      .map((sub) => {
        const _sub = sub as SubConfig;

        return {
          address: _sub.address,
          contract: new ethers.Contract(_sub.address, JSON.parse(_sub.abi)),
          fromBlock: _sub.fromBlock,
        };
      });
  }

  async setSubscriptions(subscriptions: Subscription[]): Promise<void> {
    const truncate = this.db.prepare("DELETE FROM __subscriptions");
    const insert = this.db.prepare(
      "INSERT INTO __subscriptions VALUES (?, ?, ?)"
    );

    this.db.transaction(() => {
      truncate.run();

      for (const sub of subscriptions) {
        insert.run(
          sub.address,
          sub.contract.interface.format(
            ethers.utils.FormatTypes.json
          ) as string,
          sub.fromBlock
        );
      }
    })();
  }
}
