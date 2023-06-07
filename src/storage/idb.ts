import { IDBPDatabase, openDB } from "idb";
import type { Storage, Subscription } from "../index";
import { ethers } from "ethers";

export default class IdbStorage implements Storage {
  db: IDBPDatabase | null = null;
  entities: string[];

  constructor(entities: string[]) {
    this.entities = entities;
  }

  async init() {
    this.db = await openDB("chainsauce", 1, {
      upgrade: (db) => {
        const subscriptionStore = db.createObjectStore("__subscriptions", {
          keyPath: "address",
        });
        subscriptionStore.createIndex("by-address", "address");

        for (const entity of this.entities) {
          db.createObjectStore(entity);
        }
      },
    });
  }

  async getSubscriptions(): Promise<Subscription[]> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }
    const subs = await this.db.getAll("__subscriptions");

    return subs.map(
      (sub: { address: string; abi: string; fromBlock: number }) => ({
        address: sub.address,
        contract: new ethers.Contract(sub.address, JSON.parse(sub.abi)),
        fromBlock: sub.fromBlock,
      })
    );
  }

  async setSubscriptions(subscriptions: Subscription[]): Promise<void> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }
    const tx = this.db.transaction("__subscriptions", "readwrite");
    const store = tx.objectStore("__subscriptions");

    await store.clear();
    const inserts = subscriptions.map((sub) =>
      tx.store.add({
        address: sub.address,
        abi: sub.contract.interface.format(
          ethers.utils.FormatTypes.json
        ) as string,
        fromBlock: sub.fromBlock,
      })
    );

    await Promise.all(inserts);
    await tx.done;
  }
}
