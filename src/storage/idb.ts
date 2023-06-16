import { IDBPDatabase, openDB } from "idb";
import type { Storage, Subscription } from "../index";
import { ethers } from "ethers";

/**
 * An interface representing an entity in the IndexedDB database.
 * @interface
 * @property {string} name - The name of the entity.
 * @property {Object} index - An optional object representing an index on the entity.
 * @property {string} index.name - The name of the index.
 * @property {string} index.keyPath - The key path of the index.
 */
interface Entity {
  name: string;
  index?: {
    name: string;
    keyPath: string;
  };
}
export default class IdbStorage implements Storage {
  /**
   * The IndexedDB database instance.
   * @type {IDBPDatabase<ChainsauceDB> | null}
   */
  db: IDBPDatabase | null = null;

  /**
   * The array of entity objects.
   * @type {Entity[]}
   */
  entities: Entity[];

  /**
   * Constructs a new IdbStorage instance with the given array of entity objects.
   * @param {Entity[]} entities - The array of entity objects.
   */
  constructor(entities: Entity[]) {
    this.entities = entities;
  }

  /**
   * Initializes the IndexedDB database with the name "chainsauce" and version 1.
   * @returns A promise that resolves to void.
   */
  async init() {
    /**
     * The IndexedDB database instance.
     * @type {IDBPDatabase<ChainsauceDB> | null}
     */
    this.db = await openDB("chainsauce", 1, {
      /**
       * The upgrade callback function that is called when the database is first created or when the version number is incremented.
       * @param {IDBPDatabase<ChainsauceDB>} db - The database instance.
       */
      upgrade: (db) => {
        /**
         * Creates an object store for subscriptions in the IndexedDB database with the name "__subscriptions" and a key path of "address".
         * An index is also created on the store with the name "by-address" and the key path "address".
         * @param {IDBPDatabase<ChainsauceDB>} db - The database instance.
         */
        const subscriptionStore = db.createObjectStore("__subscriptions", {
          keyPath: "address",
        });
        subscriptionStore.createIndex("by-address", "address");

        /**
         * Loops through each entity in the `entities` array and creates an object store for each entity in the IndexedDB database.
         * If an entity has an index, a new object store is created with the index as the key path and an index is created on the store.
         * @param {IDBPDatabase<ChainsauceDB>} db - The database instance.
         * @param {Entity[]} entities - The array of entity objects.
         */
        for (const entity of this.entities) {
          if (!entity.index) {
            db.createObjectStore(entity.name);
            return;
          }

          let store = db.createObjectStore(entity.name, {
            keyPath: entity.index.keyPath,
          });
          store.createIndex(entity.index.name, entity.index.keyPath);
        }
      },
    });
  }

  /**
   * Retrieves all subscriptions from the "__subscriptions" object store in the IndexedDB database.
   * @returns A promise that resolves to an array of Subscription objects.
   * @throws An error if the database has not been initialized.
   */
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

  /**
   * Retrieves all subscriptions from the "__subscriptions" object store in the IndexedDB database.
   * @returns A promise that resolves to an array of Subscription objects.
   * @throws An error if the database has not been initialized.
   */
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
