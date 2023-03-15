import type { Storage, Subscription } from "../index";
import { ethers } from "ethers";
import fs from "node:fs/promises";
import path from "node:path";

type Document = { [key: string]: unknown };

export default class JsonStorage implements Storage {
  dir: string;
  collections: { [key: string]: Document[] };

  constructor(dir: string) {
    this.dir = dir;
    this.collections = {};
  }

  async init() {
    await fs.mkdir(this.dir, { recursive: true });
  }

  collection<T extends Document>(key: string) {
    if (!this.collections[key]) {
      this.collections[key] = [];
    }

    const data = this.collections[key] as T[];

    return {
      insert(document: T): T {
        if (typeof document !== "object") {
          throw new Error("T must be an object");
        }

        data.push(document);

        return document;
      },
      findById(id: unknown): T | undefined {
        return data.find((doc: T) => doc.id === id);
      },
      updateById(id: string, fun: (doc: T) => T): T {
        const index = data.findIndex((doc: T) => doc.id === id);

        if (index < 0) {
          throw new Error(`Document with id: ${id} not found`);
        }

        data[index] = fun(data[index]);
        return data[index];
      },
      updateWhere(filter: (doc: T) => boolean, fun: (doc: T) => T): T {
        const index = data.findIndex(filter);

        if (index < 0) {
          throw new Error(`Document with id: ${id} not found`);
        }

        data[index] = fun(data[index]);

        return data[index];
      },
      findWhere(filter: (doc: T) => boolean) {
        return data.filter(filter);
      },
      findOneWhere(filter: (doc: T) => boolean) {
        return data.find(filter);
      },
      all(): T[] {
        return data;
      },
    };
  }

  async getSubscriptions(): Promise<Subscription[]> {
    return this.collection<{
      address: string;
      abi: string;
      fromBlock: number;
    }>("_subscriptions")
      .all()
      .map((sub) => ({
        address: sub.address,
        contract: new ethers.Contract(sub.address, sub.abi),
        fromBlock: sub.fromBlock,
      }));
  }

  async setSubscriptions(subscriptions: Subscription[]): Promise<void> {
    this.collections["_subscriptions"] = subscriptions.map((sub) => ({
      address: sub.address,
      abi: JSON.parse(
        sub.contract.interface.format(ethers.utils.FormatTypes.json) as string
      ),
      fromBlock: sub.fromBlock,
    }));
  }

  async write(): Promise<void> {
    const index: { [key: string]: string } = {};

    for (const name in this.collections) {
      index[name] = `${name}.json`;
      const filename = path.join(this.dir, index[name]);
      await fs.mkdir(path.dirname(filename), { recursive: true });
      await fs.writeFile(filename, JSON.stringify(this.collections[name]));
    }

    await fs.writeFile(
      path.join(this.dir, `_index.json`),
      JSON.stringify(index)
    );
  }

  async read(): Promise<void> {
    const indexFilename = path.join(this.dir, `_index.json`);

    try {
      await fs.stat(indexFilename);
    } catch {
      return;
    }

    const index: { [key: string]: string } = JSON.parse(
      (await fs.readFile(indexFilename)).toString()
    );

    for (const name in index) {
      const collection = JSON.parse(
        (await fs.readFile(path.join(this.dir, index[name]))).toString()
      );
      this.collections[name] = collection;
    }
  }
}
