import type { Storage, Subscription } from "../index";
import { ethers } from "ethers";
import fs from "node:fs/promises";
import path from "node:path";

type Document = { [key: string]: any };

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

  collection(key: string) {
    if (!this.collections[key]) {
      this.collections[key] = [];
    }

    let data = this.collections[key];

    return {
      insert(document: Document): Document {
        if (typeof document !== "object") {
          throw new Error("Document must be an object");
        }

        data.push(document);

        return document;
      },
      findById(id: any): Document | undefined {
        return data.find((doc: any) => doc.id === id);
      },
      updateById(id: string, fun: (doc: Document) => Document): Document {
        const index = data.findIndex((doc: Document) => doc.id === id);
        data[index] = fun(data[index]);
        return data[index];
      },
      updateWhere(
        filter: (doc: Document) => boolean,
        fun: (doc: Document) => Document
      ): Document {
        const index = data.findIndex(filter);
        if (index) {
          data[index] = fun(data[index]);
        }
        return data[index];
      },
      findWhere(filter: (doc: Document) => boolean) {
        return data.filter(filter);
      },
      findOneWhere(filter: (doc: Document) => boolean) {
        return data.find(filter);
      },
      all(): Document[] {
        return data;
      },
    };
  }

  async getSubscriptions(): Promise<Subscription[]> {
    return this.collection("_subscriptions")
      .all()
      .map((sub: any) => ({
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
    let index: { [key: string]: string } = {};

    for (let name in this.collections) {
      index[name] = `${name}.json`;
      let filename = path.join(this.dir, index[name]);
      await fs.mkdir(path.dirname(filename), { recursive: true });
      await fs.writeFile(filename, JSON.stringify(this.collections[name]));
    }

    await fs.writeFile(
      path.join(this.dir, `_index.json`),
      JSON.stringify(index)
    );
  }

  async read(): Promise<void> {
    let indexFilename = path.join(this.dir, `_index.json`);

    try {
      let index: { [key: string]: string } = JSON.parse(
        (await fs.readFile(indexFilename)).toString()
      );

      for (let name in index) {
        let collection: any = JSON.parse(
          (await fs.readFile(path.join(this.dir, index[name]))).toString()
        );
        this.collections[name] = collection;
      }
    } catch {}
  }
}
