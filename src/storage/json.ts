/* eslint-disable @typescript-eslint/no-non-null-assertion */
/* eslint-disable @typescript-eslint/no-explicit-any */
import type { Storage, Subscription } from "../index";
import { ethers } from "ethers";
import fs from "node:fs/promises";
import { mkdirSync } from "node:fs";
import path from "node:path";
import debounce from "../debounce.js";

type Document = { [key: string]: any };

class Collection<T extends Document> {
  private filename: string;
  private data: T[] | null = null;
  private save: ReturnType<typeof debounce>;

  constructor(filename: string) {
    this.filename = filename;
    this.save = debounce(() => this._save(), 500);
    mkdirSync(path.dirname(filename), { recursive: true });
  }

  private async load(): Promise<T[]> {
    try {
      if (this.data !== null) {
        return this.data;
      }

      this.data = JSON.parse(
        (await fs.readFile(this.filename)).toString()
      ) as T[];
    } catch {
      this.data = [];
    }

    return this.data;
  }

  private async _save() {
    if (this.data === null) {
      throw new Error("Saving without loading first!");
    }

    const rt = await fs.writeFile(this.filename, JSON.stringify(this.data));
    this.data == null;
    return rt;
  }

  async insert(document: T): Promise<T> {
    if (typeof document !== "object") {
      throw new Error("T must be an object");
    }

    await this.load();

    this.data!.push(document);

    this.save();

    return document;
  }

  async findById(id: any): Promise<T | undefined> {
    const data = await this.load();
    return data.find((doc: T) => doc.id === id);
  }

  async updateById(id: string, fun: (doc: T) => T): Promise<T> {
    await this.load();

    const index = this.data!.findIndex((doc: T) => doc.id === id);

    this.data![index] = fun(this.data![index]);

    const item = this.data![index];

    this.save();

    return item;
  }

  async updateOneWhere(
    filter: (doc: T) => boolean,
    fun: (doc: T) => T
  ): Promise<T> {
    await this.load();
    const index = this.data!.findIndex(filter);

    this.data![index] = fun(this.data![index]);

    const item = this.data![index];

    this.save();

    return item;
  }

  async findWhere(filter: (doc: T) => boolean): Promise<T[]> {
    await this.load();
    return this.data!.filter(filter);
  }

  async findOneWhere(filter: (doc: T) => boolean): Promise<T | undefined> {
    await this.load();
    return this.data!.find(filter);
  }

  async all(): Promise<T[]> {
    await this.load();
    return this.data!;
  }

  async replaceAll(data: T[]): Promise<T[]> {
    this.data = data;
    this.save();
    return this.data;
  }
}

export default class JsonStorage implements Storage {
  dir: string;
  collections: { [key: string]: Collection<Document> };

  constructor(dir: string) {
    this.dir = dir;
    this.collections = {};
  }

  async init() {
    await fs.mkdir(this.dir, { recursive: true });
  }

  collection<T extends Document>(key: string) {
    if (!this.collections[key]) {
      this.collections[key] = new Collection(
        path.join(this.dir, `${key}.json`)
      );
    }

    return this.collections[key] as Collection<T>;
  }

  async getSubscriptions(): Promise<Subscription[]> {
    const subs = await this.collection<{
      address: string;
      abi: string;
      fromBlock: number;
    }>("_subscriptions").all();

    return subs.map((sub) => ({
      address: sub.address,
      contract: new ethers.Contract(sub.address, sub.abi),
      fromBlock: sub.fromBlock,
    }));
  }

  async setSubscriptions(subscriptions: Subscription[]): Promise<void> {
    this.collection("_subscriptions").replaceAll(
      subscriptions.map((sub) => ({
        address: sub.address,
        abi: JSON.parse(
          sub.contract.interface.format(ethers.utils.FormatTypes.json) as string
        ),
        fromBlock: sub.fromBlock,
      }))
    );
  }

  async write(): Promise<void> {
    const index: { [key: string]: string } = {};

    for (const name in this.collections) {
      index[name] = `${name}.json`;
    }

    await fs.writeFile(
      path.join(this.dir, `_index.json`),
      JSON.stringify(index)
    );
  }
}
