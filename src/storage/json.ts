/* eslint-disable @typescript-eslint/no-non-null-assertion */
/* eslint-disable @typescript-eslint/no-explicit-any */
import type { Storage, Subscription } from "../index";
import { ethers } from "ethers";
import fs from "node:fs";
import { mkdirSync } from "node:fs";
import path from "node:path";
import debounce from "../debounce.js";

type Document = { id: string; [key: string]: any };
type Index = { [key: string]: number };

function buildIndex<T extends Document>(data: T[]): { [key: string]: number } {
  const index: { [key: string]: number } = {};

  for (let i = 0; i < data.length; i++) {
    index[data[i].id] = i;
  }

  return index;
}

class Collection<T extends Document> {
  private filename: string;
  private data: T[] | null = null;
  private index: Index | null = null;
  private save: ReturnType<typeof debounce>;

  constructor(filename: string) {
    this.filename = filename;
    this.save = debounce(() => this._save(), 500);
    mkdirSync(path.dirname(filename), { recursive: true });
  }

  private load(): { data: T[]; index: Index } {
    try {
      if (this.data !== null && this.index !== null) {
        return { data: this.data, index: this.index };
      }

      this.data = JSON.parse(fs.readFileSync(this.filename).toString()) as T[];
      this.index = buildIndex(this.data);
    } catch {
      this.data = [];
      this.index = {};
    }

    return { data: this.data, index: this.index };
  }

  private _save() {
    if (this.data === null) {
      throw new Error("Saving without loading first!");
    }

    const rt = fs.writeFileSync(this.filename, JSON.stringify(this.data));
    this.data = null;
    this.index = null;

    return rt;
  }

  insert(document: T): Promise<T> {
    if (typeof document !== "object") {
      throw new Error("T must be an object");
    }

    const { data, index } = this.load();

    data.push(document);
    index[document.id] = data.length - 1;

    this.save();

    return Promise.resolve(document);
  }

  findById(id: any): Promise<T | undefined> {
    const { data, index } = this.load();
    return Promise.resolve(data[index[id]]);
  }

  updateById(id: string, fun: (doc: T) => T): Promise<T | undefined> {
    const { data, index } = this.load();

    if (index[id] === undefined) {
      return Promise.resolve(undefined);
    }

    data[index[id]] = fun(data[index[id]]);

    this.save();

    return Promise.resolve(data[index[id]]);
  }

  // returns true it inserted a new record
  upsertById(id: string, fun: (doc: T | undefined) => T): Promise<boolean> {
    const { data, index } = this.load();

    const isNewRecord = index[id] === undefined;

    if (isNewRecord) {
      this.insert(fun(undefined));
    } else {
      data[index[id]] = fun(data[index[id]]);
    }

    this.save();

    return Promise.resolve(isNewRecord);
  }

  async all(): Promise<T[]> {
    const { data } = this.load();
    return data;
  }

  async replaceAll(data: T[]): Promise<T[]> {
    this.data = data;
    this.index = buildIndex(data);
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
    fs.mkdirSync(this.dir, { recursive: true });
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
      id: string;
      abi: string;
      fromBlock: number;
    }>("_subscriptions").all();

    return subs.map((sub) => ({
      address: sub.id,
      contract: new ethers.Contract(sub.id, sub.abi),
      fromBlock: sub.fromBlock,
    }));
  }

  async setSubscriptions(subscriptions: Subscription[]): Promise<void> {
    this.collection("_subscriptions").replaceAll(
      subscriptions.map((sub) => ({
        id: sub.address,
        abi: JSON.parse(
          sub.contract.interface.format(ethers.utils.FormatTypes.json) as string
        ),
        fromBlock: sub.fromBlock,
      }))
    );
  }
}
