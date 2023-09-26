import fs from "node:fs";
import path from "node:path";

import { Collection, Database, Document } from "@/storage";
import debounce from "@/debounce.js";

function buildIndex<T extends Document>(data: T[]): { [key: string]: number } {
  const index: { [key: string]: number } = {};

  for (let i = 0; i < data.length; i++) {
    index[data[i].id] = i;
  }

  return index;
}

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

type Index = { [key: string]: number };

class JsonCollection<T extends Document> implements Collection<T> {
  private filename: string;
  private data: T[] | null = null;
  private index: Index | null = null;
  private save: ReturnType<typeof debounce>;

  constructor(filename: string, writeDelay: number) {
    this.filename = filename;
    if (writeDelay > 0) {
      this.save = debounce(() => this._save(), writeDelay);
    } else {
      this.save = () => this._save();
    }
  }

  private load(): { data: T[]; index: Index } {
    try {
      if (this.data !== null && this.index !== null) {
        return { data: this.data, index: this.index };
      }

      this.data = JSON.parse(
        fs.readFileSync(this.filename).toString(),
        parse
      ) as T[];
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

    const rt = fs.writeFileSync(
      this.filename,
      JSON.stringify(this.data, stringify)
    );
    this.data = null;
    this.index = null;

    return rt;
  }

  insert(document: T): Promise<T> {
    if (typeof document !== "object") {
      throw new Error("Document must be an object");
    }

    const { data, index } = this.load();

    data.push(document);
    index[document.id] = data.length - 1;

    this.save();

    return Promise.resolve(document);
  }

  findById(id: string): Promise<T | null> {
    const { data, index } = this.load();
    return Promise.resolve(data[index[id]] ?? null);
  }

  updateById(id: string, fun: (doc: T) => T): Promise<T | null> {
    const { data, index } = this.load();

    if (index[id] === undefined) {
      return Promise.resolve(null);
    }

    const updatedRecord = fun(data[index[id]]);
    data[index[id]] = { ...updatedRecord, id: data[index[id]].id };

    this.save();

    return Promise.resolve(data[index[id]]);
  }

  // returns true it inserted a new record
  upsertById(id: string, fun: (doc: T | null) => T): Promise<boolean> {
    const { data, index } = this.load();

    const isNewDocument = index[id] === undefined;

    if (isNewDocument) {
      this.insert(fun(null));
    } else {
      const updatedRecord = fun(data[index[id]]);
      data[index[id]] = { ...updatedRecord, id: data[index[id]].id };
      this.save();
    }

    return Promise.resolve(isNewDocument);
  }

  async all(): Promise<T[]> {
    const { data } = this.load();
    return data;
  }
}

export interface Options {
  writeDelay?: number;
}

export async function createJsonDatabase(
  dir: string,
  options: Options
): Promise<Database> {
  const collections: Record<string, Collection<Document>> = {};

  fs.mkdirSync(dir, { recursive: true });

  return {
    collection<T extends Document>(name: string): Collection<T> {
      if (!collections[name]) {
        const filename = path.join(dir, `${name}.json`);

        if (!fs.existsSync(filename)) {
          fs.mkdirSync(path.dirname(filename), { recursive: true });
          fs.writeFileSync(filename, "[]");
        }

        collections[name] = new JsonCollection(
          filename,
          options.writeDelay ?? 500
        );
      }

      return collections[name] as Collection<T>;
    },
  };
}
