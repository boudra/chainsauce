import fs from "node:fs/promises";
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

type Index = { [key: string]: number };

async function loadJsonData<T extends Document>(
  filename: string,
  decodeJson: (data: string) => unknown = JSON.parse
): Promise<{ data: T[]; index: Index }> {
  try {
    await fs.stat(filename);
  } catch (err) {
    return { data: [], index: {} };
  }

  const fileContents = await fs.readFile(filename, "utf-8");
  const data = decodeJson(fileContents) as T[];
  const index = buildIndex(data);

  return { data, index };
}

class JsonCollection<T extends Document> implements Collection<T> {
  private filename: string;
  private loadingPromise: Promise<{ data: T[]; index: Index }> | null = null;
  private savingPromise: Promise<void> | null = null;
  private debouncedSave: ReturnType<typeof debounce>;
  private encodeJson: (data: unknown) => string;
  private decodeJson: (data: string) => unknown;

  constructor(filename: string, options: Options) {
    this.filename = filename;
    this.encodeJson = options.encodeJson ?? JSON.stringify;
    this.decodeJson = options.decodeJson ?? JSON.parse;
    this.debouncedSave = debounce(() => this.save(), options.writeDelay ?? 0);
  }

  private async executeOperation<TReturn>(
    op: (data: { data: T[]; index: Index }) => Promise<TReturn>
  ): Promise<TReturn> {
    const { data, index } = await this.load();
    const result = await op({ data, index });
    this.debouncedSave(data);
    return result;
  }

  private async load(): Promise<{ data: T[]; index: Index }> {
    this.debouncedSave.cancel();
    // Wait for any ongoing save operation to complete
    if (this.savingPromise !== null) {
      await this.savingPromise;
    }

    if (this.loadingPromise !== null) {
      return this.loadingPromise;
    }

    this.loadingPromise = loadJsonData(this.filename, this.decodeJson);

    return this.loadingPromise;
  }

  private async save() {
    if (this.loadingPromise === null) {
      throw new Error("Saving without loading first!");
    }

    const { data } = await this.loadingPromise;

    this.savingPromise = fs
      .mkdir(path.dirname(this.filename), { recursive: true })
      .then(() => fs.writeFile(`${this.filename}.write`, this.encodeJson(data)))
      .then(() => fs.rename(`${this.filename}.write`, this.filename))
      .finally(() => {
        this.savingPromise = null;
        this.loadingPromise = null;
      });

    return this.savingPromise;
  }

  async insert(document: T): Promise<T> {
    if (typeof document !== "object") {
      throw new Error("Document must be an object");
    }

    return this.executeOperation(async ({ data, index }) => {
      data.push(document);
      index[document.id] = data.length - 1;

      return document;
    });
  }

  async findById(id: string): Promise<T | null> {
    return this.executeOperation(async ({ data, index }) => {
      return data[index[id]] ?? null;
    });
  }

  async updateById(id: string, fun: (doc: T) => T): Promise<T | null> {
    return this.executeOperation(async ({ data, index }) => {
      if (index[id] === undefined) {
        return null;
      }

      const updatedRecord = fun(data[index[id]]);
      data[index[id]] = { ...updatedRecord, id: data[index[id]].id };

      return data[index[id]];
    });
  }

  // returns true it inserted a new record
  async upsertById(id: string, fun: (doc: T | null) => T): Promise<boolean> {
    return this.executeOperation(async ({ data, index }) => {
      const isNewDocument = index[id] === undefined;

      if (isNewDocument) {
        const document = fun(null);
        data.push(document);
        index[document.id] = data.length - 1;
      } else {
        const updatedRecord = fun(data[index[id]]);
        data[index[id]] = { ...updatedRecord, id: data[index[id]].id };
      }

      return isNewDocument;
    });
  }

  async all(): Promise<T[]> {
    return this.executeOperation(async ({ data }) => {
      return data;
    });
  }
}

export interface Options {
  dir: string;
  writeDelay?: number;
  encodeJson?: (data: unknown) => string;
  decodeJson?: (data: string) => unknown;
}

export function createJsonDatabase(options: Options): Database {
  const collections: Record<string, Collection<Document>> = {};

  return {
    collection<T extends Document>(name: string): Collection<T> {
      if (collections[name] === undefined) {
        const filename = path.join(options.dir, `${name}.json`);
        collections[name] = new JsonCollection(filename, options);
      }

      return collections[name] as Collection<T>;
    },
  };
}
