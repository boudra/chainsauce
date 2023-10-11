import fs from "node:fs/promises";
import path from "node:path";
import fastq from "fastq";

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

async function loadJsonData<T extends Document>(
  filename: string
): Promise<{ data: T[]; index: Index }> {
  let data: T[] = [];
  let index: Index = {};

  try {
    const fileContents = await fs.readFile(filename, "utf-8");
    data = JSON.parse(fileContents, parse) as T[];
    index = buildIndex(data);
  } catch (err) {
    if (
      typeof err === "object" &&
      err !== null &&
      "code" in err &&
      err.code === "ENOENT"
    ) {
      await fs.mkdir(path.dirname(filename), { recursive: true });
      await fs.writeFile(filename, "[]");
    } else {
      throw err;
    }
  }

  return { data, index };
}

class JsonCollection<T extends Document> implements Collection<T> {
  private filename: string;
  private loadingPromise: Promise<{ data: T[]; index: Index }> | null = null;
  private savingPromise: Promise<void> | null = null;
  private debouncedSave: ReturnType<typeof debounce>;

  private taskQueue = fastq.promise(
    this,
    async (task) => {
      this.debouncedSave.cancel();
      const { data, index } = await this.load();
      const result = await task({ data, index });
      this.debouncedSave(data);
      return result;
    },
    1
  );

  constructor(filename: string, writeDelay: number) {
    this.filename = filename;
    this.debouncedSave = debounce(() => this.save(), writeDelay);
  }

  private queueTask<TReturn>(
    task: (data: { data: T[]; index: Index }) => Promise<TReturn>
  ): Promise<TReturn> {
    return this.taskQueue.push(task);
  }

  private async load(): Promise<{ data: T[]; index: Index }> {
    // Wait for any ongoing save operation to complete
    if (this.savingPromise) {
      await this.savingPromise;
    }

    if (this.loadingPromise !== null) {
      return this.loadingPromise;
    }

    this.loadingPromise = loadJsonData(this.filename);

    return this.loadingPromise;
  }

  private async save() {
    if (this.loadingPromise === null) {
      throw new Error("Saving without loading first!");
    }

    const { data } = await this.loadingPromise;

    this.savingPromise = fs
      .writeFile(this.filename, JSON.stringify(data, stringify))
      .catch((err) => {
        console.log("save failed", err);
      })
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

    return this.queueTask(async ({ data, index }) => {
      data.push(document);
      index[document.id] = data.length - 1;

      this.debouncedSave();

      return document;
    });
  }

  async findById(id: string): Promise<T | null> {
    return this.queueTask(async ({ data, index }) => {
      return data[index[id]] ?? null;
    });
  }

  async updateById(id: string, fun: (doc: T) => T): Promise<T | null> {
    return this.queueTask(async ({ data, index }) => {
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
    return this.queueTask(async ({ data, index }) => {
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
    return this.queueTask(async ({ data }) => {
      return data;
    });
  }
}

export interface Options {
  dir: string;
  writeDelay?: number;
}

export function createJsonDatabase(options: Options): Database {
  const collections: Record<string, Collection<Document>> = {};

  return {
    collection<T extends Document>(name: string): Collection<T> {
      if (!collections[name]) {
        const filename = path.join(options.dir, `${name}.json`);

        collections[name] = new JsonCollection(
          filename,
          options.writeDelay ?? 0
        );
      }

      return collections[name] as Collection<T>;
    },
  };
}
