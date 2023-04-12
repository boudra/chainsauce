import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { mkdirSync } from "node:fs";

export default class Cache {
  private dir: string;
  private loading: Record<string, Promise<unknown>>;

  constructor(dir: string) {
    this.dir = dir;
    this.loading = {};
    mkdirSync(this.dir, { recursive: true });
  }

  private key(key: string) {
    return crypto.createHash("sha256").update(key).digest("hex");
  }

  private filename(key: string) {
    return path.join(this.dir, this.key(key));
  }

  async get<T>(key: string): Promise<T | undefined> {
    const filename = this.filename(key);

    try {
      return JSON.parse((await fs.readFile(filename)).toString());
    } catch {
      return undefined;
    }
  }

  lazy<T>(key: string, fun: () => Promise<T>): Promise<T> {
    if (this.loading[key] !== undefined) {
      return this.loading[key] as Promise<T>;
    }

    this.loading[key] = this.get<T>(key).then((cachedValue) => {
      if (cachedValue) {
        return cachedValue;
      } else {
        const promise = fun();

        promise.then((value) => {
          this.set(key, value);
        });

        return promise;
      }
    });

    this.loading[key].then(() => {
      delete this.loading[key];
    });

    return this.loading[key] as Promise<T>;
  }

  async set(key: string, value: unknown) {
    const filename = this.filename(key);

    try {
      await fs.writeFile(filename, JSON.stringify(value));
    } catch {
      return undefined;
    }
  }
}
