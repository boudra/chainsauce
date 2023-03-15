import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

export default class Cache {
  private dir: string;

  constructor(dir: string) {
    this.dir = dir;
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

  async lazy<T>(key: string, fun: () => Promise<T>): Promise<T> {
    const cachedValue = await this.get<T>(key);

    if (cachedValue) {
      return cachedValue;
    } else {
      const value = await fun();
      this.set(key, value);
      return value;
    }
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
