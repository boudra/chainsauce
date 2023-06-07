export default class Cache {
  private localStorage: Storage | null = null;
  private loading: Record<string, Promise<unknown>>;
  private isDisabled: boolean;

  constructor(isDisabled = false) {
    this.loading = {};
    this.isDisabled = isDisabled;

    try {
      this.localStorage = window.localStorage;
    } catch {
      console.warn("LocalStorage not available, caching disabled");
      this.localStorage = null;
      this.isDisabled = true;
    }
  }

  async get<T>(key: string): Promise<T | undefined> {
    if (this.isDisabled || !this.localStorage) {
      return undefined;
    }

    try {
      const res = this.localStorage.getItem(key);
      if (!res) {
        return undefined;
      }

      return JSON.parse(res) as T;
    } catch {
      return undefined;
    }
  }

  async set(key: string, value: string) {
    if (this.isDisabled || !this.localStorage) {
      return;
    }

    try {
      this.localStorage.setItem(key, value);
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
          this.set(key, JSON.stringify(value));
        });

        return promise;
      }
    });

    this.loading[key].then(() => {
      delete this.loading[key];
    });

    return this.loading[key] as Promise<T>;
  }
}
