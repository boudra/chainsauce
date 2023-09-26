export type Document = { id: string; [key: string]: unknown };

export interface Collection<T extends Document> {
  insert(document: T): Promise<T>;
  findById(id: string): Promise<T | null>;
  updateById(id: string, fun: (doc: T) => Omit<T, "id">): Promise<T | null>;
  upsertById(
    id: string,
    fun: (doc: T | null) => Omit<T, "id">
  ): Promise<boolean>;
  all(): Promise<T[]>;
}

export interface Database {
  collection<T extends Document>(name: string): Collection<T>;
}
