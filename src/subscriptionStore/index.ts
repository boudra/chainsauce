import { Hex, ToBlock } from "@/types";

export type Subscription = {
  id: string;
  chainId: number;
  contractName: string;
  contractAddress: Hex;
  indexedToBlock: bigint;
  indexedToLogIndex: number;
  fromBlock: bigint;
  toBlock: ToBlock;
  createdAt: Date;
  updatedAt: Date;
};

export interface SubscriptionStore {
  init(): Promise<void>;

  save(subscription: Subscription): Promise<void>;
  get(id: string): Promise<Subscription | null>;
  delete(id: string): Promise<void>;
  all(chainId: number): Promise<Subscription[]>;
  update(
    id: string,
    update: Pick<Subscription, "indexedToBlock" | "indexedToLogIndex">
  ): Promise<void>;
}
