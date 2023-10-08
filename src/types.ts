import { Abi, ExtractAbiEventNames } from "abitype";
import { GetEventArgs } from "viem";
import type { Indexer } from "@/index";

export type Hex = `0x${string}`;

export type EventHandler<
  TAbi extends Abi,
  N extends ExtractAbiEventNames<TAbi> = ExtractAbiEventNames<TAbi>,
  TContext = unknown,
  TAbis extends Record<string, Abi> = Record<string, Abi>
> = (args: {
  context: TContext;
  readContract: Indexer<TAbis, TContext>["readContract"];
  subscribeToContract: Indexer<TAbis, TContext>["subscribeToContract"];
  event: BaseEvent<
    N,
    GetEventArgs<
      TAbi,
      N,
      { EnableUnion: false; IndexedOnly: false; Required: true }
    >
  >;
}) => Promise<void>;

export type EventHandlers<
  TAbi extends Abi,
  N extends ExtractAbiEventNames<TAbi> = ExtractAbiEventNames<TAbi>,
  TContext = unknown,
  TAbis extends Record<string, Abi> = Record<string, Abi>
> = {
  [K in N]: EventHandler<TAbi, K, TContext, TAbis>;
};

export type Event<
  T extends Abi = Abi,
  N extends ExtractAbiEventNames<T> = ExtractAbiEventNames<T>
> = BaseEvent<
  N,
  GetEventArgs<T, N, { EnableUnion: false; IndexedOnly: false; Required: true }>
>;

type BaseEvent<N = string, P = Record<string, unknown>> = {
  name: N;
  params: P;
  address: Hex;
  topic: Hex;
  transactionHash: Hex;
  blockNumber: bigint;
  logIndex: number;
};

export type ToBlock = "latest" | bigint;
