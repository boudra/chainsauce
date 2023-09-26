import { Abi, ExtractAbiEventNames } from "abitype";
import { GetEventArgs } from "viem";

export type Hex = `0x${string}`;

export type EventHandler<T extends Abi, N extends ExtractAbiEventNames<T>> = (
  event: BaseEvent<
    N,
    GetEventArgs<
      T,
      N,
      { EnableUnion: false; IndexedOnly: false; Required: true }
    >
  >
) => void;

export type EventHandlers<T extends Abi, N extends ExtractAbiEventNames<T>> = {
  [K in N]: EventHandler<T, K>;
};

export type Event<T extends Abi, N extends ExtractAbiEventNames<T>> = BaseEvent<
  N,
  GetEventArgs<T, N, { EnableUnion: false; IndexedOnly: false; Required: true }>
>;

export type BaseEvent<N = string, P = Record<string, unknown>> = {
  name: N;
  params: P;
  address: Hex;
  topic: Hex;
  transactionHash: Hex;
  blockNumber: bigint;
  logIndex: number;
};

export type ToBlock = "latest" | bigint;
