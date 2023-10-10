import {
  Abi,
  AbiFunction,
  AbiParametersToPrimitiveTypes,
  ExtractAbiEventNames,
  ExtractAbiFunction,
  ExtractAbiFunctionNames,
} from "abitype";
import { GetEventArgs } from "viem";
import type { CreateSubscriptionOptions } from "@/indexer";

export type Hex = `0x${string}`;

export type EventHandler<
  TAbi extends Abi = Abi,
  N extends ExtractAbiEventNames<TAbi> = ExtractAbiEventNames<TAbi>,
  TContext = unknown,
  TAbis extends Record<string, Abi> = Record<string, Abi>
> = (args: {
  context: TContext;
  chainId: number;
  subscribeToContract: (
    opts: Omit<CreateSubscriptionOptions<keyof TAbis>, "fromBlock">
  ) => void;
  event: BaseEvent<
    N,
    GetEventArgs<
      TAbi,
      N,
      { EnableUnion: false; IndexedOnly: false; Required: true }
    >
  >;

  readContract<
    TContractName extends keyof TAbis,
    TAbi extends Abi = TAbis[TContractName],
    TFunctionName extends ExtractAbiFunctionNames<
      TAbi,
      "pure" | "view"
    > = ExtractAbiFunctionNames<TAbi, "pure" | "view">,
    TAbiFunction extends AbiFunction = ExtractAbiFunction<TAbi, TFunctionName>,
    TReturn = AbiParametersToPrimitiveTypes<TAbiFunction["outputs"], "outputs">
  >(args: {
    contract: TContractName | keyof TAbis;
    address: Hex;
    functionName:
      | TFunctionName
      | ExtractAbiFunctionNames<TAbi, "pure" | "view">;
    args?: AbiParametersToPrimitiveTypes<TAbiFunction["inputs"], "inputs">;
  }): Promise<TReturn extends readonly [infer inner] ? inner : TReturn>;
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

export type Contract<
  TAbi extends Abi = Abi,
  TContext = unknown,
  TAbis extends Record<string, Abi> = Record<string, Abi>,
  N extends ExtractAbiEventNames<TAbi> = ExtractAbiEventNames<TAbi>
> = {
  abi: TAbi;
  subscriptions?: {
    address: Hex;
    fromBlock?: bigint;
    toBlock?: ToBlock;
  }[];
  handlers?: Partial<EventHandlers<TAbi, N, TContext, TAbis>>;
};
