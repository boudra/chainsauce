import {
  Abi,
  AbiFunction,
  AbiParametersToPrimitiveTypes,
  ExtractAbiEventNames,
  ExtractAbiFunction,
  ExtractAbiFunctionNames,
} from "abitype";
import { GetEventArgs } from "viem";

export type Hex = `0x${string}`;

export type ReadContractParameters<
  TAbis extends Record<string, Abi>,
  TContractName extends keyof TAbis,
  TAbi extends Abi = TAbis[TContractName],
  TFunctionName extends ExtractAbiFunctionNames<
    TAbi,
    "pure" | "view"
  > = ExtractAbiFunctionNames<TAbi, "pure" | "view">,
  TAbiFunction extends AbiFunction = ExtractAbiFunction<TAbi, TFunctionName>
> = {
  address: Hex;
  functionName: TFunctionName | ExtractAbiFunctionNames<TAbi, "pure" | "view">;
  args?: AbiParametersToPrimitiveTypes<TAbiFunction["inputs"], "inputs">;
  blockNumber: bigint;
};

export type ReadContractReturn<
  TAbi extends Abi = Abi,
  TFunctionName extends ExtractAbiFunctionNames<
    TAbi,
    "pure" | "view"
  > = ExtractAbiFunctionNames<TAbi, "pure" | "view">,
  TAbiFunction extends AbiFunction = ExtractAbiFunction<TAbi, TFunctionName>,
  TReturn = AbiParametersToPrimitiveTypes<TAbiFunction["outputs"], "outputs">
> = TReturn extends readonly [infer inner] ? inner : TReturn;

export type EventHandlerArgs<
  TAbis extends Record<string, Abi> = Record<string, Abi>,
  TContext = unknown,
  TAbi extends Abi = TAbis[keyof TAbis],
  N extends ExtractAbiEventNames<TAbi> = ExtractAbiEventNames<TAbi>
> = {
  context: TContext;
  chainId: number;
  event: N extends unknown
    ? BaseEvent<
        N,
        GetEventArgs<
          TAbi,
          N,
          { EnableUnion: false; IndexedOnly: false; Required: true }
        >
      >
    : never;
  readContract<
    TContractName extends keyof TAbis,
    TFunctionName extends ExtractAbiFunctionNames<
      TAbis[TContractName],
      "pure" | "view"
    >
  >(
    this: void,
    args: {
      contract: TContractName;
      functionName: TFunctionName;
    } & Omit<ReadContractParameters<TAbis, TContractName>, "blockNumber">
  ): Promise<ReadContractReturn<TAbis[TContractName], TFunctionName>>;

  subscribeToContract(options: {
    contract: keyof TAbis;
    address: string;
    toBlock?: ToBlock;
  }): void;
};

export type EventHandler<
  TAbis extends Record<string, Abi> = Record<string, Abi>,
  TContext = unknown,
  TAbi extends Abi = TAbis[keyof TAbis],
  N extends ExtractAbiEventNames<TAbi> = ExtractAbiEventNames<TAbi>
> = (args: EventHandlerArgs<TAbis, TContext, TAbi, N>) => Promise<void>;

export type EventHandlers<
  TAbis extends Record<string, Abi> = Record<string, Abi>,
  TContext = unknown,
  TAbi extends Abi = Abi,
  N extends ExtractAbiEventNames<TAbi> = ExtractAbiEventNames<TAbi>
> = {
  [K in N]: EventHandler<TAbis, TContext, TAbi, K>;
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
  events?:
    | Partial<EventHandlers<TAbis, TContext, TAbi, N>>
    | ExtractAbiEventNames<TAbi>[];
};

export type UnionToIntersection<U> = (
  U extends unknown ? (k: U) => void : never
) extends (k: infer I) => void
  ? I
  : never;
