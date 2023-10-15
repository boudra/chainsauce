import { ExtractAbiEventNames } from "abitype";

import type { Indexer } from "@/indexer";
import { EventHandlerArgs } from "@/types";

type ExtractAbis<T> = T extends Indexer<infer Abis> ? Abis : never;
type ExtractContext<T> = T extends Indexer<infer _abis, infer TContext>
  ? TContext
  : never;

export type UserEventHandlerArgs<
  T extends Indexer,
  TAbiName extends keyof ExtractAbis<T> = keyof ExtractAbis<T>,
  TEventName extends ExtractAbiEventNames<
    ExtractAbis<T>[TAbiName]
  > = ExtractAbiEventNames<ExtractAbis<T>[TAbiName]>
> = EventHandlerArgs<
  ExtractAbis<T>,
  ExtractContext<T>,
  ExtractAbis<T>[TAbiName],
  TEventName
>;
