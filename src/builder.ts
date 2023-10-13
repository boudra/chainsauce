import { Contracts, Indexer, Config, createIndexer } from "@/indexer";
import { Abi, ExtractAbiEventNames } from "abitype";
import { EventHandlers } from "@/types";

export function buildIndexer() {
  return new IndexerBuilder({});
}

export class IndexerBuilder<
  TAbis extends Record<string, Abi>,
  TContext = unknown
> {
  options: Partial<Config<TAbis, TContext>>;

  constructor(options: Partial<Config<TAbis, TContext>>) {
    this.options = options;
  }

  contracts<TNewContracts extends Record<string, Abi>>(
    abis: TNewContracts
  ): IndexerBuilder<TNewContracts, TContext> {
    const contracts = Object.fromEntries(
      Object.entries(abis).map(([name, abi]) => [name, { abi }])
    ) as Contracts<TNewContracts>;

    const newOptions = {
      ...this.options,
      contracts: contracts,
    } as Config<TNewContracts, TContext>;

    return new IndexerBuilder(newOptions);
  }

  events<
    T extends {
      [K in keyof TAbis]?:
        | Partial<EventHandlers<TAbis, TContext, TAbis[K]>>
        | ExtractAbiEventNames<TAbis[K]>[];
    }
  >(events: T): IndexerBuilder<TAbis, TContext> {
    const contracts = { ...(this.options.contracts ?? {}) };

    for (const [contractName, contract] of Object.entries(events)) {
      if (!this.options.contracts?.[contractName]) {
        throw new Error(`Contract ${contractName} not found`);
      }

      const newContract = {
        ...this.options.contracts[contractName],
        events: contract,
      };

      contracts[contractName] = newContract;
    }

    const newOptions = {
      ...this.options,
      contracts: contracts,
    } as Config<TAbis, TContext>;

    return new IndexerBuilder(newOptions);
  }

  context<TNewContext>(
    context: TNewContext
  ): IndexerBuilder<TAbis, TNewContext> {
    const newOptions = {
      ...this.options,
      context,
    } as Config<TAbis, TNewContext>;

    return new IndexerBuilder<TAbis, TNewContext>(newOptions);
  }

  chain(chain: Config<TAbis>["chain"]): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, chain });
  }

  logger(logger: Config<TAbis>["logger"]): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, logger });
  }

  logLevel(
    logLevel: Config<TAbis>["logLevel"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, logLevel });
  }

  eventPollIntervalMs(
    eventPollIntervalMs: Config<TAbis>["eventPollDelayMs"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({
      ...this.options,
      eventPollDelayMs: eventPollIntervalMs,
    });
  }

  onProgress(
    onProgress: Config<TAbis>["onProgress"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, onProgress });
  }

  onEvent(
    onEvent: Config<TAbis, TContext>["onEvent"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, onEvent });
  }

  cache(cache: Config<TAbis>["cache"] | null): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, cache: cache ?? undefined });
  }

  subscriptionStore(
    subscriptionStore: Config<TAbis>["subscriptionStore"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, subscriptionStore });
  }

  build(): Indexer<TAbis, TContext> {
    if (!this.options.chain) {
      throw new Error("Failed to build indexer: chain is not set");
    }

    const options: Config<TAbis, TContext> = {
      ...this.options,
      chain: this.options.chain,
      contracts: this.options.contracts ?? ({} as Contracts<TAbis>),
      context: this.options.context ?? ({} as TContext),
    };

    return createIndexer(options);
  }
}
