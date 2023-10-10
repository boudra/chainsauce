import {
  Contracts,
  CreateSubscriptionOptions,
  Indexer,
  Config,
  createIndexer,
} from "@/indexer";
import { Abi, ExtractAbiEventNames } from "abitype";
import { EventHandler, EventHandlers } from "@/types";

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

  addEventListeners<
    TContractName extends keyof TAbis,
    TEventName extends ExtractAbiEventNames<TAbis[TContractName]>
  >(args: {
    contract: TContractName;
    events:
      | Partial<
          EventHandlers<TAbis, TContext, TAbis[TContractName], TEventName>
        >
      | ExtractAbiEventNames<TAbis[TContractName]>[];
  }): IndexerBuilder<TAbis, TContext> {
    const { contract: contractName, events } = args;

    const newOptions = {
      ...this.options,
      contracts: {
        ...(this.options.contracts ?? {}),
        [contractName]: {
          ...(this.options.contracts?.[contractName] ?? {}),
          events,
        },
      },
    } as Config<TAbis, TContext>;

    return new IndexerBuilder(newOptions);
  }

  addEventHandler<
    TContractName extends keyof TAbis,
    TEventName extends ExtractAbiEventNames<TAbis[TContractName]>
  >(args: {
    contract: TContractName;
    event: TEventName | ExtractAbiEventNames<TAbis[TContractName]>;
    handler: EventHandler<TAbis, TContext, TAbis[TContractName], TEventName>;
  }) {
    const { contract: contractName, event: eventName, handler } = args;

    const newOptions = {
      ...this.options,
      contracts: {
        ...(this.options.contracts ?? {}),
        [contractName]: {
          ...(this.options.contracts?.[contractName] ?? {}),
          handlers: {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            ...(this.options.contracts?.[contractName]?.events ?? {}),
            [eventName]: handler,
          },
        },
      },
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

  cache(cache: Config<TAbis>["cache"]): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, cache: cache });
  }

  subscriptionStore(
    subscriptionStore: Config<TAbis>["subscriptionStore"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, subscriptionStore });
  }

  addSubscription(
    options: CreateSubscriptionOptions<keyof TAbis>
  ): IndexerBuilder<TAbis, TContext> {
    const { contract: contractName } = options;

    if (!this.options.contracts) {
      throw new Error(
        `Failed to add contract subscription: contracts are not defined`
      );
    }

    const contract = this.options.contracts?.[contractName];

    if (!contract) {
      throw new Error(
        `Failed to add contract subscription: contract ${String(
          contractName
        )} is not found`
      );
    }

    const newOptions = {
      ...this.options,
      contracts: {
        ...this.options.contracts,
        [contractName]: {
          ...contract,
          subscriptions: [
            // TODO: something is up with the types here, logic is valid
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            ...(contract.subscriptions ?? []),
            {
              address: options.address,
              fromBlock: options.fromBlock,
              toBlock: options.toBlock,
            },
          ],
        },
      },
    } as Config<TAbis, TContext>;

    return new IndexerBuilder(newOptions);
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
