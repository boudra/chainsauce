import { Abi, AbiEvent, ExtractAbiEventNames } from "abitype";
import {
  AbiFunction,
  AbiParametersToPrimitiveTypes,
  ExtractAbiFunction,
  ExtractAbiFunctionNames,
} from "abitype";
import {
  decodeEventLog,
  decodeFunctionResult,
  encodeEventTopics,
  encodeFunctionData,
  fromHex,
  getAbiItem,
} from "viem";

import {
  createRpcClient,
  RpcClient,
  JsonRpcRangeTooWideError,
  Log,
} from "@/rpc";
import { Cache } from "@/cache";
import { SubscriptionStore } from "@/subscriptionStore";
import { Logger, LoggerBackend, LogLevel } from "@/logger";
import { Hex, ToBlock, EventHandler, EventHandlers, Event } from "@/types";

export { Abi };

export { Database } from "@/storage";
export { createJsonDatabase } from "@/storage/json";
export { createSqliteCache } from "@/cache";
export { createSqliteSubscriptionStore } from "@/subscriptionStore";

// class BigIntMath {
//   static min(a: bigint, b: bigint): bigint {
//     return a < b ? a : b;
//   }
//   static max(a: bigint, b: bigint): bigint {
//     return a > b ? a : b;
//   }
// }

export { Hex, ToBlock, Event, LoggerBackend, LogLevel, Log };

type Subscription = {
  id: string;
  abi: Abi;
  contractName: string;
  contractAddress: `0x${string}`;
  topic: `0x${string}`;
  eventName: string;
  eventHandler?: EventHandler<Abi>;
  eventAbi: AbiEvent;
  toBlock: ToBlock;
  fromBlock: bigint;
  fetchedToBlock: bigint;
  indexedToBlock: bigint;
  indexedToLogIndex: number;
};

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

type UserEventHandler<T, TAbiName, TEventName> = T extends Indexer<
  infer TAbis,
  infer TContext
>
  ? (args: {
      context: TContext;
      readContract: T["readContract"];
      subscribeToContract: T["subscribeToContract"];
      event: TAbiName extends keyof TAbis
        ? TEventName extends ExtractAbiEventNames<TAbis[TAbiName]>
          ? Event<TAbis[TAbiName], TEventName>
          : never
        : never;
    }) => Promise<void>
  : never;

export { UserEventHandler as EventHandler };

export type CreateSubscriptionOptions<TName> = {
  contract: TName;
  address: string;
  indexedToBlock?: bigint;
  fromBlock?: bigint;
  fromLogIndex?: number;
  toBlock?: ToBlock;
  id?: string;
};

export type Options<TAbis extends Record<string, Abi>, TContext = unknown> = {
  logLevel?: keyof typeof LogLevel;
  logger?: LoggerBackend;
  eventPollIntervalMs?: number;
  context: TContext;
  contracts: ContractsFromAbis<TAbis>;
  rpc: RpcClient | { url: string; fetch?: typeof globalThis.fetch };
  onEvent?: (args: {
    event: Event;
    context: TContext;
    readContract: Indexer<TAbis, TContext>["readContract"];
    subscribeToContract: Indexer<TAbis, TContext>["subscribeToContract"];
  }) => Promise<void>;
  onProgress?: (progress: {
    currentBlock: bigint;
    targetBlock: bigint;
    pendingEventsCount: number;
  }) => void;
  cache?: Cache;
  subscriptionStore?: SubscriptionStore;
};

type ContractsFromAbis<TAbis extends Record<string, Abi>> = {
  [K in keyof TAbis]: Contract<TAbis[K]>;
};

class IndexerBuilder<TAbis extends Record<string, Abi>, TContext = unknown> {
  options: Partial<Options<TAbis, TContext>>;

  constructor(options: Partial<Options<TAbis, TContext>>) {
    this.options = options;
  }

  contracts<TNewContracts extends Record<string, Abi>>(
    abis: TNewContracts
  ): IndexerBuilder<TNewContracts, TContext> {
    const contracts = Object.fromEntries(
      Object.entries(abis).map(([name, abi]) => [name, { abi }])
    ) as ContractsFromAbis<TNewContracts>;

    const newOptions = {
      ...this.options,
      contracts: contracts,
    } as Options<TNewContracts, TContext>;

    return new IndexerBuilder(newOptions);
  }

  addEventHandlers<
    TContractName extends keyof TAbis,
    TEventName extends ExtractAbiEventNames<TAbis[TContractName]>
  >(args: {
    contract: TContractName;
    handlers: Partial<
      EventHandlers<TAbis[TContractName], TEventName, TContext, TAbis>
    >;
  }): IndexerBuilder<TAbis, TContext> {
    const { contract: contractName, handlers } = args;

    const newOptions = {
      ...this.options,
      contracts: {
        ...(this.options.contracts ?? {}),
        [contractName]: {
          ...(this.options.contracts?.[contractName] ?? {}),
          handlers: handlers,
        },
      },
    } as Options<TAbis, TContext>;

    return new IndexerBuilder(newOptions);
  }

  addEventHandler<
    TContractName extends keyof TAbis,
    TEventName extends ExtractAbiEventNames<TAbis[TContractName]>
  >(args: {
    contract: TContractName;
    event: TEventName | ExtractAbiEventNames<TAbis[TContractName]>;
    handler: EventHandler<TAbis[TContractName], TEventName, TContext, TAbis>;
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
            ...(this.options.contracts?.[contractName]?.handlers ?? {}),
            [eventName]: handler,
          },
        },
      },
    } as Options<TAbis, TContext>;

    return new IndexerBuilder(newOptions);
  }

  context<TNewContext>(
    context: TNewContext
  ): IndexerBuilder<TAbis, TNewContext> {
    const newOptions = {
      ...this.options,
      context,
    } as Options<TAbis, TNewContext>;

    return new IndexerBuilder<TAbis, TNewContext>(newOptions);
  }

  rpc(rpc: Options<TAbis>["rpc"]): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, rpc });
  }

  logger(logger: Options<TAbis>["logger"]): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, logger });
  }

  logLevel(
    logLevel: Options<TAbis>["logLevel"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, logLevel });
  }

  eventPollIntervalMs(
    eventPollIntervalMs: Options<TAbis>["eventPollIntervalMs"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, eventPollIntervalMs });
  }

  onEvent(
    onEvent: Options<TAbis, TContext>["onEvent"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, onEvent });
  }

  onProgress(
    onProgress: Options<TAbis>["onProgress"]
  ): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, onProgress });
  }

  cache(cache: Options<TAbis>["cache"]): IndexerBuilder<TAbis, TContext> {
    return new IndexerBuilder({ ...this.options, cache: cache });
  }

  subscriptionStore(
    subscriptionStore: Options<TAbis>["subscriptionStore"]
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
    } as Options<TAbis, TContext>;

    return new IndexerBuilder(newOptions);
  }

  build(): Indexer<TAbis, TContext> {
    if (!this.options.rpc) {
      throw new Error("Failed to build indexer: rpc is not set");
    }

    const options: Options<TAbis, TContext> = {
      ...this.options,
      rpc: this.options.rpc,
      contracts: this.options.contracts ?? ({} as ContractsFromAbis<TAbis>),
      context: this.options.context ?? ({} as TContext),
    };

    return createIndexer(options);
  }
}

export function buildIndexer() {
  return new IndexerBuilder({});
}

function filterActiveSubscriptions(args: {
  subscriptions: Subscription[];
  targetBlock: bigint;
}) {
  const { targetBlock, subscriptions } = args;

  return subscriptions.flatMap((sub) => {
    let fromBlock;

    if (sub.indexedToBlock > targetBlock) {
      return [];
    } else if (sub.fetchedToBlock > sub.indexedToBlock) {
      fromBlock = sub.fetchedToBlock + 1n;
    } else {
      fromBlock = sub.indexedToBlock + 1n;
    }

    let toBlock;

    if (sub.toBlock !== "latest" && sub.toBlock < targetBlock) {
      toBlock = sub.toBlock;
    } else {
      toBlock = targetBlock;
    }

    if (fromBlock > toBlock) {
      return [];
    }

    return [
      {
        from: fromBlock,
        to: toBlock,
        subscription: sub,
      },
    ];
  });
}

async function fetchSubscriptions(args: {
  targetBlock: bigint;
  subscriptions: Subscription[];
  rpc: RpcClient;
  pushEvent: (event: Event) => void;
  cache: Cache | null;
  logger: Logger;
}) {
  const { rpc, subscriptions, targetBlock, cache, logger, pushEvent } = args;

  const activeSubscriptions = filterActiveSubscriptions({
    subscriptions,
    targetBlock,
  });

  const subscriptionIndex = activeSubscriptions.reduce(
    (acc, { subscription }) => {
      acc[`${subscription.contractAddress}:${subscription.topic}`] =
        subscription;
      return acc;
    },
    {} as Record<string, Subscription>
  );

  const fetchRequests: Record<
    string,
    { from: bigint; to: bigint; subscriptions: Subscription[] }
  > = {};

  for (const { from, to, subscription } of activeSubscriptions) {
    let finalFetchFromBlock = from;

    if (cache) {
      // fetch events from the event store
      const result = await cache.getEvents({
        address: subscription.contractAddress,
        topic: subscription.topic,
        fromBlock: from,
        toBlock: to,
      });

      if (result !== null) {
        for (const event of result.events) {
          pushEvent(event);
        }

        finalFetchFromBlock = result.toBlock + 1n;

        if (finalFetchFromBlock >= to) {
          continue;
        }
      }
    }

    // group subscriptions by fromBlock and toBlock to reduce the number of
    // requests to the RPC endpoint
    const group = `${finalFetchFromBlock}:${to}:${subscription.contractAddress}`;

    fetchRequests[group] = fetchRequests[group] ?? {
      from: finalFetchFromBlock,
      to: to,
      subscriptions: [],
    };

    fetchRequests[group].subscriptions.push(subscription);
  }

  for (const { from, to, subscriptions } of Object.values(fetchRequests)) {
    let currentBlock = from;

    const address = subscriptions[0].contractAddress;
    const topics = subscriptions.map((s) => s.topic);

    let steps = 1n;

    while (currentBlock <= to) {
      try {
        const toBlock = currentBlock + (targetBlock - currentBlock) / steps;

        logger.trace(`Fetching events ${currentBlock}-${toBlock} (${address})`);

        const logs = await rpc.getLogs({
          address: address,
          fromBlock: currentBlock,
          toBlock: toBlock,
          topics: [topics],
        });

        const events = [];

        for (const log of logs) {
          const subscription =
            subscriptionIndex[`${log.address}:${log.topics[0]}`];

          if (subscription === undefined) {
            continue;
          }

          const parsedEvent = decodeEventLog({
            abi: subscription.abi,
            data: log.data,
            topics: log.topics,
          });

          if (
            log.transactionHash === null ||
            log.blockNumber === null ||
            log.logIndex === null
          ) {
            throw new Error("Event is still pending");
          }

          const blockNumber = fromHex(log.blockNumber, "bigint");

          const event: Event = {
            name: subscription.eventName,
            params: parsedEvent.args,
            address: log.address,
            topic: log.topics[0],
            transactionHash: log.transactionHash,
            blockNumber: blockNumber,
            logIndex: fromHex(log.logIndex, "number"),
          };

          events.push(event);
          pushEvent(event);
        }

        if (cache) {
          await cache.insertEvents({
            topics: topics,
            address: address,
            fromBlock: currentBlock,
            toBlock: toBlock,
            events,
          });
        }

        currentBlock = toBlock + 1n;

        // range successfully fetched, fetch wider range
        steps = steps / 2n;
        if (steps < 1n) {
          steps = 1n;
        }
      } catch (error) {
        if (error instanceof JsonRpcRangeTooWideError) {
          logger.warn("Range too wide, splitting in half and retrying");
          // range too wide, split in half and retry
          steps = steps * 2n;
          continue;
        }

        throw error;
      }
    }
  }

  return activeSubscriptions.map(({ subscription }) => subscription.id);
}

export interface Indexer<
  TAbis extends Record<string, Abi> = Record<string, Abi>,
  TContext = unknown
> {
  context: TContext;

  indexToBlock(toBlock: ToBlock): Promise<void>;
  watch(): Promise<void>;

  stop(): Promise<void>;

  subscribeToContract(options: CreateSubscriptionOptions<keyof TAbis>): void;

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
    blockNumber: bigint;
  }): Promise<TReturn extends readonly [infer inner] ? inner : TReturn>;
}

type InitialIndexerState = {
  type: "initial";
};

type StoppedIndexerState = {
  type: "stopped";
};

type RunningIndexerState = {
  type: "running";
  pollTimeout: NodeJS.Timeout;
  targetBlock: ToBlock;
  onError: (error: unknown) => void;
  onFinish: () => void;
};

type IndexerState =
  | RunningIndexerState
  | StoppedIndexerState
  | InitialIndexerState;

export function createIndexer<
  TContext = unknown,
  TAbis extends Record<string, Abi> = Record<string, Abi>
>(options: Options<TAbis, TContext>): Indexer<TAbis, TContext> {
  const eventPollIntervalMs = options.eventPollIntervalMs ?? 1000;
  const logLevel: LogLevel = LogLevel[options.logLevel ?? "warn"];

  if (logLevel === undefined) {
    throw new Error(`Invalid log level: ${options.logLevel}`);
  }

  const loggerBackend: LoggerBackend =
    options.logger ??
    ((level, ...data: unknown[]) => {
      console.log(`[${level}]`, ...data);
    });

  const logger = new Logger(logLevel, loggerBackend);
  const cache = options.cache ?? null;
  let rpc: RpcClient;

  if ("rpc" in options && "url" in options.rpc) {
    const fetch = options.rpc.fetch ?? globalThis.fetch;
    rpc = createRpcClient(logger, options.rpc.url, fetch);
  } else if ("rpc" in options && "getLastBlockNumber" in options.rpc) {
    rpc = {
      getLastBlockNumber: options.rpc.getLastBlockNumber,
      getLogs: options.rpc.getLogs,
      readContract: options.rpc.readContract,
    };
  } else {
    throw new Error("Invalid RPC options, please provide a URL or a client");
  }

  let state: IndexerState = {
    type: "initial",
  };

  const contracts = options.contracts;
  const subscriptions: Subscription[] = [];
  const eventQueue: Event[] = [];

  async function poll() {
    if (state.type !== "running") {
      return;
    }

    function schedule(delay = eventPollIntervalMs) {
      if (state.type === "running") {
        state.pollTimeout = setTimeout(poll, delay);
      }
    }

    try {
      let targetBlock: bigint;

      if (state.targetBlock === "latest") {
        targetBlock = await rpc.getLastBlockNumber();
      } else {
        targetBlock = state.targetBlock;
      }

      const totalSubscriptionCount = subscriptions.length;

      const fetchedSubscriptionIds = await fetchSubscriptions({
        targetBlock,
        subscriptions,
        rpc,
        cache: cache,
        pushEvent(event) {
          eventQueue.push(event);
        },
        logger,
      });

      for (const id of fetchedSubscriptionIds) {
        const subscription = subscriptions.find((sub) => sub.id === id);

        if (subscription === undefined) {
          throw new Error(`Could not find subscription with id ${id}`);
        }

        subscription.fetchedToBlock = targetBlock;
      }

      // sort by block number and log index ascending
      // TODO: priority queue
      eventQueue.sort((a, b) => {
        if (a.blockNumber < b.blockNumber) {
          return -1;
        }

        if (a.blockNumber > b.blockNumber) {
          return 1;
        }

        if (a.logIndex < b.logIndex) {
          return -1;
        }

        if (a.logIndex > b.logIndex) {
          return 1;
        }

        return 0;
      });

      // if (eventQueue.length > 0) {
      //   logger.trace(`Applying ${eventQueue.length} events`);
      // }

      let event = null;

      // global indexedToBlock is the minimum of all subscriptions
      let indexedToBlock: bigint = subscriptions.reduce((acc, sub) => {
        if (sub.indexedToBlock < acc) {
          return sub.indexedToBlock;
        }
        return acc;
      }, subscriptions[0].indexedToBlock);

      const subscriptionIndex = subscriptions.reduce((acc, sub) => {
        acc[`${sub.contractAddress}:${sub.topic}`] = sub;
        return acc;
      }, {} as Record<string, Subscription>);

      while ((event = eventQueue.shift())) {
        const subscription =
          subscriptionIndex[`${event.address}:${event.topic}`];

        // should not happen
        if (subscription === undefined) {
          throw new Error(`Subscription not found ${event.topic}`);
        }

        if (
          event.blockNumber === subscription.indexedToBlock &&
          event.logIndex < subscription.indexedToLogIndex
        ) {
          continue;
        }

        // should not happen
        if (subscription === undefined) {
          throw new Error("Subscription not found");
        }

        try {
          const eventHandlerArgs = {
            event,
            context: options.context,
            readContract,
            subscribeToContract,
          };

          if (subscription.eventHandler !== undefined) {
            await subscription.eventHandler(eventHandlerArgs);
          }

          if (options.onEvent !== undefined) {
            await options.onEvent(eventHandlerArgs);
          }
        } catch (err) {
          logger.error({ message: "Error applying event", err, event });
          throw err;
        }

        subscription.indexedToBlock = event.blockNumber;
        subscription.indexedToLogIndex = event.logIndex;

        // report progress when we start a new block
        if (indexedToBlock !== event.blockNumber) {
          if (options.onProgress) {
            options.onProgress({
              currentBlock: indexedToBlock,
              targetBlock: targetBlock,
              pendingEventsCount: eventQueue.length,
            });
          }
        }

        indexedToBlock = event.blockNumber;

        // new subscriptions were added while processing
        if (subscriptions.length > totalSubscriptionCount) {
          for (const id of fetchedSubscriptionIds) {
            const subscription = subscriptions.find((sub) => sub.id === id);

            if (subscription === undefined) {
              continue;
            }

            subscription.indexedToBlock = event.blockNumber;
            subscription.indexedToLogIndex = event.logIndex;
          }

          schedule(0);
          return;
        }
      }

      for (const id of fetchedSubscriptionIds) {
        const subscription = subscriptions.find((sub) => sub.id === id);

        if (subscription === undefined) {
          continue;
        }

        subscription.indexedToBlock = targetBlock;
        subscription.indexedToLogIndex = 0;
      }

      // report progress when we reach the target block
      if (options.onProgress) {
        options.onProgress({
          currentBlock: targetBlock,
          targetBlock: targetBlock,
          pendingEventsCount: eventQueue.length,
        });
      }

      logger.trace(`Indexed to block ${targetBlock}`);

      if (options.subscriptionStore) {
        for (const subscription of subscriptions) {
          const subscriptionItem = {
            id: subscription.id,
            contractName: subscription.contractName,
            contractAddress: subscription.contractAddress,
            fromBlock: subscription.fromBlock,
            indexedToBlock: subscription.indexedToBlock,
            indexedToLogIndex: subscription.indexedToLogIndex,
            toBlock: subscription.toBlock,
          };

          options.subscriptionStore.save(subscriptionItem);
        }
      }

      if (state.targetBlock !== "latest" && targetBlock === state.targetBlock) {
        logger.trace("Reached indexing target block");
        stop();
        return;
      }

      schedule();
    } catch (err) {
      state.onError(err);
      stop();
    }
  }

  function subscribeToContract(
    subscribeOptions: CreateSubscriptionOptions<keyof TAbis>
  ) {
    const { contract: contractName, address } = subscribeOptions;
    const contract = contracts[contractName];

    if (!contract) {
      throw new Error(`Contract ${String(contractName)} not found`);
    }

    logger.trace(
      `Subscribing to ${String(contractName)} ${
        subscribeOptions.address
      } from ${subscribeOptions.fromBlock ?? 0}`
    );

    if (contract.handlers === undefined) {
      return;
    }

    let eventName: keyof typeof contract.handlers;
    for (eventName in contract.handlers) {
      const eventHandler =
        contract.handlers[eventName as keyof typeof contract.handlers];

      const eventAbi = getAbiItem<Abi, string>({
        abi: contract.abi,
        name: eventName,
      });

      if (eventAbi.type !== "event") {
        throw new Error(
          `Expected ${eventName} in ${String(contractName)} to be an event`
        );
      }

      const topics = encodeEventTopics<Abi, string>({
        abi: contract.abi,
        eventName,
      });

      if (topics.length !== 1) {
        throw new Error(`Failed to encode event topics for ${eventName}`);
      }

      const topic = topics[0];

      const id = `${address.toLowerCase()}:${topic}`;

      const fromBlock = subscribeOptions.fromBlock ?? 0n;

      const subscription: Subscription = {
        id: id,
        abi: [eventAbi],
        contractName: String(contractName),
        contractAddress: address.toLowerCase() as Hex,
        eventName,
        eventHandler: eventHandler as unknown as EventHandler<Abi>,
        topic,
        eventAbi,
        fromBlock: fromBlock,
        toBlock: subscribeOptions.toBlock ?? "latest",
        indexedToBlock: subscribeOptions.indexedToBlock ?? fromBlock - 1n,
        fetchedToBlock: -1n,
        indexedToLogIndex: 0,
      };

      subscriptions.push(subscription);
    }
  }

  async function init() {
    if (options.subscriptionStore) {
      const storedSubscriptions = await options.subscriptionStore.all();

      for (const subscription of storedSubscriptions) {
        subscribeToContract({
          contract: subscription.contractName as keyof TAbis,
          id: subscription.id,
          address: subscription.contractAddress,
          indexedToBlock: subscription.indexedToBlock,
          fromBlock: subscription.fromBlock,
          fromLogIndex: subscription.indexedToLogIndex,
          toBlock: subscription.toBlock,
        });
      }

      logger.info(`Loaded ${subscriptions.length} subscriptions from store`);
    }

    // add initial subscriptions only if none were loaded from storage
    if (subscriptions.length === 0) {
      for (const contractName in contracts) {
        const contract = contracts[contractName];

        if (contract.subscriptions === undefined) {
          continue;
        }

        const sources = [];

        if (Array.isArray(contract.subscriptions)) {
          sources.push(...contract.subscriptions);
        } else {
          sources.push(contract.subscriptions);
        }

        for (const source of sources) {
          subscribeToContract({
            contract: contractName,
            address: source.address,
            fromBlock: source.fromBlock,
            toBlock: source.toBlock,
          });
        }
      }
    }
  }

  async function indexToBlock(target: ToBlock): Promise<void> {
    if (state.type === "initial") {
      await init();
    }

    if (state.type === "running") {
      throw new Error("Indexer is already running");
    }

    logger.debug(`Indexing to block ${target}`);

    return new Promise((resolve, reject) => {
      state = {
        type: "running",
        targetBlock: target,
        onFinish: resolve,
        onError: reject,
        pollTimeout: setTimeout(poll, 0),
      };
    });
  }

  async function stop() {
    if (state.type !== "running") {
      throw new Error("Indexer is not running");
    }

    logger.debug("Stopping indexer");

    clearTimeout(state.pollTimeout);
    state.onFinish();

    state = {
      type: "stopped",
    };
  }

  async function readContract<
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
    blockNumber: bigint;
  }): Promise<TReturn extends readonly [infer inner] ? inner : TReturn> {
    const contract = contracts[args.contract];

    if (contract === undefined) {
      throw new Error(`Contract ${String(args.contract)} not found`);
    }

    const data = encodeFunctionData({
      abi: contract.abi as Abi,
      functionName: args.functionName as string,
      args: args.args as unknown[],
    });

    let result: Hex;

    if (cache) {
      const cachedRead = await cache.getContractRead({
        address: args.address,
        blockNumber: args.blockNumber,
        functionName: args.functionName,
        data: data,
      });

      if (cachedRead !== null) {
        result = cachedRead;
      }
    }

    result = await rpc.readContract({
      functionName: args.functionName,
      data: data,
      address: args.address,
      blockNumber: args.blockNumber,
    });

    if (cache) {
      await cache.insertContractRead({
        address: args.address,
        blockNumber: args.blockNumber,
        functionName: args.functionName,
        data: data,
        result,
      });
    }

    return decodeFunctionResult({
      abi: contract.abi as Abi,
      functionName: args.functionName as string,
      data: result,
    }) as TReturn extends readonly [infer inner] ? inner : TReturn;
  }

  return {
    context: options.context,
    subscribeToContract,
    stop,

    readContract,

    async watch() {
      return await indexToBlock("latest");
    },

    async indexToBlock(target: ToBlock) {
      if (target === "latest") {
        const lastBlock = await rpc.getLastBlockNumber();
        return await indexToBlock(lastBlock);
      }

      return await indexToBlock(target);
    },
  };
}
