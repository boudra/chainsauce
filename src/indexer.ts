import { Abi, ExtractAbiEventNames, ExtractAbiFunctionNames } from "abitype";
import {
  decodeFunctionResult,
  encodeEventTopics,
  encodeFunctionData,
  getAbiItem,
  getAddress,
} from "viem";
import { createRpcClient, RpcClient } from "@/rpc";
import { Cache } from "@/cache";
import { Logger, LoggerBackend, LogLevel } from "@/logger";
import {
  Hex,
  ToBlock,
  EventHandler,
  Event,
  Contract,
  ReadContractParameters,
  ReadContractReturn,
  EventHandlerArgs,
} from "@/types";
import { SubscriptionStore } from "@/subscriptionStore";
import {
  Subscription,
  findLowestIndexedBlock,
  getSubscription,
  saveSubscriptionsToStore,
  updateSubscription,
  getSubscriptionEvents,
} from "@/subscriptions";

export type Contracts<TAbis extends Record<string, Abi>> = {
  [K in keyof TAbis]: Contract<TAbis[K]>;
};

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

export type Config<TAbis extends Record<string, Abi>, TContext = unknown> = {
  logLevel?: keyof typeof LogLevel;
  logger?: LoggerBackend;
  eventPollDelayMs?: number;
  context: TContext;
  contracts: Contracts<TAbis>;
  chain: {
    name: string;
    id: number;
    rpc: RpcClient | { url: string; fetch?: typeof globalThis.fetch };
  };
  onProgress?: (progress: {
    currentBlock: bigint;
    targetBlock: bigint;
    pendingEventsCount: number;
  }) => void;
  onEvent?: EventHandler<TAbis, TContext>;
  cache?: Cache;
  subscriptionStore?: SubscriptionStore;
};

export type CreateSubscriptionOptions<TName> = {
  contract: TName;
  address: string;
  indexedToBlock?: bigint;
  fromBlock?: bigint;
  fromLogIndex?: number;
  toBlock?: ToBlock;
  id?: string;
};

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
    TFunctionName extends ExtractAbiFunctionNames<
      TAbis[TContractName],
      "pure" | "view"
    >
  >(
    args: {
      contract: TContractName;
      functionName: TFunctionName;
    } & ReadContractParameters<TAbis, TContractName>
  ): Promise<ReadContractReturn<TAbis[TContractName], TFunctionName>>;
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
  TAbis extends Record<string, Abi> = Record<string, Abi>,
  TContext = unknown
>(config: Config<TAbis, TContext>): Indexer<TAbis, TContext> {
  const eventPollDelayMs = config.eventPollDelayMs ?? 4000;
  const logLevel: LogLevel = LogLevel[config.logLevel ?? "warn"];

  if (logLevel === undefined) {
    throw new Error(`Invalid log level: ${config.logLevel}`);
  }

  const loggerBackend: LoggerBackend =
    config.logger ??
    ((level, ...data: unknown[]) => {
      console.log(`[${level}]`, ...data);
    });

  const logger = new Logger(logLevel, loggerBackend);
  const cache = config.cache ?? null;
  const rpc = createRpcClientFromConfig(config.chain.rpc, logger);

  let state: IndexerState = {
    type: "initial",
  };

  const contracts = config.contracts;
  const subscriptions: Map<string, Subscription> = new Map();
  const eventQueue = createEventQueue();

  async function poll() {
    if (state.type !== "running") {
      return;
    }

    function scheduleNextPoll(delay = eventPollDelayMs) {
      if (state.type === "running") {
        state.pollTimeout = setTimeout(poll, delay);
      }
    }

    try {
      let targetBlock: bigint;

      //  latest is a moving target
      if (state.targetBlock === "latest") {
        targetBlock = await rpc.getLastBlockNumber();
      } else {
        targetBlock = state.targetBlock;
      }

      const totalSubscriptionCount = subscriptions.size;

      const fetchedSubscriptionIds = await getSubscriptionEvents({
        chainId: config.chain.id,
        targetBlock,
        subscriptions,
        rpc,
        cache: cache,
        pushEvent(event) {
          eventQueue.queue(event);
        },
        logger,
      });

      for (const id of fetchedSubscriptionIds) {
        updateSubscription(subscriptions, id, { fetchedToBlock: targetBlock });
      }

      let indexedToBlock = findLowestIndexedBlock(subscriptions) ?? -1n;

      for (const event of eventQueue.drain()) {
        const subscription = getSubscription(
          subscriptions,
          `${event.address}:${event.topic}`
        );

        if (
          event.blockNumber === subscription.indexedToBlock &&
          event.logIndex < subscription.indexedToLogIndex
        ) {
          continue;
        }

        try {
          const eventHandlerArgs: Parameters<EventHandler>[0] = {
            event,
            context: config.context,
            readContract: (args) => {
              return readContract({
                ...args,
                blockNumber: event.blockNumber,
              });
            },
            subscribeToContract: (opts) => {
              return subscribeToContract({
                ...opts,
                fromBlock: event.blockNumber,
              });
            },
            chainId: config.chain.id,
          };

          if (subscription.eventHandler !== null) {
            await subscription.eventHandler(eventHandlerArgs);
          }

          if (config.onEvent) {
            await (config.onEvent as EventHandler)(eventHandlerArgs);
          }
        } catch (err) {
          logger.error({ message: "Error applying event", err, event });
          throw err;
        }

        updateSubscription(subscriptions, subscription.id, {
          indexedToBlock: event.blockNumber,
          indexedToLogIndex: event.logIndex,
        });

        // report progress when we start a new block
        if (
          indexedToBlock < event.blockNumber &&
          indexedToBlock > -1n &&
          config.onProgress
        ) {
          config.onProgress({
            currentBlock: indexedToBlock,
            targetBlock: targetBlock,
            pendingEventsCount: eventQueue.size(),
          });
        }

        indexedToBlock = event.blockNumber;

        // new subscriptions were added while processing
        if (subscriptions.size > totalSubscriptionCount) {
          logger.trace("New subscriptions were added while processing events");

          for (const id of fetchedSubscriptionIds) {
            updateSubscription(subscriptions, id, {
              indexedToBlock: event.blockNumber,
              indexedToLogIndex: event.logIndex,
            });
          }

          if (config.subscriptionStore) {
            await saveSubscriptionsToStore(
              config.subscriptionStore,
              subscriptions
            );
          }

          scheduleNextPoll(0);
          return;
        }
      }

      for (const id of fetchedSubscriptionIds) {
        updateSubscription(subscriptions, id, {
          indexedToBlock: targetBlock,
          indexedToLogIndex: 0,
        });
      }

      // report progress when we reach the target block
      if (config.onProgress) {
        config.onProgress({
          currentBlock: targetBlock,
          targetBlock: targetBlock,
          pendingEventsCount: eventQueue.size(),
        });
      }

      logger.trace(`Indexed to block ${targetBlock}`);

      if (config.subscriptionStore) {
        await saveSubscriptionsToStore(config.subscriptionStore, subscriptions);
      }

      if (state.targetBlock !== "latest" && targetBlock === state.targetBlock) {
        logger.trace("Reached indexing target block");
        stop();
        return;
      }

      scheduleNextPoll();
    } catch (err) {
      state.onError(err);
      stop();
    }
  }

  function subscribeToContract(
    subscribeOptions: CreateSubscriptionOptions<keyof TAbis>
  ) {
    const { contract: contractName } = subscribeOptions;
    const address = getAddress(subscribeOptions.address);
    const contract = contracts[contractName];

    if (!contract) {
      throw new Error(`Contract ${String(contractName)} not found`);
    }

    logger.trace(
      `Subscribing to ${String(contractName)} ${
        subscribeOptions.address
      } from ${subscribeOptions.fromBlock ?? 0}`
    );

    if (contract.events === undefined) {
      return;
    }

    let eventHandlers: Record<string, EventHandler | null>;

    if (Array.isArray(contract.events)) {
      eventHandlers = contract.events.reduce((acc, eventName) => {
        acc[eventName as string] = null;
        return acc;
      }, {} as typeof eventHandlers);
    } else {
      eventHandlers = {};

      for (const name in contract.events) {
        eventHandlers[name.toString()] =
          (contract.events[name as keyof typeof contract.events] as
            | EventHandler
            | undefined) ?? null;
      }
    }

    for (const eventName in eventHandlers) {
      const eventHandler = eventHandlers[eventName];

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

      const id = `${address}:${topic}`;

      const fromBlock = subscribeOptions.fromBlock ?? 0n;

      const subscription: Subscription = {
        id: id,
        abi: [eventAbi],
        contractName: String(contractName),
        contractAddress: address,
        eventName,
        eventHandler: eventHandler,
        topic,
        eventAbi,
        fromBlock: fromBlock,
        toBlock: subscribeOptions.toBlock ?? "latest",
        indexedToBlock: subscribeOptions.indexedToBlock ?? fromBlock - 1n,
        fetchedToBlock: -1n,
        indexedToLogIndex: 0,
      };

      subscriptions.set(id, subscription);
    }
  }

  async function init() {
    if (config.subscriptionStore) {
      const storedSubscriptions = await config.subscriptionStore.all();

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

      logger.info(`Loaded ${subscriptions.size} subscriptions from store`);
    }

    // add initial subscriptions only if none were loaded from storage
    if (subscriptions.size === 0) {
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
    TFunctionName extends ExtractAbiFunctionNames<
      TAbis[TContractName],
      "pure" | "view"
    >
  >(
    args: {
      contract: TContractName;
      functionName: TFunctionName;
    } & ReadContractParameters<TAbis, TContractName>
  ): Promise<ReadContractReturn<TAbis[TContractName], TFunctionName>> {
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
        chainId: config.chain.id,
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
        chainId: config.chain.id,
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
    }) as ReadContractReturn<TAbis[TContractName], TFunctionName>;
  }

  return {
    context: config.context,
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

function createRpcClientFromConfig(
  rpc: RpcClient | { url: string; fetch?: typeof globalThis.fetch },
  logger: Logger
): RpcClient {
  if ("url" in rpc) {
    return createRpcClient({
      logger,
      url: rpc.url,
    });
  } else if ("getLastBlockNumber" in rpc) {
    return {
      getLastBlockNumber: rpc.getLastBlockNumber,
      getLogs: rpc.getLogs,
      readContract: rpc.readContract,
    };
  } else {
    throw new Error("Invalid RPC options, please provide a URL or a client");
  }
}

// TODO: priority queue
function createEventQueue() {
  const queue: Event[] = [];

  return {
    queue(event: Event) {
      queue.push(event);
    },
    size() {
      return queue.length;
    },
    *drain() {
      // sort by block number and log index ascending
      queue.sort((a, b) => {
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

      while (queue.length > 0) {
        const event = queue.shift();
        if (event) {
          yield event;
        }
      }
    },
  };
}
