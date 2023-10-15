import { Abi, ExtractAbiEventNames, ExtractAbiFunctionNames } from "abitype";
import { decodeFunctionResult, encodeFunctionData, getAddress } from "viem";
import { createConcurrentRpcClient, createRpcClient, RpcClient } from "@/rpc";
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
import { ListenerSignature, TypedEmitter } from "tiny-typed-emitter";

class AwaitableEventEmitter<
  L extends ListenerSignature<L>
> extends TypedEmitter<L> {
  async emitAsync<U extends keyof L>(
    event: keyof L,
    ...args: Parameters<L[U]>
  ) {
    const listeners = this.listeners(event);
    const promises = listeners.map((listener) => listener(...args));
    await Promise.all(promises);
  }
}

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
  context?: TContext;
  contracts: TAbis;
  chain: {
    name: string;
    id: number;
    concurrency?: number;
    rpc: RpcClient | { url: string; fetch?: typeof globalThis.fetch };
  };
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

export type IndexerContractEvents<
  TAbis extends Record<string, Abi>,
  TContext
> = UnionToIntersection<
  {
    [K in keyof TAbis]: {
      [N in ExtractAbiEventNames<TAbis[K]> as `${K & string}:${N &
        string}`]: EventHandler<TAbis, TContext, TAbis[K], N>;
    };
  }[keyof TAbis]
>;

type UnionToIntersection<U> = (
  U extends unknown ? (k: U) => void : never
) extends (k: infer I) => void
  ? I
  : never;

type IndexerEvents<TAbis extends Record<string, Abi>, TContext> = {
  stopped: () => void;
  started: () => void;
  error: (err: unknown) => void;
  progress: (args: {
    currentBlock: bigint;
    targetBlock: bigint;
    pendingEventsCount: number;
  }) => void;
  event: EventHandler<TAbis, TContext>;
};

export interface Indexer<
  TAbis extends Record<string, Abi> = Record<string, Abi>,
  TContext = unknown
> extends AwaitableEventEmitter<
    IndexerEvents<TAbis, TContext> & IndexerContractEvents<TAbis, TContext>
  > {
  context?: TContext;

  indexToBlock(toBlock: ToBlock): Promise<void>;
  watch(): void;

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
  onStop: () => void;
};

type IndexerState =
  | RunningIndexerState
  | StoppedIndexerState
  | InitialIndexerState;

export function createIndexer<
  TAbis extends Record<string, Abi> = Record<string, Abi>,
  TContext = unknown
>(config: Config<TAbis, TContext>): Indexer<TAbis, TContext> {
  const eventEmitter = new AwaitableEventEmitter<
    IndexerEvents<TAbis, TContext>
  >();
  const eventPollDelayMs = config.eventPollDelayMs ?? 1000;
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
  const rpc = createRpcClientFromConfig({
    rpc: config.chain.rpc,
    concurrency: config.chain.concurrency,
    logger,
  });

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

      await getSubscriptionEvents({
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

      const currentSubscriptionIds = Array.from(subscriptions.values()).map(
        (s) => s.id
      );

      for (const id of currentSubscriptionIds) {
        updateSubscription(subscriptions, id, { fetchedToBlock: targetBlock });
      }

      let indexedToBlock = findLowestIndexedBlock(subscriptions) ?? -1n;

      for (const event of eventQueue.drain()) {
        const subscription = getSubscription(subscriptions, event.address);

        if (
          event.blockNumber === subscription.indexedToBlock &&
          event.logIndex < subscription.indexedToLogIndex
        ) {
          continue;
        }

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

        await Promise.all([
          eventEmitter.emitAsync(
            `${subscription.contractName}:${event.name}` as "event",
            eventHandlerArgs
          ),
          eventEmitter.emitAsync("event", eventHandlerArgs),
        ]);

        updateSubscription(subscriptions, subscription.id, {
          indexedToBlock: event.blockNumber,
          indexedToLogIndex: event.logIndex,
        });

        // report progress when we start a new block
        if (indexedToBlock < event.blockNumber && indexedToBlock > -1n) {
          eventEmitter.emit("progress", {
            currentBlock: indexedToBlock,
            targetBlock: targetBlock,
            pendingEventsCount: eventQueue.size(),
          });
        }

        indexedToBlock = event.blockNumber;

        // new subscriptions were added while processing
        if (subscriptions.size > currentSubscriptionIds.length) {
          for (const id of currentSubscriptionIds) {
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

      for (const id of currentSubscriptionIds) {
        updateSubscription(subscriptions, id, {
          indexedToBlock: targetBlock,
          indexedToLogIndex: 0,
        });
      }

      // report progress when we reach the target block
      eventEmitter.emit("progress", {
        currentBlock: indexedToBlock,
        targetBlock: targetBlock,
        pendingEventsCount: eventQueue.size(),
      });

      logger.trace(`Indexed to block ${targetBlock}`);

      if (config.subscriptionStore) {
        await saveSubscriptionsToStore(config.subscriptionStore, subscriptions);
      }

      if (state.targetBlock !== "latest" && targetBlock === state.targetBlock) {
        logger.trace("Reached indexing target block");
        stop();
        return;
      }
    } catch (err) {
      state.onError(err);
    }

    scheduleNextPoll();
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

    const id = address;

    const fromBlock = subscribeOptions.fromBlock ?? 0n;

    const subscription: Subscription = {
      id: id,
      abi: contract,
      contractName: String(contractName),
      contractAddress: address,
      fromBlock: fromBlock,
      toBlock: subscribeOptions.toBlock ?? "latest",
      indexedToBlock: subscribeOptions.indexedToBlock ?? fromBlock - 1n,
      fetchedToBlock: -1n,
      indexedToLogIndex: 0,
    };

    subscriptions.set(id, subscription);
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
  }

  async function stop() {
    if (state.type !== "running") {
      throw new Error("Indexer is not running");
    }

    logger.trace("Stopping indexer");

    clearTimeout(state.pollTimeout);
    eventEmitter.emit("stopped");
    state.onStop?.();

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
      abi: contract as Abi,
      functionName: args.functionName as string,
      args: args.args as unknown[],
    });

    let result: Hex | undefined;

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

    if (result === undefined) {
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
    }

    return decodeFunctionResult({
      abi: contract as Abi,
      functionName: args.functionName as string,
      data: result,
    }) as ReadContractReturn<TAbis[TContractName], TFunctionName>;
  }

  return Object.setPrototypeOf(
    {
      context: config.context,
      subscribeToContract,
      stop,

      readContract,

      watch() {
        const initPromise =
          state.type === "initial" ? init() : Promise.resolve();

        initPromise
          .then(() => {
            if (state.type === "running") {
              throw new Error("Indexer is already running");
            }

            logger.trace(`Watching chain for events`);

            state = {
              type: "running",
              targetBlock: "latest",
              // eslint-disable-next-line @typescript-eslint/no-empty-function
              onStop: () => {},
              onError: (error) => {
                eventEmitter.emit("error", error);
              },
              pollTimeout: setTimeout(poll, 0),
            };

            eventEmitter.emit("started");
          })
          .catch((error) => {
            eventEmitter.emit("error", error);
          });
      },

      async indexToBlock(target: ToBlock): Promise<void> {
        if (state.type === "initial") {
          await init();
        }

        if (state.type === "running") {
          throw new Error("Indexer is already running");
        }

        let targetBlock: bigint;

        if (target === "latest") {
          targetBlock = await rpc.getLastBlockNumber();
        } else {
          targetBlock = target;
        }

        logger.trace(`Indexing to block ${targetBlock}`);

        return new Promise((resolve, reject) => {
          state = {
            type: "running",
            targetBlock: targetBlock,
            onStop: () => {
              resolve();
            },
            onError: (error) => {
              eventEmitter.emit("error", error);
              stop();
              reject();
            },
            pollTimeout: setTimeout(poll, 0),
          };
          eventEmitter.emit("started");
        });
      },
    },
    eventEmitter
  );
}

function createRpcClientFromConfig(args: {
  concurrency?: number;
  rpc: RpcClient | { url: string; fetch?: typeof globalThis.fetch };
  logger: Logger;
}): RpcClient {
  const { rpc, logger } = args;

  let client: RpcClient;

  if ("url" in rpc) {
    client = createRpcClient({
      logger,
      url: rpc.url,
    });
  } else if ("getLastBlockNumber" in rpc) {
    client = {
      getLastBlockNumber: rpc.getLastBlockNumber,
      getLogs: rpc.getLogs,
      readContract: rpc.readContract,
    };
  } else {
    throw new Error("Invalid RPC options, please provide a URL or a client");
  }

  return createConcurrentRpcClient({
    client,
    concurrency: args.concurrency,
  });
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
