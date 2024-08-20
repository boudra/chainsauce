import { Abi, ExtractAbiEventNames, ExtractAbiFunctionNames } from "abitype";
import {
  Address,
  decodeFunctionResult,
  encodeFunctionData,
  getAddress,
} from "viem";

import { RpcClient } from "@/rpc";
import { Cache } from "@/cache";
import { Logger, LoggerBackend, LogLevel } from "@/logger";
import { EventQueue } from "@/eventQueue";
import {
  Hex,
  ToBlock,
  EventHandler,
  ReadContractParameters,
  ReadContractReturn,
  UnionToIntersection,
} from "@/types";
import { SubscriptionStore } from "@/subscriptionStore";
import {
  Subscription,
  saveSubscriptionsToStore,
  updateSubscription,
  getSubscriptionEvents,
  findLowestIndexedBlock,
} from "@/subscriptions";
import { AsyncEventEmitter } from "@/asyncEventEmitter";
import { processEvents } from "@/eventProcessor";

const DEFAULT_MAX_BLOCK_RANGE = 1_000_000n;

export type Config<TAbis extends Record<string, Abi>, TContext = unknown> = {
  contracts: TAbis;
  chain: {
    id: number;
    rpcClient: RpcClient;
    pollingIntervalMs?: number;
    maxBlockRange?: bigint;
    lagBlocks?: bigint; // New field for lag indexing strategy
  };
  context?: TContext;
  logLevel?: keyof typeof LogLevel;
  logger?: LoggerBackend;
  cache?: Cache | null;
  subscriptionStore?: SubscriptionStore;
};

export type IndexerEvents<
  TAbis extends Record<string, Abi> = Record<string, Abi>,
  TContext = unknown
> = {
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
> extends AsyncEventEmitter<
    IndexerEvents<TAbis, TContext> & IndexerContractEvents<TAbis, TContext>
  > {
  context?: TContext;

  indexToBlock(toBlock: ToBlock): Promise<void>;
  watch(): void;

  stop(): Promise<void>;

  subscribeToContract(options: {
    contract: keyof TAbis;
    address: Address;
    indexedToBlock?: bigint;
    fetchedToBlock?: bigint;
    fromBlock?: bigint;
    fromLogIndex?: number;
    toBlock?: ToBlock;
    id?: string;
    updatedAt?: Date;
    createdAt?: Date;
  }): void;

  unsubscribeFromContract(options: { address: Address }): void;
  getSubscriptions(): Subscription[];

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
  finalTargetBlock: ToBlock;
  checkpointTargetBlock: bigint | null;
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
  const eventEmitter = new AsyncEventEmitter<IndexerEvents<TAbis, TContext>>();
  const eventPollDelayMs = config.chain.pollingIntervalMs ?? 1000;
  const logLevel: LogLevel = LogLevel[config.logLevel ?? "warn"];

  if (logLevel === undefined) {
    throw new Error(`Invalid log level: ${config.logLevel}`);
  }

  const loggerBackend: LoggerBackend =
    config.logger ??
    ((level, msg, data) => {
      console.log(`[${level}]`, msg, JSON.stringify(data));
    });

  const logger = new Logger(logLevel, loggerBackend);
  const cache = config.cache ?? null;
  const rpcClient = config.chain.rpcClient;

  let state: IndexerState = {
    type: "initial",
  };

  const contracts = config.contracts;
  const subscriptions: Map<string, Subscription> = new Map();
  const eventQueue = new EventQueue();

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
      const maxBlockRange =
        config.chain.maxBlockRange ?? DEFAULT_MAX_BLOCK_RANGE;

      let finalTargetBlock: bigint;

      //  latest is a moving target
      if (state.finalTargetBlock === "latest") {
        finalTargetBlock = await rpcClient.getLastBlockNumber();
        
        // Apply lag if specified
        if (config.chain.lagBlocks) {
          finalTargetBlock = applyLag(finalTargetBlock);
        }
      } else {
        finalTargetBlock = state.finalTargetBlock;
      }

      const lowestIndexedBlock = findLowestIndexedBlock(subscriptions);

      if (lowestIndexedBlock === null) {
        scheduleNextPoll();
        return;
      }

      let targetBlock = state.checkpointTargetBlock;

      // we indexed past the checkpoint target block
      if (targetBlock !== null && lowestIndexedBlock >= targetBlock) {
        targetBlock = targetBlock + maxBlockRange;
      } else if (targetBlock === null) {
        targetBlock = lowestIndexedBlock + maxBlockRange;
      }

      // cap the target block to the final target block
      if (targetBlock > finalTargetBlock) {
        targetBlock = finalTargetBlock;
      }

      // set the checkpoint target block to the target block
      state.checkpointTargetBlock = targetBlock;

      await getSubscriptionEvents({
        chainId: config.chain.id,
        targetBlock,
        subscriptions,
        rpc: rpcClient,
        cache: cache,
        pushEvent(event) {
          eventQueue.enqueue(event);
        },
        logger,
      });

      const subscriptionIds = Array.from(subscriptions.keys());

      for (const id of subscriptionIds) {
        updateSubscription(subscriptions, id, { fetchedToBlock: targetBlock });
      }

      const { indexedToBlock, indexedToLogIndex, hasNewSubscriptions } =
        await processEvents({
          chainId: config.chain.id,
          eventQueue,
          targetBlock,
          finalTargetBlock,
          subscriptions,
          contracts,
          logger,
          eventEmitter,
          subscriptionStore: config.subscriptionStore,
          context: config.context,
          readContract: readContract,
          subscribeToContract: subscribeToContract,
          unsubscribeFromContract,
          cache,
          rpcClient,
        });

      for (const id of subscriptionIds) {
        updateSubscription(subscriptions, id, {
          indexedToBlock,
          indexedToLogIndex,
        });
      }

      if (hasNewSubscriptions) {
        if (config.subscriptionStore) {
          await saveSubscriptionsToStore(
            config.subscriptionStore,
            subscriptions
          );
        }
        scheduleNextPoll(0);
        return;
      }

      for (const id of subscriptionIds) {
        updateSubscription(subscriptions, id, {
          indexedToBlock: targetBlock,
          indexedToLogIndex: 0,
        });
      }

      // report progress when we reach the target block
      eventEmitter.emit("progress", {
        currentBlock: indexedToBlock,
        targetBlock: finalTargetBlock,
        pendingEventsCount: eventQueue.size(),
      });

      if (config.subscriptionStore) {
        await saveSubscriptionsToStore(config.subscriptionStore, subscriptions);
      }

      // stop the indexer when we reach the final target block
      if (
        state.finalTargetBlock !== "latest" &&
        targetBlock === state.finalTargetBlock
      ) {
        logger.trace("Reached indexing target block");
        stop();
        return;
      }

      // we are not at the target block yet, schedule the next poll immediately
      if (targetBlock < finalTargetBlock) {
        scheduleNextPoll(0);
        return;
      }
    } catch (err) {
      state.onError(err);
    }

    scheduleNextPoll();
  }

  const subscribeToContract: Indexer<TAbis, TContext>["subscribeToContract"] = (
    subscribeOptions
  ) => {
    const { contract: contractName } = subscribeOptions;
    const address = getAddress(subscribeOptions.address);
    const contract = contracts[contractName];

    if (!contract) {
      throw new Error(`Contract ${String(contractName)} not found`);
    }

    const fromBlock = subscribeOptions.fromBlock ?? 0n;

    logger.trace(
      `Subscribing to ${String(contractName)} ${
        subscribeOptions.address
      } from ${fromBlock}`
    );

    const id = `${config.chain.id}-${address}`;

    // TODO: change -1n to null
    const subscription: Subscription = {
      id: id,
      chainId: config.chain.id,
      abi: contract,
      contractName: String(contractName),
      contractAddress: address,
      fromBlock: fromBlock,
      toBlock: subscribeOptions.toBlock ?? "latest",
      indexedToBlock: subscribeOptions.indexedToBlock ?? -1n,
      fetchedToBlock: -1n,
      indexedToLogIndex: 0,
      createdAt: subscribeOptions.createdAt ?? new Date(),
      updatedAt: subscribeOptions.updatedAt ?? new Date(),
    };

    subscriptions.set(id, subscription);
  };

  async function init() {
    if (config.subscriptionStore) {
      await config.subscriptionStore.init();

      const storedSubscriptions = await config.subscriptionStore.all(
        config.chain.id
      );

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

      if (storedSubscriptions.length > 0) {
        const lowestIndexedBlock = findLowestIndexedBlock(subscriptions);

        logger.info(
          `Resuming indexing from block ${lowestIndexedBlock} for ${storedSubscriptions.length} subscriptions`
        );
      }
    }

    if (config.cache) {
      await config.cache.migrate();
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
      result = await rpcClient.readContract({
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

  function unsubscribeFromContract({ address }: { address: Address }): void {
    const id = `${config.chain.id}-${address}`;
    subscriptions.delete(id);
    if (config.subscriptionStore) {
      config.subscriptionStore.delete(id);
    }
  }

  // Helper function to apply lag
  function applyLag(blockNumber: bigint): bigint {
    if (config.chain.lagBlocks && blockNumber > BigInt(config.chain.lagBlocks)) {
      return blockNumber - BigInt(config.chain.lagBlocks);
    }
    return 0n;
  }

  return Object.setPrototypeOf(
    {
      context: config.context,
      subscribeToContract,
      stop,

      readContract,

      getSubscriptions(): Subscription[] {
        return Array.from(subscriptions.values()).map((s) => ({
          ...s,
        }));
      },

      unsubscribeFromContract,

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
              finalTargetBlock: "latest",
              checkpointTargetBlock: null,
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
          targetBlock = await rpcClient.getLastBlockNumber();
          
          // Apply lag if specified
          if (config.chain.lagBlocks) {
            targetBlock = applyLag(targetBlock);
          }
        } else {
          targetBlock = target;
        }

        logger.trace(`Indexing to block ${targetBlock}`);

        return new Promise((resolve, reject) => {
          state = {
            type: "running",
            finalTargetBlock: targetBlock,
            checkpointTargetBlock: null,
            onStop: () => {
              resolve();
            },
            onError: (error) => {
              reject(error);
              stop();
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

// helper that returns a type that looks like this:
// {
//  "ContractName:EventName": EventHandler,
//  "ContractName2:EventName2": EventHandler2,
//  ..
// }
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
