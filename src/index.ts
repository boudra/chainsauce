import { Abi, AbiEvent, ExtractAbiEventNames } from "abitype";
import { decodeEventLog, encodeEventTopics, fromHex, getAbiItem } from "viem";
import { v4 as uuidv4 } from "uuid";

import {
  createRpcClient,
  RpcClient,
  JsonRpcRangeTooWideError,
  Log,
} from "@/rpc";
import { EventStore } from "@/eventStore";
import { SubscriptionStore } from "@/subscriptionStore";
import { Logger, LoggerBackend, LogLevel } from "@/logger";
import { Hex, ToBlock, EventHandlers, BaseEvent, Event } from "@/types";

export { Hex, ToBlock, Event, LoggerBackend, LogLevel, Log };

type Subscription = {
  id: string;
  abi: Abi;
  contractName: string;
  contractAddress: `0x${string}`;
  topic: `0x${string}`;
  eventName: string;
  eventHandler: (event: BaseEvent) => Promise<void>;
  eventAbi: AbiEvent;
  toBlock: ToBlock;
  fetchedToBlock: bigint;
  indexedToBlock: bigint;
  indexedToLogIndex: number;
};

export type Contract<T extends Abi, N extends ExtractAbiEventNames<T>> = {
  name: string;
  abi: T;
  address?: `0x${string}`;
  handlers: EventHandlers<T, N>;
  fromBlock?: bigint;
  toBlock?: ToBlock;
};

export type CreateSubscriptionOptions = {
  id?: string;
  name: string;
  address: Hex;
  fromBlock?: bigint;
  fromLogIndex?: number;
  toBlock?: ToBlock;
};

export function contract<
  T extends Abi,
  N extends ExtractAbiEventNames<T>
>(options: {
  name: string;
  abi: T;
  address?: `0x${string}`;
  fromBlock?: bigint;
  toBlock?: ToBlock;
  handlers: EventHandlers<T, N>;
}): Contract<T, N> {
  return options;
}

export type Options = {
  logLevel?: keyof typeof LogLevel;
  Logger?: LoggerBackend;
  eventPollIntervalMs?: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  contracts: Contract<any, any>[];
  rpc: RpcClient | { url: string; fetch?: typeof globalThis.fetch };
  onEvent?: (event: BaseEvent) => Promise<void>;
  onUpdate?: (block: bigint) => void;
  onProgress?: (progress: {
    currentBlock: bigint;
    lastBlock: bigint;
    pendingEventsCount: number;
  }) => void;
  eventStore?: EventStore;
  subscriptionStore?: SubscriptionStore;
};

export interface Indexer {
  subscribeToContract(options: CreateSubscriptionOptions): void;
  indexToBlock(toBlock: ToBlock): Promise<void>;
  start(): Promise<void>;
  stop(): Promise<void>;
}

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

type IndexerState = RunningIndexerState | StoppedIndexerState;

export async function createIndexer(options: Options): Promise<Indexer> {
  const eventPollIntervalMs = options.eventPollIntervalMs ?? 1000;
  const logLevel: LogLevel = LogLevel[options.logLevel ?? "warn"];

  if (logLevel === undefined) {
    throw new Error(`Invalid log level: ${options.logLevel}`);
  }

  const loggerBackend =
    options.Logger ??
    ((level, ...data: unknown[]) => {
      console.log(`[${LogLevel[level]}]`, ...data);
    });

  const logger = new Logger(logLevel, loggerBackend);
  const eventStore = options.eventStore ?? null;
  let getLastBlockNumber: RpcClient["getLastBlockNumber"];
  let getLogs: RpcClient["getLogs"];

  if ("rpc" in options && "url" in options.rpc) {
    const fetch = options.rpc.fetch ?? globalThis.fetch;
    const client = createRpcClient(logger, options.rpc.url, fetch);
    getLastBlockNumber = () => client.getLastBlockNumber();
    getLogs = (opts) => client.getLogs(opts);
  } else if ("rpc" in options && "getLastBlockNumber" in options.rpc) {
    getLastBlockNumber = options.rpc.getLastBlockNumber;
    getLogs = options.rpc.getLogs;
  } else {
    throw new Error("Invalid RPC options");
  }

  let state: IndexerState = {
    type: "stopped",
  };

  const contracts = options.contracts;
  const subscriptions: Subscription[] = [];
  const eventQueue: BaseEvent[] = [];

  if (options.subscriptionStore) {
    const storedSubscriptions = await options.subscriptionStore.all();

    for (const subscription of storedSubscriptions) {
      subscribeToContract({
        id: subscription.id,
        name: subscription.contractName,
        address: subscription.contractAddress,
        fromBlock: subscription.indexedToBlock,
        fromLogIndex: subscription.indexedToLogIndex,
        toBlock: subscription.toBlock,
      });
    }

    logger.info("Loaded", subscriptions.length, "subscriptions from store");
  }

  // add initial subscriptions only if none were loaded from storage
  if (subscriptions.length === 0) {
    for (const contract of contracts) {
      // contract is bound to an address, subscribe to it
      if (contract.address) {
        subscribeToContract({
          name: contract.name,
          address: contract.address,
          fromBlock: contract.fromBlock,
          toBlock: contract.toBlock,
        });
      }
    }
  }

  async function poll() {
    if (state.type !== "running") {
      return;
    }

    function schedule() {
      if (state.type === "running") {
        state.pollTimeout = setTimeout(poll, eventPollIntervalMs);
      }
    }

    try {
      let lastBlock = await getLastBlockNumber();
      const subscriptionCount = subscriptions.length;

      // do not index beyond the target block
      if (state.targetBlock !== "latest" && lastBlock > state.targetBlock) {
        lastBlock = state.targetBlock;
      }

      // only work with subscriptions that index to latest or beyond the last block,
      // and that have not yet indexed to the last block
      const activeSubscriptions = subscriptions.filter((sub) => {
        return (
          (sub.toBlock === "latest" || sub.toBlock >= lastBlock) &&
          sub.indexedToBlock < lastBlock
        );
      });

      // contract address => event topic => index
      const subscriptionMap = new Map<Hex, Map<Hex, number>>();

      for (let i = 0; i < activeSubscriptions.length; i++) {
        const subscription = activeSubscriptions[i];

        if (!subscriptionMap.has(subscription.contractAddress)) {
          subscriptionMap.set(subscription.contractAddress, new Map());
        }

        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        const eventMap = subscriptionMap.get(subscription.contractAddress)!;

        if (!eventMap.has(subscription.topic)) {
          eventMap.set(subscription.topic, i);
        }
      }

      // fromBlock => toBlock => contract address => topics
      const fetchRequests = new Map<bigint, Map<bigint, Map<Hex, Hex[]>>>();

      // group subscriptions by fromBlock and toBlock to reduce the number of
      // requests to the RPC endpoint
      for (const subscription of activeSubscriptions) {
        let toBlock = lastBlock;

        if (subscription.toBlock !== "latest") {
          toBlock = subscription.toBlock;
        }

        // continue fetching from the last fetched block
        let fetchFromBlock = subscription.fetchedToBlock + 1n;

        // fetch from the latest indexed block if we have indexed but not fetched
        if (fetchFromBlock < subscription.indexedToBlock) {
          fetchFromBlock = subscription.indexedToBlock + 1n;
        }

        if (eventStore) {
          // fetch events from the event store
          const storedEvents = await eventStore.getEvents({
            address: subscription.contractAddress,
            topic: subscription.topic,
            fromBlock: fetchFromBlock,
            toBlock: lastBlock,
          });

          for (const event of storedEvents) {
            eventQueue.push(event);
          }

          // if there are stored events, fetch from the block after the last
          // stored event
          if (storedEvents.length > 0) {
            fetchFromBlock =
              storedEvents[storedEvents.length - 1].blockNumber + 1n;
            subscription.fetchedToBlock = fetchFromBlock;
          }
        }

        const fromMap = fetchRequests.get(fetchFromBlock) ?? new Map();
        const toMap = fromMap.get(toBlock) ?? new Map();
        const topics = toMap.get(subscription.contractAddress) ?? [];

        topics.push(subscription.topic);
        toMap.set(subscription.contractAddress, topics);
        fromMap.set(toBlock, toMap);
        fetchRequests.set(fetchFromBlock, fromMap);
      }

      for (const [startBlock, innerMap] of fetchRequests) {
        for (const [toBlock, addressesTopics] of innerMap) {
          let currentBlock = startBlock;

          const addresses = Array.from(addressesTopics.keys());
          const topics = Array.from(addressesTopics.values()).flat();

          let steps = 1n;

          while (currentBlock <= toBlock) {
            try {
              const toBlock = currentBlock + (lastBlock - currentBlock) / steps;

              logger.trace("Fetching events", currentBlock, "to", toBlock);

              const logs = await getLogs({
                address: addresses,
                fromBlock: currentBlock,
                toBlock: toBlock,
                topics: [topics],
              });

              logger.trace("Fetched new", logs.length, "events");

              for (const log of logs) {
                const subscriptionIndex = subscriptionMap
                  .get(log.address)
                  ?.get(log.topics[0]);

                if (subscriptionIndex === undefined) {
                  continue;
                }

                const subscription = activeSubscriptions[subscriptionIndex];

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

                const event: BaseEvent = {
                  name: subscription.eventName,
                  params: parsedEvent.args,
                  address: log.address,
                  topic: log.topics[0],
                  transactionHash: log.transactionHash,
                  blockNumber: blockNumber,
                  logIndex: fromHex(log.logIndex, "number"),
                };

                if (eventStore) {
                  await eventStore.insert(event);
                }

                eventQueue.push(event);

                subscription.fetchedToBlock = blockNumber;
              }

              currentBlock = toBlock + 1n;

              // range successfully fetched, fetch wider range
              steps = steps / 2n;
              if (steps < 1n) {
                steps = 1n;
              }
            } catch (error) {
              if (error instanceof JsonRpcRangeTooWideError) {
                // range too wide, split in half and retry
                steps = steps * 2n;
                continue;
              }

              throw error;
            }
          }
        }
      }

      // sort by block number and log index ascending
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

      if (eventQueue.length > 0) {
        logger.trace("Applying", eventQueue.length, "events");
      }

      if (options.onProgress) {
        const currentBlock = activeSubscriptions.reduce((acc, sub) => {
          if (sub.indexedToBlock < acc) {
            return sub.indexedToBlock;
          }
          return acc;
        }, lastBlock);

        options.onProgress({
          currentBlock,
          lastBlock: lastBlock,
          pendingEventsCount: eventQueue.length,
        });
      }

      while (eventQueue.length > 0) {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        const event = eventQueue.shift()!;

        const index = subscriptionMap.get(event.address)?.get(event.topic);

        // should not happen
        if (index === undefined) {
          throw new Error("Subscription not found");
        }

        const subscription = activeSubscriptions[index];

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
          await subscription.eventHandler(event);

          if (options.onEvent) {
            await options.onEvent(event);
          }
        } catch (err) {
          logger.error("Error applying event", err);
        }

        subscription.indexedToBlock = event.blockNumber;
        subscription.indexedToLogIndex = event.logIndex;

        // new subscriptions were added while processing
        if (subscriptions.length > subscriptionCount) {
          schedule();
        }
      }

      logger.trace("Indexed to block", lastBlock);

      if (options.onUpdate) {
        options.onUpdate(lastBlock);
      }

      if (options.subscriptionStore) {
        for (const subscription of subscriptions) {
          const subscriptionItem = {
            id: subscription.id,
            contractName: subscription.contractName,
            contractAddress: subscription.contractAddress,
            indexedToBlock: subscription.indexedToBlock,
            indexedToLogIndex: subscription.indexedToLogIndex,
            toBlock: subscription.toBlock,
          };

          options.subscriptionStore.save(subscriptionItem);
        }
      }

      if (state.targetBlock !== "latest" && lastBlock === state.targetBlock) {
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

  function subscribeToContract(subscribeOptions: CreateSubscriptionOptions) {
    const contractName = subscribeOptions.name;
    const contract = contracts.find((c) => c.name === contractName);
    const id = subscribeOptions.id ?? uuidv4();

    if (!contract) {
      throw new Error(`Contract ${contractName} not found`);
    }

    logger.trace("Subscribing to", contractName, subscribeOptions.address);

    for (const eventName in contract.handlers) {
      const eventHandler = contract.handlers[eventName];

      const eventAbi = getAbiItem({
        abi: contract.abi,
        name: eventName,
      });

      if (eventAbi.type !== "event") {
        throw new Error(
          `Expected ${eventName} in ${contract.name} to be an event`
        );
      }

      const topics = encodeEventTopics({
        abi: contract.abi,
        eventName,
      });

      if (topics.length === 0) {
        throw new Error(`Failed to encode event topics for ${eventName}`);
      }

      const topic = topics[0];

      const subscription: Subscription = {
        id: id,
        abi: [eventAbi],
        contractName,
        contractAddress: subscribeOptions.address.toLowerCase() as Hex,
        eventName,
        eventHandler: eventHandler as (event: BaseEvent) => Promise<void>,
        topic,
        eventAbi,
        indexedToBlock: subscribeOptions.fromBlock ?? -1n,
        toBlock: subscribeOptions.toBlock ?? "latest",
        fetchedToBlock: -1n,
        indexedToLogIndex: 0,
      };

      subscriptions.push(subscription);
    }
  }

  function indexToBlock(target: ToBlock): Promise<void> {
    if (state.type !== "stopped") {
      throw new Error("Indexer is already running");
    }

    logger.debug("Indexing to block", target);

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

  return {
    subscribeToContract,
    stop,

    start: async () => {
      return await indexToBlock("latest");
    },

    indexToBlock: async (target: ToBlock) => {
      if (target === "latest") {
        const lastBlock = await getLastBlockNumber();
        return await indexToBlock(lastBlock);
      }

      return await indexToBlock(target);
    },
  };
}
