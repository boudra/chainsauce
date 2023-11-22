import { decodeEventLog, fromHex, getAddress } from "viem";

import { Abi, Address } from "abitype";
import { Event, Hex, ToBlock } from "@/types";
import { Logger } from "@/logger";
import { JsonRpcRangeTooWideError, Log, RpcClient } from "@/rpc";
import { SubscriptionStore } from "@/subscriptionStore";
import { Cache } from "@/cache";

const MAX_CONTRACT_ADDRESSES_PER_GET_LOGS_REQUEST = 25;

export type Subscription = {
  id: string;
  chainId: number;
  abi: Abi;
  contractName: string;
  contractAddress: `0x${string}`;
  toBlock: ToBlock;
  fromBlock: bigint;
  fetchedToBlock: bigint;
  indexedToBlock: bigint;
  indexedToLogIndex: number;
};

export type Subscriptions = Map<string, Subscription>;

export function getSubscription(subscriptions: Subscriptions, id: string) {
  const subscription = subscriptions.get(id);
  if (subscription === undefined) {
    throw new Error(`Subscription ${id} not found`);
  }

  return subscription;
}

export function updateSubscription(
  subscriptions: Subscriptions,
  id: string,
  update: Partial<Subscription>
) {
  const subscription = subscriptions.get(id);
  if (subscription === undefined) {
    throw new Error(`Subscription ${id} not found`);
  }

  subscriptions.set(id, {
    ...subscription,
    ...update,
  });
}

export function findLowestIndexedBlock(subscriptions: Subscriptions) {
  let min = null;

  for (const sub of subscriptions.values()) {
    if (min === null) {
      min = sub.indexedToBlock;
    } else if (sub.indexedToBlock < min) {
      min = sub.indexedToBlock;
    }
  }

  return min;
}

export async function saveSubscriptionsToStore(
  store: SubscriptionStore,
  subscriptions: Subscriptions
): Promise<void> {
  for (const subscription of subscriptions.values()) {
    const subscriptionItem = {
      id: subscription.id,
      chainId: subscription.chainId,
      contractName: subscription.contractName,
      contractAddress: subscription.contractAddress,
      fromBlock: subscription.fromBlock,
      indexedToBlock: subscription.indexedToBlock,
      indexedToLogIndex: subscription.indexedToLogIndex,
      toBlock: subscription.toBlock,
    };

    // TODO: saveMany?
    await store.save(subscriptionItem);
  }
}

function getOutdatedSubscriptions(args: {
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

async function fetchLogsWithRetry(args: {
  rpc: RpcClient;
  address: Hex[];
  fromBlock: bigint;
  toBlock: bigint;
  topics: [Hex[]] | [];
  logger: Logger;
  onLogs: (args: { from: bigint; to: bigint; logs: Log[] }) => Promise<void>;
}) {
  const { onLogs, rpc, address, fromBlock, toBlock, topics, logger } = args;

  let cursor = fromBlock;

  let steps = 1n;

  while (cursor <= toBlock) {
    const pageToBlock = cursor + (toBlock - cursor) / steps;

    try {
      const logs = await rpc.getLogs({
        address: address,
        fromBlock: cursor,
        toBlock: pageToBlock,
        topics: topics,
      });

      await onLogs({ logs, from: cursor, to: pageToBlock });

      cursor = pageToBlock + 1n;

      // range successfully fetched, fetch wider range
      steps = steps / 2n;
      if (steps < 1n) {
        steps = 1n;
      }
    } catch (error) {
      // range too wide or too many logs returned, split in half and retry
      if (error instanceof JsonRpcRangeTooWideError) {
        logger.trace(
          `Range too wide ${cursor}-${pageToBlock}, retrying with smaller range`
        );
        steps = steps * 2n;
        continue;
      }

      throw error;
    }
  }
}

type OutdatedSubscription = {
  from: bigint;
  to: bigint;
  subscription: Subscription;
};

function createFetchPlan(subscriptions: OutdatedSubscription[]) {
  const ranges = new Map<
    string,
    { from: bigint; to: bigint; subscriptions: Subscription[] }
  >();

  for (const { from, to, subscription } of subscriptions) {
    const key = `${from}-${to}`;
    const existing = ranges.get(key);

    if (existing === undefined) {
      ranges.set(key, { from, to, subscriptions: [subscription] });
    } else {
      existing.subscriptions.push(subscription);
    }
  }

  const chunkedFetchRanges = new Map<
    string,
    {
      from: bigint;
      to: bigint;
      subscriptions: Subscription[][];
    }
  >();

  for (const [key, { from, to, subscriptions }] of ranges) {
    const chunkedSubscriptions = chunk(
      subscriptions,
      MAX_CONTRACT_ADDRESSES_PER_GET_LOGS_REQUEST
    );

    chunkedFetchRanges.set(key, {
      from,
      to,
      subscriptions: chunkedSubscriptions,
    });
  }

  return chunkedFetchRanges;
}

export async function getSubscriptionEvents(args: {
  targetBlock: bigint;
  chainId: number;
  subscriptions: Subscriptions;
  rpc: RpcClient;
  pushEvent: (event: Event) => void;
  cache: Cache | null;
  logger: Logger;
}) {
  const { chainId, rpc, subscriptions, targetBlock, cache, logger, pushEvent } =
    args;

  let outdatedSubscriptions = getOutdatedSubscriptions({
    subscriptions: Array.from(subscriptions.values()),
    targetBlock,
  });

  const fetchPromises = [];

  if (cache) {
    outdatedSubscriptions = await Promise.all(
      outdatedSubscriptions.map(async ({ from, to, subscription }) => {
        // fetch events from the event store
        const result = await cache.getEvents({
          chainId,
          address: subscription.contractAddress,
          fromBlock: from,
          toBlock: to,
        });

        if (result !== null) {
          for (const event of result.events) {
            pushEvent(event);
          }

          const cachedToBlock = result.toBlock + 1n;

          // we got all events from the cache
          if (cachedToBlock >= to) {
            return [];
          }

          // fetch the remaining events
          return [
            {
              from: cachedToBlock,
              to: to,
              subscription: subscription,
            },
          ];
        }

        // no cache at all, fetch all events
        return [{ from: from, to: to, subscription }];
      })
    ).then((results) => results.flat());
  }

  const fetchPlan = createFetchPlan(outdatedSubscriptions);

  for (const {
    from,
    to,
    subscriptions: subscriptionChunk,
  } of fetchPlan.values()) {
    for (const subscriptionsToFetch of subscriptionChunk) {
      const addresses = subscriptionsToFetch.map((sub) => sub.contractAddress);

      const promise = fetchLogsWithRetry({
        rpc,
        address: addresses,
        fromBlock: from,
        toBlock: to,
        topics: [],
        logger,
        onLogs: async ({ from: chunkFromBlock, to: chunkToBlock, logs }) => {
          const eventsPerContract = new Map<Address, Event[]>();

          for (const log of logs) {
            const logAddress = getAddress(log.address);
            const subscription = getSubscription(
              subscriptions,
              `${chainId}-${logAddress}`
            );

            if (subscription === undefined) {
              continue;
            }

            let parsedEvent;

            try {
              parsedEvent = decodeEventLog({
                abi: subscription.abi,
                data: log.data,
                topics: log.topics,
              });
            } catch (error) {
              // event probably not in the ABI
              // TODO: only try decoding if the event is in the ABI
              logger.debug(
                `Failed to decode event log ${logAddress} ${log.topics[0]}`
              );
              continue;
            }

            if (
              log.transactionHash === null ||
              log.blockNumber === null ||
              log.logIndex === null
            ) {
              throw new Error("Event is still pending");
            }

            const blockNumber = fromHex(log.blockNumber, "bigint");

            const event: Event = {
              name: parsedEvent.eventName,
              params: parsedEvent.args,
              address: logAddress,
              topic: log.topics[0],
              transactionHash: log.transactionHash,
              blockNumber: blockNumber,
              logIndex: fromHex(log.logIndex, "number"),
            };

            const events = eventsPerContract.get(subscription.contractAddress);

            if (events === undefined) {
              eventsPerContract.set(subscription.contractAddress, [event]);
            } else {
              events.push(event);
            }
            pushEvent(event);
          }

          if (cache) {
            for (const [address, events] of eventsPerContract.entries()) {
              await cache.insertEvents({
                chainId,
                address,
                fromBlock: chunkFromBlock,
                toBlock: chunkToBlock,
                events,
              });
            }
          }
        },
      });

      fetchPromises.push(promise);
    }
  }

  await Promise.all(fetchPromises);

  return outdatedSubscriptions.map(({ subscription }) => subscription.id);
}

function chunk(subscriptions: Subscription[], size: number) {
  const chunks = [];

  for (let i = 0; i < subscriptions.length; i += size) {
    chunks.push(subscriptions.slice(i, i + size));
  }

  return chunks;
}
