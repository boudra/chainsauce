import { decodeEventLog, fromHex, getAddress } from "viem";

import { Abi } from "abitype";
import { Event, Hex, ToBlock } from "@/types";
import { Logger } from "@/logger";
import { JsonRpcRangeTooWideError, Log, RpcClient } from "@/rpc";
import { SubscriptionStore } from "@/subscriptionStore";
import { Cache } from "@/cache";

export type Subscription = {
  id: string;
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
  address: Hex;
  fromBlock: bigint;
  toBlock: bigint;
  topics: Hex[] | Hex[][];
  logger: Logger;
  onLogs: (args: { from: bigint; to: bigint; logs: Log[] }) => void;
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

      logger.trace(
        `Fetched ${logs.length} events ${cursor}-${pageToBlock} (${address})`
      );

      onLogs({ logs, from: cursor, to: pageToBlock });

      cursor = pageToBlock + 1n;

      // range successfully fetched, fetch wider range
      steps = steps / 2n;
      if (steps < 1n) {
        steps = 1n;
      }
    } catch (error) {
      // range too wide or too many logs returned, split in half and retry
      if (error instanceof JsonRpcRangeTooWideError) {
        logger.warn(
          `Range too wide ${cursor}-${pageToBlock}, retrying with smaller range`
        );
        steps = steps * 2n;
        continue;
      }

      throw error;
    }
  }
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

  const outdatedSubscriptions = getOutdatedSubscriptions({
    subscriptions: Array.from(subscriptions.values()),
    targetBlock,
  });

  const fetchPromises = [];

  for (const { from, to, subscription } of outdatedSubscriptions) {
    let finalFetchFromBlock = from;

    if (cache) {
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

        finalFetchFromBlock = result.toBlock + 1n;

        if (finalFetchFromBlock >= to) {
          continue;
        }
      }
    }

    const promise = fetchLogsWithRetry({
      rpc,
      address: subscription.contractAddress,
      fromBlock: finalFetchFromBlock,
      toBlock: to,
      topics: [],
      logger,
      onLogs: async ({ from: chunkFromBlock, to: chunkToBlock, logs }) => {
        const events: Event[] = [];

        for (const log of logs) {
          const logAddress = getAddress(log.address);
          const subscription = getSubscription(subscriptions, logAddress);

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
            name: parsedEvent.eventName,
            params: parsedEvent.args,
            address: logAddress,
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
            chainId,
            address: subscription.contractAddress,
            fromBlock: chunkFromBlock,
            toBlock: chunkToBlock,
            events,
          });
        }
      },
    });

    fetchPromises.push(promise);
  }

  await Promise.all(fetchPromises);

  return outdatedSubscriptions.map(({ subscription }) => subscription.id);
}
