import { decodeEventLog, fromHex, getAddress } from "viem";

import { Abi, AbiEvent } from "abitype";
import { Event, EventHandler, ToBlock } from "@/types";
import { Logger } from "@/logger";
import { JsonRpcRangeTooWideError, RpcClient } from "@/rpc";
import { SubscriptionStore } from "@/subscriptionStore";
import { Cache } from "@/cache";

export type Subscription = {
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

function getActiveSubscriptions(args: {
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

  const activeSubscriptions = getActiveSubscriptions({
    subscriptions: Array.from(subscriptions.values()),
    targetBlock,
  });

  const fetchRequests: Record<
    string,
    { from: bigint; to: bigint; subscriptions: Subscription[] }
  > = {};

  for (const { from, to, subscription } of activeSubscriptions) {
    let finalFetchFromBlock = from;

    if (cache) {
      // fetch events from the event store
      const result = await cache.getEvents({
        chainId,
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

  for (const { from, to, subscriptions: batchSubscriptions } of Object.values(
    fetchRequests
  )) {
    let currentBlock = from;

    const address = batchSubscriptions[0].contractAddress;
    const topics = batchSubscriptions.map((s) => s.topic);

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

        logger.trace(`Fetched events ${logs.length}`);

        const events = [];

        for (const log of logs) {
          const logAddress = getAddress(log.address);
          const subscription = getSubscription(
            subscriptions,
            `${logAddress}:${log.topics[0]}`
          );

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
