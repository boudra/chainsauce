import { Abi } from "abitype";

import { EventQueue } from "@/eventQueue";
import {
  Subscription,
  findLowestIndexedBlock,
  getSubscriptionSafe,
  updateSubscription,
} from "@/subscriptions";
import { Logger } from "@/logger";
import { AsyncEventEmitter } from "@/asyncEventEmitter";
import { Indexer, IndexerEvents } from "@/indexer";
import { EventHandler } from "@/types";
import { SubscriptionStore } from "./subscriptionStore";
import { Cache } from "@/cache";
import { RpcClient } from "@/rpc";

export async function processEvents<
  TAbis extends Record<string, Abi>,
  TContext
>(args: {
  chainId: number;
  targetBlock: bigint;
  finalTargetBlock: bigint;
  eventQueue: EventQueue;
  subscriptions: Map<string, Subscription>;
  contracts: Record<string, Abi>;
  logger: Logger;
  eventEmitter: AsyncEventEmitter<IndexerEvents<TAbis, TContext>>;
  subscriptionStore?: SubscriptionStore;
  context?: TContext;
  readContract: Indexer<TAbis, TContext>["readContract"];
  subscribeToContract: Indexer<TAbis, TContext>["subscribeToContract"];
  unsubscribeFromContract: Indexer<TAbis, TContext>["unsubscribeFromContract"];
  cache: Cache | null;
  rpcClient: RpcClient;
}) {
  const {
    chainId,
    targetBlock,
    finalTargetBlock,
    eventQueue,
    subscriptions,
    eventEmitter,
    context,
    readContract,
    subscriptionStore,
    subscribeToContract,
    unsubscribeFromContract,
    cache,
    rpcClient,
  } = args;

  const subscriptionCount = subscriptions.size;

  let indexedToBlock = findLowestIndexedBlock(subscriptions) ?? -1n;

  for (const event of eventQueue.drain()) {
    const subscription = getSubscriptionSafe(
      subscriptions,
      `${chainId}-${event.address}`
    );

    // If the subscription is null, it means that the subscription was deleted
    if (subscription === null) {
      continue;
    }

    const eventWithContractName = {
      ...event,
      contractName: subscription.contractName,
    };

    if (
      event.blockNumber === subscription.indexedToBlock &&
      event.logIndex < subscription.indexedToLogIndex
    ) {
      continue;
    }

    const eventHandlerArgs: Parameters<EventHandler>[0] = {
      event: eventWithContractName,
      chainId,
      context,
      readContract: (args) => {
        return readContract({
          ...args,
          blockNumber: event.blockNumber,
        });
      },
      unsubscribeFromContract: (opts) => {
        return unsubscribeFromContract(opts);
      },
      getBlock: async () => {
        const cachedBlock = await cache?.getBlockByNumber({
          chainId: chainId,
          blockNumber: event.blockNumber,
        });

        if (cachedBlock) {
          return cachedBlock;
        }

        const rpcBlock = await rpcClient.getBlockByNumber({
          number: event.blockNumber,
        });

        // should not happen
        if (!rpcBlock) {
          throw new Error(`Block ${event.blockNumber} not found`);
        }

        const block = {
          chainId: chainId,
          blockNumber: event.blockNumber,
          blockHash: rpcBlock.hash,
          timestamp: rpcBlock.timestamp,
        };

        if (cache) {
          await cache?.insertBlock(block);
        }

        return block;
      },
      subscribeToContract: (opts) => {
        return subscribeToContract({
          ...opts,
          fromBlock: event.blockNumber,
        });
      },
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

    if (subscriptionStore) {
      await subscriptionStore.update(subscription.id, {
        indexedToBlock: event.blockNumber,
        indexedToLogIndex: event.logIndex,
      });
    }

    // report progress when we start a new block
    if (indexedToBlock < event.blockNumber && indexedToBlock > -1n) {
      eventEmitter.emit("progress", {
        currentBlock: indexedToBlock,
        targetBlock: finalTargetBlock,
        pendingEventsCount: eventQueue.size(),
      });
    }

    indexedToBlock = event.blockNumber;

    // new subscriptions were added while processing
    // pause processing
    if (subscriptions.size > subscriptionCount) {
      return {
        indexedToBlock,
        indexedToLogIndex: event.logIndex,
        hasNewSubscriptions: true,
      };
    }
  }

  return {
    indexedToBlock: targetBlock,
    indexedToLogIndex: -1,
    hasNewSubscriptions: false,
  };
}
