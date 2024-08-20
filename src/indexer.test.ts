import { beforeEach, describe, expect, test, vi } from "vitest";

import { createIndexer, ToBlock, Hex, Log, Event } from "@/index";
import { createSqliteCache } from "@/cache/sqlite";
import { createSqliteSubscriptionStore } from "@/subscriptionStore/sqlite";
import { RpcClient } from "@/rpc";
import { encodeEventTopics, zeroAddress } from "viem";

const counterABI = [
  {
    inputs: [],
    name: "Increment",
    type: "event",
  },
  {
    inputs: [],
    name: "Decrement",
    type: "event",
  },
  {
    type: "function",
    name: "counter",
    stateMutability: "view",
    inputs: [],
    outputs: [],
  },
] as const;

const incrementTopic = encodeEventTopics({
  abi: counterABI,
  eventName: "Increment",
})[0];

const decrementTopic = encodeEventTopics({
  abi: counterABI,
  eventName: "Decrement",
})[0];

const Contracts = {
  Counter: counterABI,
};

const initialBlocks: { number: bigint; logs: Log[] }[] = [
  {
    number: 0n,
    logs: [
      {
        address: "0x0000000000000000000000000000000000000001",
        topics: [incrementTopic],
        data: zeroAddress,
        blockNumber: "0x0",
        logIndex: "0x0",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x0000000000000000000000000000000000000002",
        topics: [incrementTopic],
        data: zeroAddress,
        blockNumber: "0x0",
        logIndex: "0x3",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x0000000000000000000000000000000000000001",
        topics: [decrementTopic],
        data: zeroAddress,
        blockNumber: "0x0",
        logIndex: "0x4",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
    ],
  },
  {
    number: 1n,
    logs: [],
  },
  {
    number: 2n,
    logs: [
      {
        address: "0x0000000000000000000000000000000000000001",
        topics: [incrementTopic],
        data: zeroAddress,
        blockNumber: "0x2",
        logIndex: "0x0",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x0000000000000000000000000000000000000001",
        topics: [incrementTopic],
        data: zeroAddress,
        blockNumber: "0x2",
        logIndex: "0x1",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x0000000000000000000000000000000000000002",
        topics: [decrementTopic],
        data: zeroAddress,
        blockNumber: "0x2",
        logIndex: "0x2",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
    ],
  },
];

describe("counter contract", () => {
  let state: {
    events: Event[];
    counters: Record<Hex, bigint>;
  };

  let blocks: typeof initialBlocks = [];

  const rpcClient: RpcClient = {
    getLastBlockNumber: async () => {
      return blocks[blocks.length - 1].number;
    },
    getBlockByNumber(_args) {
      throw new Error("Not implemented");
    },
    getLogs: async (args: {
      address: Hex[];
      topics: [Hex[]] | [];
      fromBlock: bigint;
      toBlock: ToBlock;
    }): Promise<Log[]> => {
      return blocks
        .filter(
          (block) =>
            block.number >= args.fromBlock &&
            (args.toBlock === "latest" || block.number <= args.toBlock)
        )
        .flatMap((block) =>
          block.logs.filter((l) => {
            const matchesAddress = args.address.includes(l.address);
            let matchesTopics = true;

            if (args.topics.length > 0) {
              const eventSignatures = args.topics[0];

              if (eventSignatures && eventSignatures.length === 0) {
                matchesTopics = eventSignatures.some((eventSignature) => {
                  return l.topics.includes(eventSignature);
                });
              }
            }

            return matchesAddress && matchesTopics;
          })
        );
    },
    async readContract<T>(_args: unknown) {
      // TODO: implement
      return undefined as T;
    },
  };

  async function handleIncrement({
    event,
  }: {
    event: Event<typeof counterABI, "Increment">;
  }) {
    state.events.push(event as unknown as Event);
    state.counters[event.address] = (state.counters[event.address] ?? 0n) + 1n;
  }

  async function handleDecrement({
    event,
  }: {
    event: Event<typeof counterABI, "Decrement">;
  }) {
    state.events.push(event as unknown as Event);
    state.counters[event.address] = (state.counters[event.address] ?? 0n) - 1n;
  }

  beforeEach(() => {
    state = { events: [], counters: {} };
    blocks = [...initialBlocks];
  });

  test("index to latest", async () => {
    const indexer = createIndexer({
      chain: {
        id: 1,
        rpcClient: rpcClient,
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000001",
    });

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000002",
    });

    await indexer.indexToBlock("latest");

    expect(state.events).toHaveLength(6);

    expect(state.counters).toEqual({
      "0x0000000000000000000000000000000000000001": 2n,
      "0x0000000000000000000000000000000000000002": 0n,
    });
  });

  test("watch mode event handler error handling", async () => {
    const indexer = createIndexer({
      chain: {
        id: 1,
        rpcClient: rpcClient,
        pollingIntervalMs: 0,
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", async (args) => {
      await handleIncrement(args);
      throw new Error("error");
    });
    indexer.on("Counter:Decrement", handleDecrement);

    const errorHandler = vi.fn();

    indexer.on("error", errorHandler);

    indexer.on("progress", async ({ currentBlock }) => {
      if (currentBlock === 2n) {
        indexer.stop();
      }
    });

    await new Promise<void>((resolve) => {
      indexer.on("stopped", () => {
        expect(state.events).toHaveLength(6);
        expect(state.counters).toEqual({
          "0x0000000000000000000000000000000000000001": 2n,
          "0x0000000000000000000000000000000000000002": 0n,
        });
        resolve();
      });

      indexer.subscribeToContract({
        contract: "Counter",
        address: "0x0000000000000000000000000000000000000001",
      });

      indexer.subscribeToContract({
        contract: "Counter",
        address: "0x0000000000000000000000000000000000000002",
      });

      indexer.watch();
    });
    expect(errorHandler).toHaveBeenCalledTimes(4);
  });

  test("watch mode rpc error handling", async () => {
    vi.useFakeTimers();

    const intervalMs = 1000;

    const indexer = createIndexer({
      chain: {
        id: 1,
        pollingIntervalMs: intervalMs,
        rpcClient: {
          ...rpcClient,
          getLastBlockNumber: async () => {
            throw new Error("error");
          },
        },
      },
      contracts: Contracts,
    });

    const errorHandler = vi.fn();

    indexer.on("error", errorHandler);

    indexer.watch();

    await vi.waitFor(() => {
      expect(errorHandler).toHaveBeenCalledTimes(1);
    });

    vi.advanceTimersByTime(intervalMs);

    await vi.waitFor(() => {
      expect(errorHandler).toHaveBeenCalledTimes(2);
    });

    vi.advanceTimersByTime(intervalMs);

    await vi.waitFor(() => {
      expect(errorHandler).toHaveBeenCalledTimes(3);
    });

    vi.useRealTimers();
  });

  test("progress reporting", async () => {
    const indexer = createIndexer({
      chain: {
        id: 1,
        rpcClient: rpcClient,
        pollingIntervalMs: 0,
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000001",
    });

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000002",
    });

    const progressHandler = vi.fn();

    indexer.on("progress", progressHandler);

    await indexer.indexToBlock("latest");

    expect(progressHandler).toHaveBeenCalledTimes(2);

    expect(progressHandler.mock.calls[0][0]).toEqual({
      currentBlock: 0n,
      targetBlock: 2n,
      pendingEventsCount: 2,
    });

    expect(progressHandler.mock.calls[1][0]).toEqual({
      currentBlock: 2n,
      targetBlock: 2n,
      pendingEventsCount: 0,
    });
  });

  test("subscription control", async () => {
    const indexer = createIndexer({
      chain: {
        id: 1,
        rpcClient: rpcClient,
        pollingIntervalMs: 0,
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000001",
    });

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000002",
    });

    expect(indexer.getSubscriptions()).toHaveLength(2);

    indexer.unsubscribeFromContract({
      address: "0x0000000000000000000000000000000000000001",
    });

    expect(indexer.getSubscriptions()).toHaveLength(1);

    expect(indexer.getSubscriptions()[0].contractAddress).toEqual(
      "0x0000000000000000000000000000000000000002"
    );
  });

  test("live indexing of new blocks", async () => {
    const indexer = createIndexer({
      chain: {
        id: 1,
        pollingIntervalMs: 0,
        rpcClient: rpcClient,
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.on("progress", async ({ currentBlock }) => {
      // when we reach block 1, we add a new block to the chain
      if (currentBlock === 2n) {
        blocks.push({
          number: 3n,
          logs: [
            {
              address: "0x0000000000000000000000000000000000000002",
              topics: [incrementTopic],
              data: zeroAddress,
              blockNumber: "0x3",
              logIndex: "0x0",
              transactionIndex: "0x0",
              transactionHash: "0x123",
              blockHash: "0x123",
            },
          ],
        });
      }

      // when the new block is indexed, we stop the indexer
      if (currentBlock === 3n) {
        indexer.stop();
      }
    });

    await new Promise<void>((resolve) => {
      indexer.on("stopped", () => {
        expect(state.events).toHaveLength(7);
        expect(state.counters).toEqual({
          "0x0000000000000000000000000000000000000001": 2n,
          "0x0000000000000000000000000000000000000002": 1n,
        });
        resolve();
      });

      indexer.subscribeToContract({
        contract: "Counter",
        address: "0x0000000000000000000000000000000000000001",
      });

      indexer.subscribeToContract({
        contract: "Counter",
        address: "0x0000000000000000000000000000000000000002",
      });

      indexer.watch();
    });
  });

  test("lagBlocks config option", async () => {
    const mockRpcClient = {
      ...rpcClient,
      getLastBlockNumber: vi.fn()
        .mockResolvedValueOnce(4n)
        .mockResolvedValueOnce(4n)
        .mockResolvedValue(4n)
        .mockResolvedValue(6n)
    };

    const indexer = createIndexer({
      chain: {
        id: 1,
        rpcClient: mockRpcClient,
        lagBlocks: 2n,
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000001",
    });


    let tryTwice: boolean;
    indexer.on("progress", ({ currentBlock, targetBlock }) => {
      console.log(currentBlock);
      console.log(targetBlock);

      if (currentBlock == targetBlock && targetBlock == 4n){
        if (tryTwice){
          indexer.stop();
        }
        tryTwice = true;
      }
    });

    await new Promise<void>((resolve) => {
      indexer.on("stopped", () => {
        resolve();
        expect(tryTwice).toBe(true);
      });

      indexer.watch();
    });

  });

  

  test("resumable index with the same indexer instance", async () => {
    const indexer = createIndexer({
      chain: {
        id: 1,
        rpcClient: rpcClient,
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000001",
    });

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000002",
    });

    await indexer.indexToBlock(0n);

    expect(state.counters).toEqual({
      "0x0000000000000000000000000000000000000001": 0n,
      "0x0000000000000000000000000000000000000002": 1n,
    });

    await indexer.indexToBlock(2n);

    expect(state.counters).toEqual({
      "0x0000000000000000000000000000000000000001": 2n,
      "0x0000000000000000000000000000000000000002": 0n,
    });
  });

  test("event store is used", async () => {
    const getLogsMock = vi.fn().mockImplementation(rpcClient.getLogs);
    const cache = createSqliteCache(":memory:");

    let indexer = createIndexer({
      cache: cache,
      chain: {
        id: 1,
        rpcClient: { ...rpcClient, getLogs: getLogsMock },
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000001",
    });

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000002",
    });

    await indexer.indexToBlock(2n);

    expect(state.counters).toEqual({
      "0x0000000000000000000000000000000000000001": 2n,
      "0x0000000000000000000000000000000000000002": 0n,
    });

    expect(getLogsMock).toHaveBeenCalled();

    getLogsMock.mockClear();

    state.events = [];
    state.counters = {};

    indexer = createIndexer({
      cache: cache,
      chain: {
        id: 1,
        rpcClient: { ...rpcClient, getLogs: getLogsMock },
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000001",
    });

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000002",
    });

    await indexer.indexToBlock(2n);

    expect(state.counters).toEqual({
      "0x0000000000000000000000000000000000000001": 2n,
      "0x0000000000000000000000000000000000000002": 0n,
    });

    expect(getLogsMock).toHaveBeenCalledTimes(0);
  });

  test("resumable indexing across restarts", async () => {
    const subscriptionStore = createSqliteSubscriptionStore(":memory:");

    {
      const indexer = createIndexer({
        subscriptionStore: subscriptionStore,
        chain: {
          id: 1,
          rpcClient: rpcClient,
        },
        contracts: Contracts,
      });

      indexer.on("Counter:Increment", handleIncrement);
      indexer.on("Counter:Decrement", handleDecrement);

      indexer.subscribeToContract({
        contract: "Counter",
        address: "0x0000000000000000000000000000000000000001",
      });

      indexer.subscribeToContract({
        contract: "Counter",
        address: "0x0000000000000000000000000000000000000002",
      });

      await indexer.indexToBlock("latest");

      expect(state.events).toHaveLength(6);

      expect(state.counters).toEqual({
        "0x0000000000000000000000000000000000000001": 2n,
        "0x0000000000000000000000000000000000000002": 0n,
      });

      expect(await subscriptionStore.all(1)).toHaveLength(2);
    }

    {
      blocks.push({
        number: 3n,
        logs: [
          {
            address: "0x0000000000000000000000000000000000000002",
            topics: [incrementTopic],
            data: zeroAddress,
            blockNumber: "0x3",
            logIndex: "0x0",
            transactionIndex: "0x0",
            transactionHash: "0x123",
            blockHash: "0x123",
          },
        ],
      });

      const indexer = createIndexer({
        subscriptionStore: subscriptionStore,
        chain: {
          id: 1,
          rpcClient: rpcClient,
        },
        contracts: Contracts,
      });

      indexer.on("Counter:Increment", handleIncrement);
      indexer.on("Counter:Decrement", handleDecrement);

      indexer.subscribeToContract({
        contract: "Counter",
        address: "0x0000000000000000000000000000000000000001",
      });

      indexer.subscribeToContract({
        contract: "Counter",
        address: "0x0000000000000000000000000000000000000002",
      });

      await indexer.indexToBlock("latest");

      expect(state.events).toHaveLength(7);

      expect(state.counters).toEqual({
        "0x0000000000000000000000000000000000000001": 2n,
        "0x0000000000000000000000000000000000000002": 1n,
      });
    }
  });

  test("subscription with fromBlock and toBlock", async () => {
    const fromBlock = 2n;
    const toBlock = 2n;

    const indexer = createIndexer({
      chain: {
        id: 1,
        rpcClient: rpcClient,
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000001",
      fromBlock,
      toBlock,
    });

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000002",
      fromBlock,
      toBlock,
    });

    await indexer.indexToBlock("latest");

    expect(state.events).toHaveLength(3);
    expect(state.counters).toEqual({
      "0x0000000000000000000000000000000000000001": 2n,
      "0x0000000000000000000000000000000000000002": -1n,
    });
  });

  test("no events if subscriptions are up to date", async () => {
    const indexer = createIndexer({
      chain: {
        id: 1,
        rpcClient: rpcClient,
      },
      contracts: Contracts,
    });

    indexer.on("Counter:Increment", handleIncrement);
    indexer.on("Counter:Decrement", handleDecrement);

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000001",
      fromBlock: 3n,
    });

    indexer.subscribeToContract({
      contract: "Counter",
      address: "0x0000000000000000000000000000000000000002",
      fromBlock: 3n,
    });

    await indexer.indexToBlock("latest");

    expect(state.events).toHaveLength(0);
    expect(state.counters).toEqual({});
  });
});
