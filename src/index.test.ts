import { beforeEach, describe, expect, test, vi } from "vitest";

import { buildIndexer, ToBlock, Hex, Log, Event } from "@/index";
import { createInMemoryCache } from "@/cache";
import { createSqliteSubscriptionStore } from "@/subscriptionStore";
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
        address: "0x1",
        topics: [incrementTopic],
        data: zeroAddress,
        blockNumber: "0x0",
        logIndex: "0x0",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x2",
        topics: [incrementTopic],
        data: zeroAddress,
        blockNumber: "0x0",
        logIndex: "0x3",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x1",
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
        address: "0x1",
        topics: [incrementTopic],
        data: zeroAddress,
        blockNumber: "0x2",
        logIndex: "0x0",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x1",
        topics: [incrementTopic],
        data: zeroAddress,
        blockNumber: "0x2",
        logIndex: "0x1",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x2",
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

describe("index ERC20 contract", () => {
  let state: {
    events: Event[];
    counters: Record<Hex, bigint>;
  };

  let blocks: typeof initialBlocks = [];

  const rpcClient: RpcClient = {
    getLastBlockNumber: async () => {
      return blocks[blocks.length - 1].number;
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
    const indexer = buildIndexer()
      .rpc(rpcClient)
      .contracts(Contracts)
      .addSubscription({
        contract: "Counter",
        address: "0x1",
      })
      .addSubscription({
        contract: "Counter",
        address: "0x2",
      })
      .addEventHandlers({
        contract: "Counter",
        handlers: {
          Increment: handleIncrement,
          Decrement: handleDecrement,
        },
      })
      .build();

    await indexer.indexToBlock("latest");

    expect(state.counters).toEqual({
      "0x1": 2n,
      "0x2": 0n,
    });
  });

  test("live indexing of new blocks", async () => {
    const indexer = buildIndexer()
      .rpc(rpcClient)
      .onProgress(({ currentBlock }) => {
        // when we reach block 1, we add a new block to the chain
        if (currentBlock === 2n) {
          blocks.push({
            number: 3n,
            logs: [
              {
                address: "0x2",
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
      })
      .contracts(Contracts)
      .addSubscription({
        contract: "Counter",
        address: "0x1",
      })
      .addSubscription({
        contract: "Counter",
        address: "0x2",
      })
      .addEventHandlers({
        contract: "Counter",
        handlers: {
          Increment: handleIncrement,
          Decrement: handleDecrement,
        },
      })
      .build();

    await indexer.watch();

    expect(state.events).toHaveLength(7);
    expect(state.counters).toEqual({
      "0x1": 2n,
      "0x2": 1n,
    });
  });

  test("resumable index with the same indexer instance", async () => {
    const indexer = buildIndexer()
      .rpc(rpcClient)
      .contracts(Contracts)
      .addSubscription({
        contract: "Counter",
        address: "0x1",
      })
      .addSubscription({
        contract: "Counter",
        address: "0x2",
      })
      .addEventHandlers({
        contract: "Counter",
        handlers: {
          Increment: handleIncrement,
          Decrement: handleDecrement,
        },
      })
      .build();

    await indexer.indexToBlock(0n);

    expect(state.counters).toEqual({
      "0x1": 0n,
      "0x2": 1n,
    });

    await indexer.indexToBlock(2n);

    expect(state.counters).toEqual({
      "0x1": 2n,
      "0x2": 0n,
    });
  });

  test("event store is used", async () => {
    const getLogsMock = vi.fn().mockImplementation(rpcClient.getLogs);
    const cache = createInMemoryCache();

    let indexer = buildIndexer()
      .rpc({ ...rpcClient, getLogs: getLogsMock })
      .cache(cache)
      .contracts(Contracts)
      .addSubscription({
        contract: "Counter",
        address: "0x1",
      })
      .addSubscription({
        contract: "Counter",
        address: "0x2",
      })
      .addEventHandlers({
        contract: "Counter",
        handlers: {
          Increment: handleIncrement,
          Decrement: handleDecrement,
        },
      })
      .build();

    await indexer.indexToBlock(2n);

    expect(state.counters).toEqual({
      "0x1": 2n,
      "0x2": 0n,
    });

    expect(getLogsMock).toHaveBeenCalled();

    getLogsMock.mockClear();

    state.events = [];
    state.counters = {};

    indexer = buildIndexer()
      .rpc({ ...rpcClient, getLogs: getLogsMock })
      .cache(cache)
      .contracts(Contracts)
      .addSubscription({
        contract: "Counter",
        address: "0x1",
      })
      .addSubscription({
        contract: "Counter",
        address: "0x2",
      })
      .addEventHandlers({
        contract: "Counter",
        handlers: {
          Increment: handleIncrement,
          Decrement: handleDecrement,
        },
      })
      .build();

    await indexer.indexToBlock(2n);

    expect(state.counters).toEqual({
      "0x1": 2n,
      "0x2": 0n,
    });

    expect(getLogsMock).toHaveBeenCalledTimes(0);
  });

  test("resumable index across restarts", async () => {
    const subscriptionStore = createSqliteSubscriptionStore(":memory:");

    {
      const indexer = buildIndexer()
        .rpc(rpcClient)
        .subscriptionStore(subscriptionStore)
        .contracts(Contracts)
        .addSubscription({
          contract: "Counter",
          address: "0x1",
        })
        .addSubscription({
          contract: "Counter",
          address: "0x2",
        })
        .addEventHandlers({
          contract: "Counter",
          handlers: {
            Increment: handleIncrement,
            Decrement: handleDecrement,
          },
        })
        .build();

      await indexer.indexToBlock("latest");

      expect(state.counters).toEqual({
        "0x1": 2n,
        "0x2": 0n,
      });

      expect(await subscriptionStore.all()).toHaveLength(4);
    }

    {
      blocks.push({
        number: 3n,
        logs: [
          {
            address: "0x2",
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

      const indexer = buildIndexer()
        .rpc(rpcClient)
        .subscriptionStore(subscriptionStore)
        .contracts(Contracts)
        .addSubscription({
          contract: "Counter",
          address: "0x1",
        })
        .addSubscription({
          contract: "Counter",
          address: "0x2",
        })
        .addEventHandlers({
          contract: "Counter",
          handlers: {
            Increment: handleIncrement,
            Decrement: handleDecrement,
          },
        })
        .build();

      await indexer.indexToBlock("latest");

      expect(state.events).toHaveLength(7);
      expect(state.counters).toEqual({
        "0x1": 2n,
        "0x2": 1n,
      });
    }
  });
});
