import { beforeEach, describe, expect, test, vi } from "vitest";

import { createIndexer, contract, ToBlock, Hex, Log, Event } from "@/index";
import { createSqliteEventStore } from "@/eventStore";
import { createSqliteSubscriptionStore } from "@/subscriptionStore";
import erc20ABI from "@/../test/erc20ABI";

const initialBlocks = [
  {
    number: 0n,
    logs: [
      {
        address: "0x123",
        topics: [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          "0x0000000000000000000000000000000000000000000000000000000000000001",
          "0x0000000000000000000000000000000000000000000000000000000000000002",
        ],
        data: "0x00000000000000000000000000000000000000000000000000000000000f4240",
        blockNumber: "0x0",
        logIndex: "0x0",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x123",
        topics: [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          "0x0000000000000000000000000000000000000000000000000000000000000002",
          "0x0000000000000000000000000000000000000000000000000000000000000001",
        ],
        data: "0x00000000000000000000000000000000000000000000000000000000000f4240",
        blockNumber: "0x0",
        logIndex: "0x1",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
    ] as Log[],
  },
  {
    number: 1n,
    logs: [
      {
        address: "0x123",
        topics: [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          "0x0000000000000000000000000000000000000000000000000000000000000001",
          "0x0000000000000000000000000000000000000000000000000000000000000002",
        ],
        data: "0x00000000000000000000000000000000000000000000000000000000000f4240",
        blockNumber: "0x1",
        logIndex: "0x0",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
      {
        address: "0x123",
        topics: [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          "0x0000000000000000000000000000000000000000000000000000000000000001",
          "0x0000000000000000000000000000000000000000000000000000000000000002",
        ],
        data: "0x00000000000000000000000000000000000000000000000000000000000f4240",
        blockNumber: "0x1",
        logIndex: "0x1",
        transactionIndex: "0x0",
        transactionHash: "0x123",
        blockHash: "0x123",
      },
    ] as Log[],
  },
];

// import ProjectRegistryABI from "test/projectRegistryABI";
// import VotingStrategyImplementationABI from "test/votingStrategyImplementation";
// type ProjectCreatedEvent = Event<typeof ProjectRegistryABI, "ProjectCreated">;
// type MetadataUpdatedEvent = Event<typeof ProjectRegistryABI, "MetadataUpdated">;
//
// async function handleProjectCreated(event: ProjectCreatedEvent) {
//   console.log(event);
// }

describe("index ERC20 contract", () => {
  let balances: Record<string, bigint>;
  let blocks: typeof initialBlocks = [];

  const rpcClient = {
    getLastBlockNumber: async () => {
      return blocks[blocks.length - 1].number;
    },
    getLogs: async (args: {
      address: Hex[] | Hex;
      topics: Hex[] | Hex[][];
      fromBlock: bigint;
      toBlock: ToBlock;
    }): Promise<Log[]> => {
      return blocks
        .filter(
          (block) =>
            block.number >= args.fromBlock &&
            (args.toBlock === "latest" || block.number <= args.toBlock)
        )
        .flatMap((block) => block.logs);
    },
  };

  const erc20Contract = contract({
    name: "MyToken",
    abi: erc20ABI,
    address: "0x123",
    handlers: {
      Transfer: handleTransfer,
    },
  });

  function handleTransfer(event: Event<typeof erc20ABI, "Transfer">) {
    const from = balances[event.params.from] || 0n;
    const to = balances[event.params.to] || 0n;

    balances[event.params.from] = from - event.params.value;
    balances[event.params.to] = to + event.params.value;
  }

  beforeEach(() => {
    balances = {};
    blocks = [...initialBlocks];
  });

  test("index to latest", async () => {
    const indexer = await createIndexer({
      rpc: rpcClient,
      contracts: [erc20Contract],
    });

    await indexer.indexToBlock("latest");

    expect(balances).toEqual({
      "0x0000000000000000000000000000000000000001": -2000000n,
      "0x0000000000000000000000000000000000000002": 2000000n,
    });
  });

  test("live indexing of new blocks", async () => {
    const indexer = await createIndexer({
      rpc: rpcClient,
      onUpdate: (block) => {
        // when we reach block 1, we add a new block to the chain
        if (block === 1n) {
          blocks.push({
            number: 2n,
            logs: [
              {
                address: "0x123",
                topics: [
                  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                  "0x0000000000000000000000000000000000000000000000000000000000000001",
                  "0x0000000000000000000000000000000000000000000000000000000000000002",
                ],
                data: "0x00000000000000000000000000000000000000000000000000000000000f4240",
                blockNumber: "0x2",
                logIndex: "0x0",
                transactionIndex: "0x0",
                transactionHash: "0x123",
                blockHash: "0x123",
              },
            ] as Log[],
          });
        }

        // when the new block is indexed, we stop the indexer
        if (block === 2n) {
          indexer.stop();
        }
      },
      contracts: [erc20Contract],
    });

    await indexer.start();

    expect(balances).toEqual({
      "0x0000000000000000000000000000000000000001": -3000000n,
      "0x0000000000000000000000000000000000000002": 3000000n,
    });
  });

  test("resumable index with the same indexer instance", async () => {
    const indexer = await createIndexer({
      rpc: rpcClient,
      contracts: [erc20Contract],
    });

    await indexer.indexToBlock(0n);

    expect(balances).toEqual({
      "0x0000000000000000000000000000000000000001": 0n,
      "0x0000000000000000000000000000000000000002": 0n,
    });

    await indexer.indexToBlock(1n);

    expect(balances).toEqual({
      "0x0000000000000000000000000000000000000001": -2000000n,
      "0x0000000000000000000000000000000000000002": 2000000n,
    });
  });

  test("event store is used", async () => {
    const getLogsMock = vi.fn().mockImplementation(rpcClient.getLogs);
    const eventStore = createSqliteEventStore(":memory:");

    let indexer = await createIndexer({
      eventStore,
      rpc: { ...rpcClient, getLogs: getLogsMock },
      contracts: [erc20Contract],
    });

    await indexer.indexToBlock(1n);

    expect(balances).toEqual({
      "0x0000000000000000000000000000000000000001": -2000000n,
      "0x0000000000000000000000000000000000000002": 2000000n,
    });

    expect(getLogsMock).toHaveBeenCalledTimes(1);

    getLogsMock.mockClear();

    balances = {};
    indexer = await createIndexer({
      eventStore,
      rpc: { ...rpcClient, getLogs: getLogsMock },
      contracts: [erc20Contract],
    });

    await indexer.indexToBlock(1n);

    expect(balances).toEqual({
      "0x0000000000000000000000000000000000000001": -2000000n,
      "0x0000000000000000000000000000000000000002": 2000000n,
    });

    expect(getLogsMock).toHaveBeenCalledTimes(0);
  });

  test("resumable index across restarts", async () => {
    const subscriptionStore = createSqliteSubscriptionStore(":memory:");

    {
      const indexer = await createIndexer({
        subscriptionStore,
        rpc: rpcClient,
        contracts: [erc20Contract],
      });

      await indexer.indexToBlock("latest");

      expect(balances).toEqual({
        "0x0000000000000000000000000000000000000001": -2000000n,
        "0x0000000000000000000000000000000000000002": 2000000n,
      });

      expect(await subscriptionStore.all()).toHaveLength(1);
    }

    {
      const indexer = await createIndexer({
        subscriptionStore,
        rpc: rpcClient,
        contracts: [erc20Contract],
      });

      await indexer.indexToBlock("latest");

      expect(balances).toEqual({
        "0x0000000000000000000000000000000000000001": -2000000n,
        "0x0000000000000000000000000000000000000002": 2000000n,
      });
    }
  });
});
