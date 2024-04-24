import { Block } from "@/cache";
import { createPostgresCache } from "@/cache/postgres";
import { Event } from "@/types";
import { describe, it, expect } from "vitest";
import { Pool } from "pg";

const makeEvent = (blockNumber: bigint): Event => ({
  name: "EventName",
  params: { string: "value", bigint: blockNumber },
  address: "0x123",
  topic: "0x456",
  transactionHash: "0x789",
  blockNumber,
  logIndex: 0,
});

const DATABASE_URL = process.env.DATABASE_URL;

async function createNewPostgresCache() {
  const pool = new Pool({
    connectionString: DATABASE_URL,
  });

  const cache = createPostgresCache({
    connectionPool: pool,
    schemaName: `test_${Math.random().toString(36).substring(7)}`,
  });

  await cache.migrate();

  return cache;
}

describe.runIf(DATABASE_URL !== undefined)("postgres cache", () => {
  it("inserts and retrieves events", async () => {
    const max256BitBigInt = 2n ** 256n - 1n;
    const cache = await createNewPostgresCache();

    const event: Event = {
      name: "EventName",
      params: { string: "value", bigint: max256BitBigInt },
      address: "0x123",
      topic: "0x456",
      transactionHash: "0x789",
      blockNumber: 100n,
      logIndex: 0,
    };

    await cache.insertEvents({
      chainId: 1,
      events: [event],
      address: "0x123",
      fromBlock: 100n,
      toBlock: 150n,
    });

    {
      const storedEvents = await cache.getEvents({
        chainId: 1,
        address: "0x123",
        fromBlock: 0n,
        toBlock: 150n,
      });

      expect(storedEvents).not.toBe(null);
      if (storedEvents !== null) {
        expect(storedEvents.events.length).toBe(1);
        expect(storedEvents.events[0]).toEqual(event);
        expect(storedEvents.fromBlock).toEqual(100n);
        expect(storedEvents.toBlock).toEqual(150n);
      }
    }

    {
      const storedEvents = await cache.getEvents({
        chainId: 1,
        address: "0x123",
        topic0: "0x456",
        fromBlock: 0n,
        toBlock: 99n,
      });

      expect(storedEvents).toBe(null);
    }

    {
      const storedEvents = await cache.getEvents({
        chainId: 1,
        address: "0x123",
        topic0: "0x456",
        fromBlock: 100n,
        toBlock: 101n,
      });

      expect(storedEvents).not.toBe(null);
      if (storedEvents !== null) {
        expect(storedEvents.events.length).toBe(1);
        expect(storedEvents.events[0]).toEqual(event);
        expect(storedEvents.fromBlock).toEqual(100n);
        expect(storedEvents.toBlock).toEqual(101n);
      }
    }

    {
      const storedEvents = await cache.getEvents({
        chainId: 1,
        address: "0x123",
        topic0: "0x456",
        fromBlock: 0n,
        toBlock: 100n,
      });

      expect(storedEvents).not.toBe(null);
      if (storedEvents !== null) {
        expect(storedEvents.events.length).toBe(1);
        expect(storedEvents.events[0]).toEqual(event);
        expect(storedEvents.fromBlock).toEqual(100n);
        expect(storedEvents.toBlock).toEqual(100n);
      }
    }
  });

  it("merges sequential log ranges", async () => {
    const cache = await createNewPostgresCache();

    const makeEvent = (blockNumber: bigint): Event => ({
      name: "EventName",
      params: { string: "value", bigint: blockNumber },
      address: "0x123",
      topic: "0x456",
      transactionHash: "0x789",
      blockNumber,
      logIndex: 0,
    });

    const eventsBatch1 = [makeEvent(1n), makeEvent(2n)];
    const eventsBatch2 = [makeEvent(3n), makeEvent(4n)];

    await cache.insertEvents({
      chainId: 1,
      events: eventsBatch1,
      address: "0x123",
      fromBlock: 1n,
      toBlock: 2n,
    });

    await cache.insertEvents({
      chainId: 1,
      events: eventsBatch2,
      address: "0x123",
      fromBlock: 3n,
      toBlock: 4n,
    });

    const storedEvents = await cache.getEvents({
      chainId: 1,
      address: "0x123",
      topic0: "0x456",
      fromBlock: 1n,
      toBlock: 5n,
    });

    expect(storedEvents).not.toBeNull();

    if (storedEvents !== null) {
      expect(storedEvents.events.length).toBe(4);
      const expectedEvents = [...eventsBatch1, ...eventsBatch2];
      expect(storedEvents.events).toEqual(expectedEvents);
      expect(storedEvents.fromBlock).toEqual(1n);
      expect(storedEvents.toBlock).toEqual(4n);
    }
  });

  it("merges overlapping log ranges", async () => {
    const cache = await createNewPostgresCache();

    const eventsBatch1 = [makeEvent(1n), makeEvent(2n)];
    const eventsBatch2 = [makeEvent(2n), makeEvent(4n)];

    await cache.insertEvents({
      chainId: 1,
      events: eventsBatch1,
      address: "0x123",
      fromBlock: 1n,
      toBlock: 2n,
    });

    await cache.insertEvents({
      chainId: 1,
      events: eventsBatch2,
      address: "0x123",
      fromBlock: 2n,
      toBlock: 4n,
    });

    const storedEvents = await cache.getEvents({
      chainId: 1,
      address: "0x123",
      topic0: "0x456",
      fromBlock: 1n,
      toBlock: 5n,
    });

    expect(storedEvents).not.toBeNull();

    if (storedEvents !== null) {
      expect(storedEvents.events.length).toBe(3);
      const expectedEvents = [makeEvent(1n), makeEvent(2n), makeEvent(4n)];
      expect(storedEvents.events).toEqual(expectedEvents);
      expect(storedEvents.fromBlock).toEqual(1n);
      expect(storedEvents.toBlock).toEqual(4n);
    }
  });

  it("returns empty array if range not fetched", async () => {
    const cache = await createNewPostgresCache();

    await cache.insertEvents({
      chainId: 1,
      events: [makeEvent(1n)],
      address: "0x123",
      fromBlock: 1n,
      toBlock: 2n,
    });

    const storedEvents = await cache.getEvents({
      chainId: 1,
      address: "0x123",
      topic0: "0x456",
      fromBlock: 3n,
      toBlock: 4n,
    });

    expect(storedEvents).toBeNull();
  });

  describe("block cache", async () => {
    it("returns null on non existent blocks", async () => {
      const cache = await createNewPostgresCache();
      const cachedBlock = await cache.getBlockByNumber({
        chainId: 1,
        blockNumber: 1n,
      });

      expect(cachedBlock).toBeNull();
    });

    it("returns block on existent blocks", async () => {
      const cache = await createNewPostgresCache();
      const block: Block = {
        chainId: 1,
        blockNumber: 1n,
        blockHash: "0x123",
        timestamp: 123,
      };
      await cache.insertBlock(block);

      const cachedBlock = await cache.getBlockByNumber({
        chainId: 1,
        blockNumber: 1n,
      });

      expect(cachedBlock).toEqual(block);
    });
  });
});
