import { createSqliteCache } from "@/cache/sqlite";
import { Event } from "@/types";
import { describe, it, expect } from "vitest";

const makeEvent = (blockNumber: bigint): Event => ({
  name: "EventName",
  params: { string: "value", bigint: blockNumber },
  address: "0x123",
  topic: "0x456",
  transactionHash: "0x789",
  blockNumber,
  logIndex: 0,
});

describe("event store", () => {
  it("inserts and retrieves events", async () => {
    const max256BitBigInt = 2n ** 256n - 1n;
    const cache = createSqliteCache(":memory:");

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
    const cache = createSqliteCache(":memory:");

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
    const cache = createSqliteCache(":memory:");

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
    const cache = createSqliteCache(":memory:");

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
});
