import { createMemoryEventStore } from "@/eventStore";
import { BaseEvent } from "@/types";
import { describe, it, expect } from "vitest";

describe("event store", () => {
  it("inserts an event into memory event store", async () => {
    const max256BitBigInt = 2n ** 256n - 1n;

    const eventStore = createMemoryEventStore();
    const event: BaseEvent = {
      name: "EventName",
      params: { string: "value", bigint: max256BitBigInt },
      address: "0x123",
      topic: "0x456",
      transactionHash: "0x789",
      blockNumber: 100n,
      logIndex: 0,
    };

    await eventStore.insert(event);

    const storedEvents = await eventStore.getEvents({
      address: "0x123",
      topic: "0x456",
      fromBlock: BigInt(0),
      toBlock: "latest",
    });

    expect(storedEvents.length).toBe(1);
    expect(storedEvents[0]).toEqual(event);
  });
});
