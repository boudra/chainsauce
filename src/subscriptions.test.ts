import { describe, test, it, expect } from "vitest";
import { getSubscriptionsToFetch, Subscription } from "./subscriptions";
import { zeroAddress } from "viem";

const subscription = {
  id: "1",
  chainId: 1,
  abi: [],
  contractName: "ContractA",
  contractAddress: zeroAddress,
  indexedToLogIndex: 0,
  createdAt: new Date(),
  updatedAt: new Date(),
};

describe("getSubscriptionsToFetch", () => {
  test("fetches from start", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 90n,
        toBlock: "latest",
        fetchedToBlock: -1n,
        indexedToBlock: -1n,
      },
    ];

    const result = getSubscriptionsToFetch({
      subscriptions,
      targetBlock: 100n,
    });

    expect(result).toEqual([
      {
        from: 90n,
        to: 100n,
        subscription: subscriptions[0],
      },
    ]);
  });

  it("filters out fetched subscriptions", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 50n,
        toBlock: "latest",
        fetchedToBlock: 90n,
        indexedToBlock: 80n,
      },
    ];

    const result = getSubscriptionsToFetch({
      subscriptions,
      targetBlock: 90n,
    });

    expect(result).toHaveLength(0);
  });

  it("fetches to latest", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 50n,
        toBlock: "latest",
        fetchedToBlock: 80n,
        indexedToBlock: 80n,
      },
    ];

    const result = getSubscriptionsToFetch({
      subscriptions,
      targetBlock: 90n,
    });

    expect(result).toEqual([
      {
        from: 81n,
        to: 90n,
        subscription: subscriptions[0],
      },
    ]);
  });

  it("fetches to end block", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 50n,
        toBlock: 85n,
        fetchedToBlock: 80n,
        indexedToBlock: 80n,
      },
    ];
    const targetBlock = 90n;

    const result = getSubscriptionsToFetch({ subscriptions, targetBlock });
    expect(result).toEqual([
      {
        from: 81n,
        to: 85n,
        subscription: subscriptions[0],
      },
    ]);
  });

  test("doesn't fetch if already indexed", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 90n,
        toBlock: "latest",
        fetchedToBlock: 0n,
        indexedToBlock: 100n,
      },
    ];

    const result = getSubscriptionsToFetch({
      subscriptions,
      targetBlock: 90n,
    });

    expect(result).toEqual([]);
  });

  test("doesn't fetch for blocks less than fromBlock", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 90n,
        toBlock: "latest",
        fetchedToBlock: 0n,
        indexedToBlock: 100n,
      },
    ];

    const result = getSubscriptionsToFetch({
      subscriptions,
      targetBlock: 80n,
    });

    expect(result).toHaveLength(0);
  });
});
