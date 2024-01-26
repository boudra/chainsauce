import { describe, test, it, expect } from "vitest";
import {
  getSubscriptionsToIndex,
  getSubscriptionsToFetch,
  Subscription,
} from "./subscriptions";
import { zeroAddress } from "viem";

const subscription = {
  id: "1",
  chainId: 1,
  abi: [],
  contractName: "ContractA",
  contractAddress: zeroAddress,
  indexedToLogIndex: 0,
};

describe("getUnfetchedSubscriptions", () => {
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

  test("only fetches starting at fromBlock", () => {
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

    expect(result).toEqual([
      {
        from: 90n,
        to: 90n,
        subscription: subscriptions[0],
      },
    ]);
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

describe("getOutdatedSubscriptions", () => {
  it("filters out indexed subscriptions", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 50n,
        toBlock: "latest",
        fetchedToBlock: 90n,
        indexedToBlock: 90n,
      },
    ];

    const result = getSubscriptionsToIndex({
      subscriptions,
      targetBlock: 90n,
    });

    expect(result).toHaveLength(0);
  });

  it("returns outdated subscriptions", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 50n,
        toBlock: "latest",
        fetchedToBlock: 100n,
        indexedToBlock: 90n,
      },
    ];

    const result = getSubscriptionsToIndex({
      subscriptions,
      targetBlock: 100n,
    });

    expect(result).toEqual([
      {
        from: 91n,
        to: 100n,
        subscription: subscriptions[0],
      },
    ]);
  });

  it("doesn't index before fromBlock", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 50n,
        toBlock: "latest",
        fetchedToBlock: 90n,
        indexedToBlock: 90n,
      },
    ];

    const result = getSubscriptionsToIndex({
      subscriptions,
      targetBlock: 40n,
    });

    expect(result).toHaveLength(0);
  });

  it("it always starts at fromBlock", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 50n,
        toBlock: "latest",
        fetchedToBlock: 90n,
        indexedToBlock: 0n,
      },
    ];

    const result = getSubscriptionsToIndex({
      subscriptions,
      targetBlock: 80n,
    });

    expect(result).toEqual([
      {
        from: 50n,
        to: 80n,
        subscription: subscriptions[0],
      },
    ]);
  });

  it("fails if we're indexing to a block that we haven't fetched", () => {
    const subscriptions: Subscription[] = [
      {
        ...subscription,
        fromBlock: 50n,
        toBlock: "latest",
        fetchedToBlock: 50n,
        indexedToBlock: 0n,
      },
    ];

    expect(() => {
      getSubscriptionsToIndex({
        subscriptions,
        targetBlock: 80n,
      });
    }).toThrow();
  });
});
