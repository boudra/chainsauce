import { test, assert, expect, vi } from "vitest";
import { Logger } from "@/logger";
import { createRpcClient, JsonRpcError, JsonRpcRangeTooWideError } from "./rpc";

const mockFetch = vi.fn();
const mockLogger = new Logger();

test("should get last block number", async () => {
  mockFetch.mockResolvedValue({
    status: 200,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({ result: "0x2A" }),
  });

  const rpcClient = createRpcClient({
    logger: mockLogger,
    url: "http://localhost",
    fetch: mockFetch,
  });
  const result = await rpcClient.getLastBlockNumber();
  expect(result).toBe(42n);

  const expectedBody = JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "eth_blockNumber",
    params: [],
  });

  expect(mockFetch).toHaveBeenCalledWith("http://localhost", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: expectedBody,
  });
});

test("should get logs", async () => {
  const fakeLogs = [
    {
      address: "0x123",
      blockHash: "0xabc",
      data: "0x456",
      blockNumber: "0x2A",
      logIndex: "0x1",
      topics: ["0x789"],
      transactionIndex: "0x3",
      transactionHash: "0xdef",
    },
  ];
  mockFetch.mockResolvedValue({
    status: 200,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({ result: fakeLogs }),
  });

  const rpcClient = createRpcClient({
    logger: mockLogger,
    url: "http://localhost",
    fetch: mockFetch,
  });

  const logs = await rpcClient.getLogs({
    address: "0x123",
    topics: ["0x789"],
    fromBlock: 1n,
    toBlock: 42n,
  });

  assert.deepEqual(logs, fakeLogs);
});

test("should throw JsonRpcError on error", async () => {
  mockFetch.mockResolvedValue({
    status: 200,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({ error: { message: "fail", code: -1, data: null } }),
  });

  const rpcClient = createRpcClient({
    logger: mockLogger,
    url: "http://localhost",
    fetch: mockFetch,
  });

  try {
    await rpcClient.getLastBlockNumber();
    throw new Error("Should not reach this point");
  } catch (err) {
    assert.instanceOf(err, JsonRpcError);
  }
});

test("should throw JsonRpcError on 500 error", async () => {
  mockFetch.mockResolvedValue({
    status: 500,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({}),
  });

  const rpcClient = createRpcClient({
    logger: mockLogger,
    url: "http://localhost",
    fetch: mockFetch,
  });

  try {
    await rpcClient.getLastBlockNumber();
    throw new Error("Should not reach this point");
  } catch (err) {
    assert.instanceOf(err, JsonRpcError);
  }
});

test("should throw JsonRpcRangeTooWideError on Alchemy query error", async () => {
  mockFetch.mockResolvedValue({
    status: 200,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({
      error: {
        message: "query returned more than 10000 results",
        code: -1,
        data: null,
      },
    }),
  });

  const rpcClient = createRpcClient({
    logger: mockLogger,
    url: "http://localhost",
    fetch: mockFetch,
  });

  try {
    await rpcClient.getLogs({
      address: "0x123",
      topics: ["0x789"],
      fromBlock: 1n,
      toBlock: 42n,
    });
    throw new Error("Should not reach this point");
  } catch (err) {
    assert.instanceOf(err, JsonRpcRangeTooWideError);
  }
});
