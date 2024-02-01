import { test, assert, expect, vi } from "vitest";
import {
  createHttpRpcClient,
  JsonRpcError,
  JsonRpcRangeTooWideError,
} from "./rpc";

test("should get last block number", async () => {
  const mockFetch = vi.fn().mockResolvedValue({
    status: 200,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({ result: "0x2A" }),
  });

  const rpcClient = createHttpRpcClient({
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

  expect(mockFetch.mock.calls[0][0]).toBe("http://localhost");
  expect(mockFetch.mock.calls[0][1]).toMatchObject({
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
  const mockFetch = vi.fn().mockResolvedValue({
    status: 200,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({ result: fakeLogs }),
  });

  const rpcClient = createHttpRpcClient({
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
  const mockFetch = vi.fn().mockResolvedValue({
    status: 200,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({ error: { message: "fail", code: -1, data: null } }),
  });

  const rpcClient = createHttpRpcClient({
    url: "http://localhost",
    fetch: mockFetch,
  });

  const promise = rpcClient.getLastBlockNumber();
  await expect(promise).rejects.toThrow(JsonRpcError);
  expect(mockFetch).toHaveBeenCalledTimes(1);
});

test("should retry on 500 status", async () => {
  const mockFetch = vi.fn().mockResolvedValue({
    status: 500,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({}),
  });

  const rpcClient = createHttpRpcClient({
    url: "http://localhost",
    fetch: mockFetch,
    retryDelayMs: 0,
    maxRetries: 4,
  });

  const promise = rpcClient.getLastBlockNumber();
  await expect(promise).rejects.toThrow(JsonRpcError);
  expect(mockFetch).toHaveBeenCalledTimes(5);
});

test("should retry on 429 status", async () => {
  const mockFetch = vi.fn().mockResolvedValue({
    status: 429,
    headers: new Headers({ "Content-Type": "application/json" }),
    json: async () => ({}),
  });

  const rpcClient = createHttpRpcClient({
    url: "http://localhost",
    fetch: mockFetch,
    retryDelayMs: 0,
    maxRetries: 4,
  });

  const promise = rpcClient.getLastBlockNumber();
  await expect(promise).rejects.toThrow(JsonRpcError);
  expect(mockFetch).toHaveBeenCalledTimes(5);
});

test("should throw JsonRpcRangeTooWideError on Alchemy query error", async () => {
  const mockFetch = vi.fn().mockResolvedValue({
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

  const rpcClient = createHttpRpcClient({
    url: "http://localhost",
    fetch: mockFetch,
  });

  const promise = rpcClient.getLogs({
    address: "0x123",
    topics: ["0x789"],
    fromBlock: 1n,
    toBlock: 42n,
  });
  await expect(promise).rejects.toThrow(JsonRpcRangeTooWideError);
  expect(mockFetch).toHaveBeenCalledTimes(1);
});
