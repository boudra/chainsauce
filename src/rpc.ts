import fastq from "fastq";

import retry from "async-retry";
import { Hex, ToBlock } from "@/types";

export type Log = {
  address: Hex;
  blockHash: string;
  data: Hex;
  blockNumber: Hex;
  logIndex: Hex;
  topics: [Hex, ...Hex[]];
  transactionIndex: string;
  transactionHash: Hex;
};

type JsonRpcErrorCause = {
  method: string;
  url: string;
  params: unknown;
  responseStatusCode: number;
  errorResponse?: { message: string; code: number; data: unknown };
};

export class JsonRpcError extends Error {
  cause: JsonRpcErrorCause;
  constructor(cause: JsonRpcErrorCause) {
    super(
      `JsonRpcError: ${JSON.stringify({
        url: cause.url,
        method: cause.method,
        params: cause.params,
        error: cause.errorResponse,
        responseStatusCode: cause.responseStatusCode,
      })}`
    );
    this.cause = cause;
  }
}

export class JsonRpcRangeTooWideError extends Error {
  cause: JsonRpcError;
  constructor(cause: JsonRpcError) {
    super("JsonRpcRangeTooWideError");
    this.cause = cause;
  }
}

export interface RpcClient {
  getLastBlockNumber(): Promise<bigint>;
  getLogs(args: {
    address: Hex[] | Hex;
    topics: Hex[] | Hex[][];
    fromBlock: bigint;
    toBlock: ToBlock;
  }): Promise<Log[]>;
  readContract(args: {
    functionName: string;
    address: Hex;
    data: Hex;
    blockNumber: bigint;
  }): Promise<Hex>;
  subscribeToLogs(args: {
    onLogs: (logs: Log[]) => Promise<void>;
    address: Hex;
    topics: Hex[] | Hex[][];
  }): Promise<() => void>;
}

export function createConcurrentRpcClientWithRetry(args: {
  client: RpcClient;
  retryDelayMs: number;
  maxRetries: number;
  maxConcurrentRequests: number;
}): RpcClient {
  const { client, maxConcurrentRequests } = args;

  const queue = fastq.promise(async (task: () => Promise<unknown>) => {
    return retry(
      async (bail) => {
        try {
          return await task();
        } catch (error) {
          // do not retry if it's a non-500 or rate limit
          if (
            error instanceof JsonRpcError &&
            error.cause.responseStatusCode !== 429 &&
            error.cause.responseStatusCode !== 408 &&
            error.cause.responseStatusCode !== 420 &&
            error.cause.responseStatusCode < 500
          ) {
            bail(error);
            return;
          }

          // do not retry if the range is too wide
          if (error instanceof JsonRpcRangeTooWideError) {
            bail(error);
            return;
          }

          throw error;
        }
      },
      {
        retries: args.maxRetries,
        minTimeout: args.retryDelayMs,
        maxTimeout: args.retryDelayMs,
      }
    );
  }, maxConcurrentRequests);

  function queueRpcCall<T>(task: () => Promise<unknown>) {
    return queue.push(task) as Promise<T>;
  }

  return {
    async getLastBlockNumber(): Promise<bigint> {
      return queueRpcCall(() => client.getLastBlockNumber());
    },
    async getLogs(args): Promise<Log[]> {
      return queueRpcCall(() => client.getLogs(args));
    },
    async readContract(args): Promise<Hex> {
      return queueRpcCall(() => client.readContract(args));
    },
    async subscribeToLogs(args): Promise<() => void> {
      return client.subscribeToLogs(args);
    },
  };
}

export function createRpcClient(
  args: Parameters<typeof createRpcBaseClient>[0] &
    Parameters<typeof createConcurrentRpcClientWithRetry>[0]
): RpcClient {
  const retryDelayMs = args.retryDelayMs ?? 1000;
  const maxConcurrentRequests = args.maxConcurrentRequests ?? 10;
  const maxRetries = args.maxRetries ?? 5;

  return createConcurrentRpcClientWithRetry({
    client: createRpcBaseClient({
      url: args.url,
      fetch: args.fetch,
      onRequest: args.onRequest,
    }),
    retryDelayMs,
    maxRetries,
    maxConcurrentRequests,
  });
}

export function createRpcBaseClient(args: {
  url: string;
  fetch?: typeof globalThis.fetch;
  onRequest?: (request: {
    method: string;
    params: unknown;
    url: string;
  }) => void;
}): RpcClient {
  const { url } = args;

  const fetch = args.fetch ?? globalThis.fetch;

  async function rpcCall<T>(method: string, params: unknown): Promise<T> {
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method,
      params: params,
    });

    if (args.onRequest !== undefined) {
      args.onRequest({ method, params, url });
    }

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body,
    });

    if (response.status !== 200) {
      throw new JsonRpcError({
        url,
        method,
        params,
        responseStatusCode: response.status,
      });
    }

    const contentType = response.headers.get("Content-Type");

    if (!contentType || !contentType.includes("application/json")) {
      const body = await response.text();
      throw new Error(`Invalid response: ${body}`);
    }

    const json = await response.json();

    if ("error" in json) {
      throw new JsonRpcError({
        url,
        method,
        params,
        errorResponse: json.error,
        responseStatusCode: response.status,
      });
    }

    return json.result;
  }

  return {
    async getLogs(opts: {
      address: Hex[] | Hex;
      topics: Hex[] | Hex[][];
      fromBlock: bigint;
      toBlock: ToBlock;
    }): Promise<Log[]> {
      const toBlock =
        opts.toBlock === "latest"
          ? opts.toBlock
          : `0x${opts.toBlock.toString(16)}`;

      try {
        return await rpcCall<Log[]>("eth_getLogs", [
          {
            address: opts.address,
            topics: opts.topics,
            fromBlock: `0x${opts.fromBlock.toString(16)}`,
            toBlock: toBlock,
          },
        ]);
      } catch (e) {
        // there's no standard error code for this, so we have to check the message,
        // different providers have different error messages
        if (
          e instanceof JsonRpcError &&
          e.cause.errorResponse &&
          (e.cause.errorResponse.message.includes("query returned more than") ||
            e.cause.errorResponse.message.includes(
              "Log response size exceeded"
            ) ||
            e.cause.errorResponse.message.includes("block range is too wide") ||
            e.cause.errorResponse.message.includes(
              "exceed maximum block range"
            ))
        ) {
          throw new JsonRpcRangeTooWideError(e);
        } else {
          throw e;
        }
      }
    },

    async getLastBlockNumber(): Promise<bigint> {
      const response = await rpcCall<string>("eth_blockNumber", []);

      return BigInt(response);
    },

    async readContract(args: {
      functionName: string;
      address: Hex;
      data: Hex;
      blockNumber: bigint;
    }): Promise<Hex> {
      const blockNumber = `0x${args.blockNumber.toString(16)}`;

      return await rpcCall<Hex>("eth_call", [
        {
          to: args.address,
          data: args.data,
        },
        blockNumber,
      ]);
    },
  };
}
