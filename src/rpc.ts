import fastq from "fastq";

import { retry } from "@/retry";
import { Logger } from "@/logger";
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
}

export function createRpcClientFromConfig(args: {
  maxConcurrentRequests: number;
  maxRetries: number;
  delayBetweenRetries: number;
  client: RpcClient | { url: string; fetch?: typeof globalThis.fetch };
  logger: Logger;
}): RpcClient {
  const { client: rpc, logger } = args;

  let client: RpcClient;

  if ("url" in rpc) {
    client = createRpcClient({
      logger,
      url: rpc.url,
    });
  } else if ("getLastBlockNumber" in rpc) {
    client = rpc;
  } else {
    throw new Error("Invalid RPC options, please provide a URL or a client");
  }

  return createConcurrentRpcClient({
    client,
    maxConcurrentRequests: args.maxConcurrentRequests,
    maxRetries: args.maxRetries,
    delayBetweenRetries: args.delayBetweenRetries,
  });
}
export function createConcurrentRpcClient(args: {
  client: RpcClient;
  delayBetweenRetries: number;
  maxRetries: number;
  maxConcurrentRequests: number;
}): RpcClient {
  const { client, maxConcurrentRequests } = args;

  const queue = fastq.promise(async (task: () => Promise<unknown>) => {
    return retry(
      async () => {
        return await task();
      },
      {
        maxRetries: args.maxRetries,
        delay: args.delayBetweenRetries,
        shouldRetry: (error) => {
          if (error instanceof JsonRpcError) {
            // retry on 429
            if (error.cause.responseStatusCode === 429) {
              return true;
            }
            // do not retry on all other Json-RPC errors
            return false;
          }

          // retry on other errors, e.g. network errors
          return true;
        },
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
  };
}

export function createRpcClient(args: {
  logger: Logger;
  url: string;
  fetch?: typeof globalThis.fetch;
  concurrency?: number;
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
