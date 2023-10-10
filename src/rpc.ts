import { retry } from "@/retry";
import { Logger } from "@/logger";
import { Hex, ToBlock } from "@/types";
import fastq from "fastq";

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

export class JsonRpcError extends Error {
  cause: unknown;
  error?: { message: string; code: number; data: unknown };
  url: string;
  method: string;
  params: unknown;
  constructor(
    url: string,
    method: string,
    params: unknown,
    cause: unknown,
    error?: { message: string; code: number; data: unknown }
  ) {
    super(
      `JsonRpcError: ${JSON.stringify({
        url,
        method,
        params,
        error,
      })}`
    );
    this.url = url;
    this.error = error;
    this.method = method;
    this.params = params;
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

export function createRpcClient(args: {
  logger: Logger;
  url: string;
  fetch?: typeof globalThis.fetch;
  concurrency?: number;
}): RpcClient {
  const { url } = args;

  const fetch = args.fetch ?? globalThis.fetch;
  const concurrency = args.concurrency ?? 5;

  const queue = fastq.promise(
    async (task: { method: string; params: unknown }) => {
      return rpcCall(task.method, task.params);
    },
    concurrency
  );

  function queueRpcCall<T>(method: string, params: unknown): Promise<T> {
    return queue.push({ method, params }) as Promise<T>;
  }

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
      throw new JsonRpcError(url, method, params, response);
    }

    const contentType = response.headers.get("Content-Type");

    if (!contentType || !contentType.includes("application/json")) {
      const body = await response.text();
      throw new Error(`Invalid response: ${body}`);
    }

    const json = await response.json();

    if ("error" in json) {
      throw new JsonRpcError(url, method, params, response, json.error);
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
      return retry(
        async () => {
          const toBlock =
            opts.toBlock === "latest"
              ? opts.toBlock
              : `0x${opts.toBlock.toString(16)}`;

          try {
            return await queueRpcCall<Log[]>("eth_getLogs", [
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
              (e.error?.message?.includes("query returned more than") ||
                e.error?.message?.includes("Log response size exceeded"))
            ) {
              throw new JsonRpcRangeTooWideError(e);
            } else {
              throw e;
            }
          }
        },
        {
          maxRetries: 5,
          shouldRetry: (error) => {
            // do not retry when the range is too wide
            if (error instanceof JsonRpcRangeTooWideError) {
              return false;
            }

            return true;
          },
          delay: 1000,
        }
      );
    },

    async getLastBlockNumber(): Promise<bigint> {
      return retry(
        async () => {
          const response = await queueRpcCall<string>("eth_blockNumber", []);

          return BigInt(response);
        },
        {
          maxRetries: 5,
          delay: 1000,
        }
      );
    },

    async readContract(args: {
      functionName: string;
      address: Hex;
      data: Hex;
      blockNumber: bigint;
    }): Promise<Hex> {
      return retry(
        async () => {
          const blockNumber = `0x${args.blockNumber.toString(16)}`;

          return await queueRpcCall<Hex>("eth_call", [
            {
              to: args.address,
              data: args.data,
            },
            blockNumber,
          ]);
        },
        {
          maxRetries: 5,
          delay: 1000,
        }
      );
    },
  };
}
