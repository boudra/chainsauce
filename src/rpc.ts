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
}

export function createRpcClient(
  _logger: Logger,
  rpcUrl: string,
  fetch: typeof globalThis.fetch
): RpcClient {
  async function rpcCall<T>(method: string, params: unknown): Promise<T> {
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method,
      params: params,
    });

    const response = await fetch(rpcUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body,
    });

    if (response.status !== 200) {
      throw new JsonRpcError(rpcUrl, method, params, response);
    }

    const contentType = response.headers.get("Content-Type");

    if (!contentType || !contentType.includes("application/json")) {
      const body = await response.text();
      throw new Error(`Invalid response: ${body}`);
    }

    const json = await response.json();

    if ("error" in json) {
      throw new JsonRpcError(rpcUrl, method, params, response, json.error);
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
            return await rpcCall<Log[]>("eth_getLogs", [
              {
                address: opts.address,
                topics: opts.topics,
                fromBlock: `0x${opts.fromBlock.toString(16)}`,
                toBlock: toBlock,
              },
            ]);
          } catch (e) {
            // handle range too wide errors
            if (
              e instanceof JsonRpcError &&
              e.error?.message?.includes("query returned more than")
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
          onRetry: () => {
            return;
          },
          delay: 1000,
        }
      );
    },

    async getLastBlockNumber(): Promise<bigint> {
      return retry(
        async () => {
          const response = await rpcCall<string>("eth_blockNumber", []);

          return BigInt(response);
        },
        {
          maxRetries: 5,
          onRetry: () => {
            return;
          },
          delay: 1000,
        }
      );
    },
  };
}
