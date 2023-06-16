import { ethers } from "ethers";

function wait(delay: number) {
  return new Promise((resolve) => setTimeout(resolve, delay));
}

/**
 * A provider that retries requests that fail with a 429 (Too Many Requests) error.
 * @extends ethers.providers.StaticJsonRpcProvider
 * @property {number} attempts - The number of times to retry a request that fails with a 429 error.
 * @property {unknown[]} requests - An array of pending requests.
 * @property {number} currentRequests - The number of requests currently being processed.
 * @property {number} maxConcurrentRequests - The maximum number of requests that can be processed concurrently.
 */
export class RetryProvider extends ethers.providers.StaticJsonRpcProvider {
  public attempts: number;
  public requests: [];
  public currentRequests = 0;
  public maxConcurrentRequests = 0;
  public requestCount = 0;

  /**
   * Creates a new RetryProvider instance.
   * @param {ethers.utils.ConnectionInfo | string} url - The URL of the JSON-RPC endpoint.
   * @param {number} attempts - The number of times to retry a request that fails with a 429 error.
   * @param {number} maxConcurrentRequests - The maximum number of requests that can be processed concurrently.
   */
  constructor(
    url?: ethers.utils.ConnectionInfo | string,
    attempts: number = 5,
    maxConcurrentRequests: number = 20
  ) {
    super(url);
    this.attempts = attempts ?? 5;
    this.requests = [];
    this.maxConcurrentRequests = maxConcurrentRequests;
  }

  /**
   * Performs a JSON-RPC request with retries.
   * @param {string} method - The JSON-RPC method to call.
   * @param {unknown} params - The parameters to pass to the JSON-RPC method.
   * @returns {Promise<unknown>} A promise that resolves to the JSON-RPC response.
   */
  public async perform(method: string, params: unknown): Promise<unknown> {
    while (this.currentRequests >= this.maxConcurrentRequests) {
      await wait(100);
    }

    this.currentRequests++;
    this.requestCount++;

    try {
      let attempts = 0;

      const response = await ethers.utils.poll(
        async () => {
          attempts++;

          return super.perform(method, params).then(
            (result) => {
              return result;
            },
            (error: { statusCode: number }) => {
              if (error.statusCode !== 429 && attempts >= this.attempts) {
                return Promise.reject(error);
              } else {
                return Promise.resolve(undefined);
              }
            }
          );
        },
        { interval: 500 }
      );
      return response;
    } finally {
      this.currentRequests -= 1;
    }
  }
}
