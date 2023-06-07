import { ethers } from "ethers";

function wait(delay: number) {
  return new Promise((resolve) => setTimeout(resolve, delay));
}

export class RetryProvider extends ethers.providers.StaticJsonRpcProvider {
  public attempts: number;
  public requests: [];
  public currentRequests = 0;
  public maxConcurrentRequests = 0;
  public requestCount = 0;

  constructor(
    url?: ethers.utils.ConnectionInfo | string,
    attempts = 5,
    maxConcurrentRequests = 20
  ) {
    super(url);
    this.attempts = attempts ?? 5;
    this.requests = [];
    this.maxConcurrentRequests = maxConcurrentRequests;
  }

  public async perform(method: string, params: unknown) {
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
