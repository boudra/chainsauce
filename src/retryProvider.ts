import { ethers } from "ethers";

export class RetryProvider extends ethers.providers.StaticJsonRpcProvider {
  public attempts: number;

  constructor(url?: ethers.utils.ConnectionInfo | string, attempts?: number) {
    super(url);
    this.attempts = attempts ?? 5;
  }

  public perform(method: string, params: unknown) {
    let attempts = 0;
    return ethers.utils.poll(
      async () => {
        attempts++;
        return super.perform(method, params).then(
          (result) => {
            return result;
          },
          (error: { statusCode: number }) => {
            console.error("FAILED", error);
            if (error.statusCode !== 429 || attempts >= this.attempts) {
              return Promise.reject(error);
            } else {
              return Promise.resolve(undefined);
            }
          }
        );
      },
      { interval: 500 }
    );
  }
}
