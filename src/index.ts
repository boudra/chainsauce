import { ethers } from "ethers";

import Cache from "./cache.js";
import debounce from "./debounce.js";
import fetch from "node-fetch";
import { RetryProvider } from "./retryProvider.js";

export { RetryProvider } from "./retryProvider.js";

export { default as JsonStorage } from "./storage/json.js";
export { default as SqliteStorage } from "./storage/sqlite.js";

export { Cache };

export type RawEvent = {
  address: string;
  blockHash: string;
  blockNumber: string;
  data: string;
  logIndex: string;
  topics: string[];
  transactionHash: string;
  transactionIndex: string;
};

export type Event = {
  name: string;
  signature: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  args: { [key: string]: any };
  address: string;
  transactionHash: string;
  blockNumber: number;
  logIndex: number;
};

export type EventHandler<T extends Storage> = (
  indexer: Indexer<T>,
  event: Event
) => Promise<void | (() => Promise<void>)>;

export type Subscription = {
  address: string;
  contract: ethers.Contract;
  fromBlock: number;
};

export interface Storage {
  getSubscriptions(): Promise<Subscription[]>;
  setSubscriptions(subscriptions: Subscription[]): Promise<void>;

  init?(): Promise<void>;
  write?(): Promise<void>;
  read?(): Promise<void>;
}

export enum Log {
  None = 0,
  Debug,
  Info,
  Warning,
  Error,
}

export type ToBlock = "latest" | number;

export type Options = {
  pollingInterval: number;
  logLevel: Log;
  getLogsMaxRetries: number;
  getLogsContractChunkSize: number;
  eventCacheDirectory: string | null;
  toBlock: ToBlock;
  runOnce: boolean;
};

export const defaultOptions: Options = {
  pollingInterval: 20 * 1000,
  logLevel: Log.Info,
  getLogsMaxRetries: 20,
  getLogsContractChunkSize: 25,
  eventCacheDirectory: "./.cache",
  toBlock: "latest",
  runOnce: false,
};

export class Indexer<T extends Storage> {
  subscriptions: Subscription[];
  chainId: number;
  chainName: string;
  provider: Provider;
  eventHandler: EventHandler<T>;
  update: () => void;
  writeToStorage: () => void;
  storage: T;

  lastBlock = 0;
  currentIndexedBlock = 0;
  isUpdating = false;
  options: Options;
  cache: Cache;
  pollingTimer: ReturnType<typeof setInterval>;

  constructor(
    provider: Provider,
    network: ethers.providers.Network,
    subscriptions: Subscription[],
    persistence: T,
    handleEvent: EventHandler<T>,
    options: Partial<Options>
  ) {
    this.chainId = network.chainId;
    this.chainName = network.name;
    this.provider = provider;
    this.eventHandler = handleEvent;
    this.subscriptions = subscriptions;
    this.storage = persistence;
    this.options = Object.assign(defaultOptions, options);

    if (this.options.eventCacheDirectory) {
      this.cache = new Cache(this.options.eventCacheDirectory);
    } else {
      this.cache = new Cache("", true);
    }

    this.update = debounce(() => this._update(), 500);
    this.writeToStorage = debounce(() => {
      if (this.storage.write) {
        this.storage.write();
      }
    }, 500);

    if (this.subscriptions.length > 0) {
      this.update();
    }

    this.pollingTimer = setInterval(
      () => this._update(),
      this.options.pollingInterval
    );

    this.log(
      Log.Info,
      "Initialized indexer with",
      subscriptions.length,
      "contract subscriptions"
    );
  }

  private log(level: Log, ...data: unknown[]) {
    if (level < this.options.logLevel) {
      return;
    }

    if (level === Log.Warning) {
      console.warn(`[${this.chainName}][warn]`, ...data);
    } else if (level === Log.Error) {
      console.error(`[${this.chainName}][error]`, ...data);
    } else if (level === Log.Debug) {
      console.debug(`[${this.chainName}][debug]`, ...data);
    } else {
      console.log(`[${this.chainName}][info]`, ...data);
    }
  }

  private async _update() {
    if (this.isUpdating) return;

    this.isUpdating = true;

    try {
      if (this.options.toBlock === "latest") {
        this.lastBlock = await this.provider.getBlockNumber();
      } else {
        this.lastBlock = this.options.toBlock;
      }

      let pendingEvents: Event[] = [];

      // eslint-disable-next-line no-constant-condition
      while (true) {
        const subscriptionCount = this.subscriptions.length;
        const outdatedSubscriptions = this.subscriptions.filter((sub) => {
          return sub.fromBlock <= this.lastBlock;
        });

        if (outdatedSubscriptions.length === 0 && pendingEvents.length === 0) {
          break;
        }

        if (outdatedSubscriptions.length > 0) {
          this.log(
            Log.Debug,
            "Fetching events for",
            outdatedSubscriptions.length,
            "subscriptions",
            "to block",
            this.lastBlock
          );
        }

        const subscriptionBatches = outdatedSubscriptions.reduce(
          (acc: { [key: string]: Subscription[][] }, sub) => {
            acc[sub.fromBlock] ||= [[]];

            const last = acc[sub.fromBlock][acc[sub.fromBlock].length - 1];

            if (last.length > this.options.getLogsContractChunkSize) {
              acc[sub.fromBlock].push([sub]);
            } else {
              last.push(sub);
            }
            return acc;
          },
          {}
        );

        const eventBatches = Promise.all(
          Object.entries(subscriptionBatches).flatMap(
            ([fromBlock, subscriptionBatches]) => {
              return subscriptionBatches.map(async (subscriptionBatch) => {
                const addresses = subscriptionBatch.map((s) => s.address);

                const eventContractIndex = Object.fromEntries(
                  subscriptionBatch.flatMap(({ contract }) => {
                    return Object.keys(contract.interface.events).map(
                      (name) => {
                        return [
                          contract.interface.getEventTopic(name),
                          {
                            eventFragment: contract.interface.getEvent(name),
                            contract,
                          },
                        ];
                      }
                    );
                  })
                );

                const from = Number(fromBlock);
                const to = this.lastBlock;

                const eventLogs = await this.fetchLogs(from, to, addresses);

                if (eventLogs.length > 0) {
                  this.log(
                    Log.Debug,
                    "Fetched events (",
                    eventLogs.length,
                    ")",
                    "Range:",
                    from,
                    "to",
                    to
                  );
                }

                const parsedEvents = eventLogs.flatMap((log: RawEvent) => {
                  try {
                    const fragmentContract = eventContractIndex[log.topics[0]];

                    if (!fragmentContract) {
                      this.log(
                        Log.Warning,
                        "Unrecognized event",
                        "Address:",
                        log.address,
                        "TxHash:",
                        log.transactionHash,
                        "Topic:",
                        log.topics[0]
                      );

                      return [];
                    }

                    const { eventFragment, contract } = fragmentContract;

                    const args = contract.interface.decodeEventLog(
                      eventFragment,
                      log.data,
                      log.topics
                    );

                    const event: Event = {
                      name: eventFragment.name,
                      args: args,
                      address: ethers.utils.getAddress(log.address),
                      signature: eventFragment.format(),
                      transactionHash: log.transactionHash,
                      blockNumber: parseInt(log.blockNumber, 16),
                      logIndex: parseInt(log.logIndex, 16),
                    };

                    return [event];
                  } catch (e) {
                    this.log(
                      Log.Error,
                      "Failed to parse event",
                      log.address,
                      "Tx Hash:",
                      log.transactionHash,
                      "Topic:",
                      log.topics[0]
                    );

                    return [];
                  }
                });

                return parsedEvents;
              });
            }
          )
        );

        const events = (await eventBatches).flat();

        pendingEvents = pendingEvents.concat(events);

        pendingEvents.sort((a, b) => {
          return a.blockNumber - b.blockNumber || a.logIndex - b.logIndex;
        });

        let appliedEventCount = 0;

        while (pendingEvents.length > 0) {
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          const event = pendingEvents.shift()!;

          try {
            const ret = await this.eventHandler(this, event);

            // handle thunk
            if (typeof ret === "function") {
              ret().catch((e) => {
                this.log(Log.Error, "Failed to apply event", event);
                this.log(Log.Error, e);
                this.log(Log.Error, "Exiting...");
                process.exit(1);
              });
            }
          } catch (e) {
            this.log(Log.Error, "Failed to apply event", event);
            throw e;
          }

          appliedEventCount = appliedEventCount + 1;

          // If a new subscription is added, stop playing events and catch up
          if (this.subscriptions.length > subscriptionCount) {
            break;
          }
        }

        if (appliedEventCount > 0) {
          this.log(Log.Debug, "Applied", appliedEventCount, "events");
        }

        for (const subscription of outdatedSubscriptions) {
          subscription.fromBlock = this.lastBlock + 1;
        }

        this.storage.setSubscriptions(this.subscriptions);
        this.writeToStorage();
      }

      this.log(Log.Info, "Indexed up to", this.lastBlock);
      this.currentIndexedBlock = this.lastBlock + 1;

      this.storage.setSubscriptions(this.subscriptions);

      if (this.lastBlock === this.options.toBlock || this.options.runOnce) {
        clearInterval(this.pollingTimer);
      }
    } finally {
      this.isUpdating = false;
    }
  }

  subscribe(
    address: string,
    abi: ethers.ContractInterface,
    fromBlock = 0
  ): ethers.Contract {
    const existing = this.subscriptions.find((s) => s.address === address);

    if (existing) {
      return existing.contract;
    }

    const contract = new ethers.Contract(address, abi, this.provider);

    fromBlock = Math.max(this.currentIndexedBlock, fromBlock);

    this.log(Log.Info, "Subscribed", contract.address, "from block", fromBlock);

    this.subscriptions.push({
      address: address,
      contract,
      fromBlock: fromBlock,
    });

    this.update();

    return contract;
  }

  private async fetchLogs(
    fromBlock: number,
    toBlock: number,
    address: string[]
  ): Promise<RawEvent[]> {
    const cacheKey = `${
      this.provider.network.chainId
    }-${fromBlock}-${address.join("")}`;

    const cached = await this.cache.get<{
      toBlock: number;
      events: RawEvent[];
    }>(cacheKey);

    if (cached) {
      const { toBlock: cachedToBlock, events } = cached;

      if (toBlock > cachedToBlock) {
        const newEvents = await this._fetchLogs(
          cachedToBlock + 1,
          toBlock,
          address
        );

        const allEvents = events.concat(newEvents);

        this.cache.set(cacheKey, { toBlock, events: allEvents });

        return allEvents;
      }

      return cached.events;
    }

    const events = await this._fetchLogs(fromBlock, toBlock, address);

    this.cache.set(cacheKey, { toBlock, events });

    return events;
  }

  private async _fetchLogs(
    fromBlock: number,
    toBlock: number,
    address: string[],
    depth = 0
  ): Promise<RawEvent[]> {
    try {
      // We don't use the Provider to get logs because it's
      // too slow for calls that return thousands of events
      const url = this.provider.connection.url;

      const body = {
        jsonrpc: "2.0",
        id: "1",
        method: "eth_getLogs",
        params: [
          {
            fromBlock: ethers.utils.hexValue(fromBlock),
            toBlock: ethers.utils.hexValue(toBlock),
            address: address,
          },
        ],
      };

      const response = await fetch(url, {
        headers: {
          "content-type": "application/json",
        },
        method: "POST",
        body: JSON.stringify(body),
      });

      const responseBody = (await response.json()) as {
        error?: { code: number; message: string };
        result?: RawEvent[];
      };

      if (responseBody.error) {
        // back off if we get rate limited
        if (
          responseBody.error?.code == 429 &&
          depth < this.options.getLogsMaxRetries
        ) {
          await new Promise((r) => setTimeout(r, (depth + 1) * 1000));
        }

        throw new Error(
          `eth_getLogs failed, code: ${responseBody.error.code}, message: ${responseBody.error.message}`
        );
      }

      if (responseBody.result) {
        return responseBody.result;
      }

      throw new Error(
        `eth_getLogs failed, unexpected response: ${JSON.stringify(
          responseBody
        )}`
      );
    } catch (e) {
      this.log(
        Log.Debug,
        "Failed range:",
        fromBlock,
        "to",
        toBlock,
        "retrying smaller range ...",
        e
      );

      if (depth === this.options.getLogsMaxRetries) {
        throw e;
      }

      const chunks = [];
      const step = Math.ceil((toBlock - fromBlock) / Math.min(depth, 2));

      for (let i = fromBlock; i < toBlock; i += step + 1) {
        chunks.push([i, Math.min(i + step, toBlock)]);
      }

      return (
        await Promise.all(
          chunks.map(([from, to]) => {
            return this._fetchLogs(from, to, address, depth + 1);
          })
        )
      ).flat();
    }
  }
}

type Provider =
  | ethers.providers.JsonRpcProvider
  | ethers.providers.StaticJsonRpcProvider
  | RetryProvider;

export async function createIndexer<T extends Storage>(
  provider: Provider,
  database: T,
  handleEvent: EventHandler<T>,
  options?: Partial<Options>
): Promise<Indexer<T>> {
  if (database.read) {
    await database.read();
  }

  if (database.init) {
    await database.init();
  }

  const subscriptions = await database.getSubscriptions();
  const network = await provider.getNetwork();
  return new Indexer(
    provider,
    network,
    subscriptions,
    database,
    handleEvent,
    options ?? {}
  );
}
