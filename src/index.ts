import { ethers } from "ethers";

import Cache from "./cache.js";
import debounce from "./debounce.js";
import { RetryProvider } from "./retryProvider.js";

export { RetryProvider } from "./retryProvider.js";

export { default as IdbStorage } from "./storage/idb.js";
export { default as SqliteStorage } from "./storage/sqlite.js";

export { Cache };

/**
 * An interface representing a raw event emitted by a contract.
 */
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

/**
 * An interface representing a parsed event emitted by a contract.
 */
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

/**
 * A function that handles an event emitted by a contract.
 */
export type EventHandler<T extends Storage> = (
  indexer: Indexer<T>,
  event: Event
) => Promise<void | (() => Promise<void>)>;

/**
 * An interface representing a subscription to events emitted by a contract.
 */
export type Subscription = {
  address: string;
  contract: ethers.Contract;
  fromBlock: number;
};

/**
 * An interface representing a storage mechanism for subscriptions.
 */
export interface Storage {
  /**
   * Gets the subscriptions stored in the storage mechanism.
   * @returns {Promise<Subscription[]>} A promise that resolves to an array of subscriptions.
   */
  getSubscriptions(): Promise<Subscription[]>;
  /**
   * Sets the subscriptions stored in the storage mechanism.
   * @param {Subscription[]} subscriptions - The subscriptions to set.
   * @returns {Promise<void>} A promise that resolves when the subscriptions have been set.
   */
  setSubscriptions(subscriptions: Subscription[]): Promise<void>;

  /**
   * Initializes the storage mechanism.
   * @returns {Promise<void>} A promise that resolves when the storage mechanism has been initialized.
   */
  init?(): Promise<void>;
  /**
   * Writes data to the storage mechanism.
   * @returns {Promise<void>} A promise that resolves when the data has been written.
   */
  write?(): Promise<void>;
  /**
   * Reads data from the storage mechanism.
   * @returns {Promise<void>} A promise that resolves when the data has been read.
   */
  read?(): Promise<void>;
}

/**
 * An enum representing the log levels.
 */
export enum Log {
  None = 0,
  Debug,
  Info,
  Warning,
  Error,
}

/**
 * A type representing the block number to subscribe to.
 */
export type ToBlock = "latest" | number;

/**
 * An interface representing the options for the indexer.
 */
export type Options = {
  pollingInterval: number;
  logLevel: Log;
  getLogsMaxRetries: number;
  getLogsContractChunkSize: number;
  enableCache: boolean;
  toBlock: ToBlock;
  runOnce: boolean;
};

/**
 * The default options for the indexer.
 */
export const defaultOptions: Options = {
  pollingInterval: 20 * 1000,
  logLevel: Log.Info,
  getLogsMaxRetries: 2,
  getLogsContractChunkSize: 25,
  enableCache: true,
  toBlock: "latest",
  runOnce: false,
};

/**
 * The Indexer class for subscribing to events emitted by contracts.
 * @template T - The type of the storage mechanism for subscriptions.
 */
export class Indexer<T extends Storage> {
  /**
   * An array of subscriptions to contracts to listen for events on.
   */
  subscriptions: Subscription[];

  /**
   * The ID of the blockchain network.
   */
  chainId: number;

  /**
   * The name of the blockchain network.
   */
  chainName: string;

  /**
   * The ethers.js provider instance to use for querying the blockchain.
   */
  provider: Provider;

  /**
   * A function to handle events emitted by contracts.
   */
  eventHandler: EventHandler<T>;

  /**
   * A debounced function to update the indexer.
   */
  update: () => void;

  /**
   * A debounced function to write data to the storage mechanism.
   */
  writeToStorage: () => void;

  /**
   * An instance of a storage mechanism for subscriptions.
   */
  storage: T;

  /**
   * The last block number processed by the indexer.
   */
  lastBlock = 0;

  /**
   * The current block number being indexed by the indexer.
   */
  currentIndexedBlock = 0;

  /**
   * A flag indicating whether the indexer is currently updating.
   */
  isUpdating = false;

  /**
   * An object containing options for the indexer.
   */
  options: Options;

  /**
   * An instance of a cache for storing event data.
   */
  cache: Cache;

  /**
   * The ID of the polling timer.
   */
  pollingTimer: ReturnType<typeof setInterval>;

  /**
   * The constructor for the Indexer class.
   * @constructor
   * @param {Provider} provider - The ethers.js provider instance to use for querying the blockchain.
   * @param {ethers.providers.Network} network - The network to use for querying the blockchain.
   * @param {Subscription[]} subscriptions - An array of subscriptions to contracts to listen for events on.
   * @param {T} persistence - An instance of a storage mechanism for subscriptions.
   * @param {EventHandler<T>} handleEvent - A function to handle events emitted by contracts.
   * @param {Partial<Options>} options - An object containing options for the indexer.
   */
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
    this.cache = new Cache(options.enableCache);

    console.log("storage", this.storage);
    console.log("subscriptions", this.subscriptions);

    // Set the update property to a debounced function that calls the _update method of the class.
    this.update = debounce(() => this._update(), 5000);

    // Set the writeToStorage property to a debounced function that writes data to the storage mechanism, if the storage mechanism has a write method.
    this.writeToStorage = debounce(() => {
      if (this.storage.write) {
        this.storage.write();
      }
    }, 5000);

    // If the subscriptions array has a length greater than 0, call the update method.
    if (this.subscriptions.length > 0) {
      this.update();
    }

    // Set the pollingTimer property to the ID of a setInterval call that calls the _update method of the class at the interval specified by the pollingInterval property of the options parameter.
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

  /**
   * A method for logging messages to the console.
   * @private
   * @param {Log} level - The log level of the message.
   * @param {...unknown[]} data - The data to log.
   * @returns {void}
   */
  private log(level: Log, ...data: unknown[]): void {
    // If the log level is less than the log level specified in the options, return.
    if (level < this.options.logLevel) {
      return;
    }

    // Log the message to the console based on the log level.
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

  /**
   * A method to update the indexer with new events.
   * @private
   * @returns {Promise<void>}
   */
  private async _update(): Promise<void> {
    // If the indexer is already updating, return.
    if (this.isUpdating) return;

    this.isUpdating = true;

    try {
      // If the toBlock property of the options object is "latest", set the lastBlock property to the current block number of the provider. Otherwise, set the lastBlock property to the value of the toBlock property.
      if (this.options.toBlock === "latest") {
        this.lastBlock = await this.provider.getBlockNumber();
      } else {
        this.lastBlock = this.options.toBlock;
      }

      // Initialize an empty array to hold pending events.
      let pendingEvents: Event[] = [];

      // Loop until there are no more outdated subscriptions or pending events.
      // eslint-disable-next-line no-constant-condition
      while (true) {
        // Get the number of subscriptions and an array of outdated subscriptions.
        const subscriptionCount = this.subscriptions.length;
        const outdatedSubscriptions = this.subscriptions.filter((sub) => {
          return sub.fromBlock <= this.lastBlock;
        });

        // If there are no more outdated subscriptions or pending events, break out of the loop.
        if (outdatedSubscriptions.length === 0 && pendingEvents.length === 0) {
          break;
        }

        // If there are outdated subscriptions, log a debug message indicating the number of subscriptions and the last block being queried.
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

        // Group the outdated subscriptions by their fromBlock property.
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

                /**
                 * Creates an index of event contracts and their event fragments.
                 * @param {SubscriptionBatch[]} subscriptionBatch - An array of subscription batches to index.
                 * @returns {Record<string, { eventFragment: EventFragment; contract: ethers.Contract }>} An object containing the indexed event contracts and their event fragments.
                 */
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

                /**
                 * Parses event logs into a standardized format.
                 * @param {RawEvent[]} eventLogs - An array of raw event logs to parse.
                 * @param {Record<string, { eventFragment: EventFragment; contract: ethers.Contract }>} eventContractIndex - An object containing the indexed event contracts and their event fragments.
                 * @returns {Event[]} An array of parsed events.
                 */
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
          // Get the next pending event and remove it from the array.
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          const event = pendingEvents.shift()!;

          try {
            // Call the event handler for the event.
            const ret = await this.eventHandler(this, event);

            // handle thunk
            if (typeof ret === "function") {
              ret()
                .then()
                .catch((e) => {
                  this.log(Log.Error, "Failed to apply event", event);
                  this.log(Log.Error, e);
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

  /**
   * Subscribes to events emitted by a contract at a given address.
   * @param {string} address - The address of the contract to subscribe to.
   * @param {ethers.ContractInterface} abi - The ABI of the contract to subscribe to.
   * @param {number} fromBlock - The block number to start fetching logs from.
   * @returns {ethers.Contract} A contract instance that can be used to interact with the contract.
   */
  subscribe(
    address: string,
    abi: ethers.ContractInterface,
    fromBlock = 0
  ): ethers.Contract {
    // Check if a subscription for this address already exists, and return the existing contract if it does.
    const existing = this.subscriptions.find((s) => s.address === address);

    if (existing) {
      return existing.contract;
    }

    let provider = this.provider;

    this.log(Log.Info, "Subscribing", provider);

    // Create a new contract instance and add it to the subscriptions array.
    const contract = new ethers.Contract(address, abi);

    fromBlock = Math.max(this.currentIndexedBlock, fromBlock);

    this.log(Log.Info, "Subscribed", contract.address, "from block", fromBlock);

    this.subscriptions.push({
      address: address,
      contract,
      fromBlock: fromBlock,
    });

    this.log(Log.Info, "Contract", contract);

    // Update the indexer with the new subscription.
    this.update();

    return contract;
  }

  /**
   * A private method for fetching logs from the blockchain.
   * @private
   * @param {number} fromBlock - The block number to start fetching logs from.
   * @param {number} toBlock - The block number to stop fetching logs at.
   * @param {string[]} address - An array of contract addresses to filter logs by.
   * @returns {Promise<RawEvent[]>} A promise that resolves to an array of raw events.
   */
  private async fetchLogs(
    fromBlock: number,
    toBlock: number,
    address: string[]
  ): Promise<RawEvent[]> {
    // Construct a cache key based on the chain ID, from block, and contract addresses.
    const cacheKey = `${
      this.provider.network.chainId
    }-${fromBlock}-${address.join("")}`;

    // Check if the logs are already cached, and return them if they are.
    const cached = await this.cache.get<{
      toBlock: number;
      events: RawEvent[];
    }>(cacheKey);

    if (cached) {
      const { toBlock: cachedToBlock, events } = cached;

      // If the requested toBlock is greater than the cached toBlock, fetch new logs and merge them with the cached logs.
      if (toBlock > cachedToBlock) {
        const newEvents = await this._fetchLogs(
          cachedToBlock + 1,
          toBlock,
          address
        );

        const allEvents = events.concat(newEvents);

        // Update the cache with the new logs.
        this.cache.set(
          cacheKey,
          JSON.stringify({ toBlock, events: allEvents })
        );

        return allEvents;
      }

      // If the requested toBlock is less than or equal to the cached toBlock, return the cached logs.
      return cached.events;
    }

    // If the logs are not cached, fetch them and cache them.
    const events = await this._fetchLogs(fromBlock, toBlock, address);

    this.cache.set(cacheKey, JSON.stringify({ toBlock, events }));

    return events;
  }

  /**
   * A private method for fetching logs from the blockchain.
   * @private
   * @param {number} fromBlock - The block number to start fetching logs from.
   * @param {number} toBlock - The block number to stop fetching logs at.
   * @param {string[]} address - An array of contract addresses to filter logs by.
   * @param {number} depth - The current depth of the recursive call.
   * @returns {Promise<RawEvent[]>} A promise that resolves to an array of raw events.
   * @throws {Error} Throws an error if the eth_getLogs call fails.
   */
  private async _fetchLogs(
    fromBlock: number,
    toBlock: number,
    address: string[],
    depth: number = 0
  ): Promise<RawEvent[]> {
    try {
      // We don't use the Provider to get logs because it's
      // too slow for calls that return thousands of events
      // const url = this.provider.connection.url;

      // const url = "https://rpc.gnosis.gateway.fm";

      // Construct the JSON-RPC request body for the eth_getLogs call.
      const body = {
        jsonprc: "2.0",
        id: "1",
        method: "eth_getLogs",
        params: [
          {
            fromBlock: fromBlock,
            toBlock: toBlock,
            address: address,
          },
        ],
      };

      //TODO dynamic provider URL
      // Send the JSON-RPC request to the blockchain node.
      const response = await fetch("https://rpc.gnosischain.com", {
        headers: {
          "content-type": "application/json",
        },
        method: "POST",
        body: JSON.stringify(body),
      });

      // Parse the response body as JSON.
      const responseBody = (await response.json()) as {
        error?: { code: number; message: string };
        result?: RawEvent[];
      };

      // If the response body contains an error, throw an error.
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

      // If the response body contains a result, return it.
      if (responseBody.result) {
        return responseBody.result;
      }

      // If the response body is unexpected, throw an error.
      throw new Error(
        `eth_getLogs failed, unexpected response: ${JSON.stringify(
          responseBody
        )}`
      );
    } catch (e) {
      // If the eth_getLogs call fails, log an error and retry with a smaller range.
      this.log(
        Log.Debug,
        "Failed range:",
        fromBlock,
        "to",
        toBlock,
        "retrying smaller range ...",
        e
      );

      // If we've reached the maximum number of retries, throw an error.
      if (depth === this.options.getLogsMaxRetries) {
        throw e;
      }

      // Split the range into smaller chunks and recursively fetch logs for each chunk.
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

/**
 * A function to create an instance of the Indexer class.
 * @param {Provider} provider - The provider to use for interacting with the blockchain.
 * @param {Storage} database - The storage mechanism to use for persisting data.
 * @param {EventHandler<Storage>} handleEvent - The event handler to use for processing events.
 * @param {Partial<Options>} options - An optional object containing options for the indexer.
 * @returns {Promise<Indexer<Storage>>} A promise that resolves to an instance of the Indexer class.
 * @throws {Error} Throws an error if the storage mechanism does not implement the init method.
 */
export async function createIndexer<T extends Storage>(
  provider: Provider,
  database: T,
  handleEvent: EventHandler<T>,
  options?: Partial<Options>
): Promise<Indexer<T>> {
  // Check if the storage mechanism implements the init method, and throw an error if it doesn't.
  if (!database.init) {
    throw new Error("Storage must implement init");
  }

  // Call the init method of the storage mechanism.
  await database.init();

  // Get the subscriptions from the storage mechanism.
  const subscriptions = await database.getSubscriptions();

  // Get the network from the provider.
  const network = await provider.getNetwork();

  // Create a new instance of the Indexer class with the provider, network, subscriptions, storage mechanism, event handler, and options.
  return new Indexer(
    provider,
    network,
    subscriptions,
    database,
    handleEvent,
    options ?? {}
  );
}
