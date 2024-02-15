<h1 align="center">
<strong>Chainsauce 💃</strong>
</h1>
<p align="center">
<strong>General-purpose Ethereum blockchain indexing library.</strong>
</p>

-------

![main check](https://github.com/boudra/chainsauce/actions/workflows/check.yml/badge.svg?branch=main)

Chainsauce is a general-purpose Ethereum indexer that sources contract events from a JSON-RPC endpoint.

## Installation

```bash
$ npm install boudra/chainsauce#main
```

## Basic usage


```ts
import { createIndexer, createHttpRpcClient } from "chainsauce";
import { erc20ABI } from "./erc20ABI.ts";

// -- Define contracts
const MyContracts = {
  ERC20: erc20ABI,
};

// -- Create an indexer:

const indexer = createIndexer({
  chain: {
    id: 1,
    rpcClient: createHttpRpcClient({
      url: "https://mainnet.infura.io/v3/...",
    }),
  },
  contracts: MyContracts,
});

// -- Attach event listeners:

// subscribe to a specific event
indexer.on("ERC20:Approval", async ({ event }) => {
  console.log("Approval event:", event.params);
});

// subscribe to all events
indexer.on("event", async ({ event }) => {
  console.log("Event:", event.params);
});

// -- Subscribe to deployed contracts:

indexer.subscribeToContract({
  contract: "ERC20",
  address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",

  // optional
  fromBlock: 18363594n,
  toBlock: "latest"
});

// -- One off indexing:

// one off indexing, this will resolve when finished or reject if any error happens
await indexer.indexToBlock("latest");

// -- Continous indexing:

// indexes to the latest block and watches the chain for new events
// until stopped with `indexer.stop()`
// errors will be emitted and will not stop indexing
indexer.on("error", (error) => {
   console.error("whoops", error);
});
indexer.watch();
```

## Event handler types

Event handlers should be automatically inferred when used like this:

```ts
indexer.on("ERC20:Approval", async ({ event, context }) => {
  // event is inferred to be an Approval event
  // context is inferred to be the context passed into `createIndexer`
});
```

But if you need to split out event handler function to other files, you can type them like this;

```ts
import { Indexer as ChainsauceIndexer } from "chainsauce";

type MyContext = {
  db: DatabaseConnection
};

const MyContracts = {
  ERC20: erc20ABI,
};

type Indexer = ChainsauceIndexer<typeof MyContracts, MyContext>;

const indexer: Indexer = createIndexer({
   ...
   context: { db: new DatabaseConnection() }
});

async function handleTransfer({
  event, context: { db }
}: EventHandlerArgs<Indexer, "ERC20", "Transfer">) {
  // event is a Transfer event
  // context is a MyContext type
}

indexer.on("ERC20:Transfer", handleTransfer);
```

## How to define ABIs

ABIs in Chainsauce are of type type [Abi in abitype](https://abitype.dev/api/types#abi).

ABIs must be defined in Typescript if you want automatic typing of events and contract reads, make sure you cast ABIs `as const`:

```ts
const myAbi = [
  ...
] as const;
```

## Factory Contracts

You can subscribe to new deployed contracts in your event handlers by using the function `subscribeToContract`:

```ts
indexer.on("FactoryContract:ContractCreated", async ({ event, context, subscribeToContract }) => {
  subscribeToContract({
    contract: "MyContract",
    address: event.params.contractAddress,

    // optional
    toBlock: "latest"
  });
});
```

## Caching events and contract reads

Chainsauce comes with a cache to speed up reindexing, all you have to do is pass a cache when creating the indexer.

Currently only a SQLite version is available, but other options are planned.

```ts
import { createIndexer, createSqliteCache } from "chainsauce";

const indexer = createIndexer({
  cache: createSqliteCache("./chainsauce.db"),
});
```

## Complete examples

- [https://github.com/gitcoinco/grants-stack-indexer](https://github.com/gitcoinco/grants-stack-indexer)
