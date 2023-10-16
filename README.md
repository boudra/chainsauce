<h1 align="center">
<strong>Chainsauce 💃</strong>
</h1>
<p align="center">
<strong>General-purpose Ethereum blockchain indexing library.</strong>
</p>

-------

![main check](https://github.com/boudra/chainsauce/actions/workflows/check.yml/badge.svg?branch=main)

Chainsauce is a general-purpose Ethereum indexer that sources contract events to build easily queryable data.

## Installation

```bash
$ npm install boudra/chainsauce#main
```

## Basic usage

Create an indexer:

```ts
const MyContracts = {
  ERC20: erc20ABI,
};

const indexer = createIndexer({
  chain: {
    id: 1,
    rpcClient: createHttpRpcClient({
      url: "https://mainnet.infura.io/v3/...",
    }),
  },
  contracts: MyContracts,
});
```

Subscribe to deployed contracts:

```ts
indexer.subscribeToContract({
  contract: "ERC20",
  address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",

  // optional
  fromBlock: 18363594n,
  toBlock: "latest"
});
```

Subscribe to events:

```ts
// subscribe to a specific event
indexer.on("ERC20:Approval", async ({ event }) => {
  console.log("Approval event:", event.params);
});

// subscribe to all events
indexer.on("events", async ({ event }) => {
  console.log("Approval event:", event.params);
});
```

Type an event handler:

```ts
import { Indexer as ChainsauceIndexer } from "chainsauce";

type MyContext = {
  db: DatabaseConnection
};

type Indexer = ChainsauceIndexer<typeof MyContracts, MyContext>;

async function handleTransfer({
  event, context: { db }
}: EventHandlerArgs<Indexer, "ERC20", "Transfer">) {
  // db is a DatabaseConnection
  console.log("Transfer event:", event.params);
}

indexer.on("ERC20:Transfer", handleTransfer);
```

## Complete examples

- [Allo Protocol Indexer](https://github.com/gitcoinco/allo-indexer)
