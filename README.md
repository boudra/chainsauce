<h1 align="center">
<strong>Chainsauce 💃</strong>
</h1>
<p align="center">
<strong>General-purpose Ethereum blockchain indexing library.</strong>
</p>

-------

![main check](https://github.com/boudra/chainsauce/actions/workflows/check.yml/badge.svg?branch=main)

Chainsauce is a general-purpose Ethereum indexer that sources contract events to build easily queryable data.

## How to use?

Install the package:

```bash
$ npm install boudra/chainsauce#main
```

Example:

```ts
import { createIndexer} from "chainsauce";
import { erc20ABI } from "./erc20ABI";

const MyContracts = {
  ERC20: erc20ABI,
};

const indexer = createIndexer({
  chain: {
    name: "mainnet",
    id: 1,
    rpc: {
      url: "https://mainnet.infura.io/v3/...",
    },
  },
  contracts: MyContracts,
});

indexer.on("ERC20:Transfer", async ({ event }) => {
  console.log("Transfer event:", event.params);
});

// Subscribe to deployed contracts
indexer.subscribeToContract({
  contract: "ERC20",
  address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
});

// this will index to latest and return
await indexer.indexToBlock("latest");
// or index to a specific block number
// await indexer.indexToBlock(16000000n);
// or, this will index to latest and watch for new blocks
// await indexer.watch();

```

## Complete examples

- [Allo Protocol Indexer](https://github.com/gitcoinco/allo-indexer)
