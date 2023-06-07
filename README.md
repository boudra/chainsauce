<h1 align="center">
<strong>Chainsauce 💃</strong>
</h1>
<p align="center">
<strong>Source EVM events into a database for easy querying</strong>
</p>

-------

Fork of [Chainsauce] that's client side only using IndexedDB for caching.

Chainsauce is a general-purpose EVM indexer that sources contract events to build easily queryable data. It works in the browser.

## How does it work?

The indexer uses JSON-RPC to fetch all the events for your contracts from a node, then calls your supplied reducer function for each event, which should build the database.

## How to use?

Install the package:

```bash
$ npm install chainsauce
```

Example:

```ts
import ethers from "ethers";
import {createIndex, JsonStorage, Event} from "chainsauce";

import MyContractABI from "./abis/MyContract.json" assert { type: "json" };

async function handleEvent(indexer: Indexer<JsonStorage>, event: Event) {
  const db = indexer.storage;
  
  switch (event.name) {
    case "UserCreated":
      db.collection("users").insert({
        id: event.args.id,
        name: event.args.name
      });
      break;
      
    case "UserUpdated":
      db.collection("users").updateById(event.args.id, {
        name: event.args.name
      });
      break;
  }
}

const provider = new ethers.providers.JsonRpcProvider("http://mynode.com");
const storage = new JsonStorage("./data");
const indexer = await createIndexer(provider, storage, handleEvent);

// Susbscribe to events with the contract address and ABI
indexer.subscribe("0x1234567890", MyContractABI);
```

## Complete examples

- [Allo Protocol Indexer](https://github.com/gitcoinco/allo-indexer)

## Why event sourcing? 🤔

- The database can be rebuilt any time only from the logs
- Reuse the exact same codebase to build queryable databases for any chain
- Easily testable, it's just a single function ✨
- Super fast database rebuilds with cached events ⚡️

## Persistence options

- **JSON**: Use this if your data fits in memory and you don't want the complexity of an actual database. You can serve your JSON data through a static HTTP server, a custom REST API, as a [GraphQL API](https://github.com/marmelab/json-graphql-server) or even pin it to IPFS for front-end usage. Serve behind a CDN for even better performance.
- **SQLite**: This is a great alternative if you still don't want the complexity of a server database. It will give you all the niceties of SQL and you'll be able to serve the database over IPFS for people to use. Query from the front end using: [sql.js with an HTTP VFS](https://github.com/phiresky/sql.js-httpvfs)
- **Bring your own storage**: You can easily store your data elsewhere by implementing the `Storage` interface. You can for example store your data to PostgreSQL with Prisma, MongoDB or anything else you like.

## Limitations

- Because the indexer uses JSON-RPC to fetch logs, it relies on the provider's ability to filter and return blockchain events, some providers limit the amount of events that can be returned in one call. The indexer gets around this by fetching smaller block ranges. It's best to use your own node if you encounter issues, but **for most people it should be fine**.
