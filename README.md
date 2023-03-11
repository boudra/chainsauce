<h1 align="center">
<strong>Chainsauce 💃</strong>
</h1>
<p align="center">
<strong>Source EVM events into a database for easy querying</strong>
</p>

-------

Chainsauce is an EVM indexer that sources contract events to build easily queryable data.

## How does it work?

The indexer uses your specified JSON-RPC endpoint to fetch all the events for your contracts, then for all the events it calls your supplied reducer function which should build the database.

## Why event sourcing? 🤔

- The database can be rebuilt any time from the logs
- The EVM was designed with events in mind, it's only natural to use them!
- Reuse the exact same codebase to build queryable databases for any chain
- Separation of concerns, event sourcing builds the database, another service can serve it
- Easily testable, it's just a single function ✨
- It's fast, events are cached for super fast database rebuilds ⚡️

## Storage options

- **JsonStorage**: Use this if your data fits in memory and you don't want the complexity of an actual database. You can serve your JSON data through a static HTTP server, a custom REST API, as a [GraphQL API](https://github.com/marmelab/json-graphql-server) or even pin it to IPFS for front-end usage. Serve behind a CDN for even better performance.
- **SqliteStorage**: This is a great alternative if you still don't want the complexity of a server database. It will give you all the niceties of SQL and you'll be able to serve the database over IPFS for people to use.
- **Bring your own storage**: You can easily store your data elsewhere by implementing the `Storage` interface. You can for example store your data to PostgreSQL with Prisma, MongoDB or anything else you like.

## Limitations

- Because the indexer uses JSON-RPC to fetch logs, it depends on the gateway's ability to process and return events, some providers like Infura or Alchemy limit the amount of events that can be returned in one call. Chainsauce gets around this by fetching smaller block ranges. It's best to use your own node if you encounter issues, but this **should not be the case for most people**.

## Roadmap

- SQLite storage
- Parquet storage for DuckDB?
- Prisma storage?
- Websocket subscriptions for real-time events
