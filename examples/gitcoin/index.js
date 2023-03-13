import { ethers } from "ethers";
import { createIndexer, SqlitePersistence } from "chainsauce";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import config from "./config.js";

import ProjectRegistryABI from "./abis/ProjectRegistry.json" assert { type: "json" };
import RoundFactoryABI from "./abis/RoundFactory.json" assert { type: "json" };
import RoundImplementationABI from "./abis/RoundImplementation.json" assert { type: "json" };
import QuadraticFundingFactoryABI from "./abis/QuadraticFundingVotingStrategyFactory.json" assert { type: "json" };
import QuadraticFundingImplementationABI from "./abis/QuadraticFundingVotingStrategyImplementation.json" assert { type: "json" };

const chains = {
  mainnet: {
    rpc: `https://mainnet.infura.io/v3/${process.env.INFURA_API_KEY}`,
    subscriptions: [
      {
        address: "0x03506eD3f57892C85DB20C36846e9c808aFe9ef4",
        abi: ProjectRegistryABI,
      },
      {
        address: "0xE2Bf906f7d10F059cE65769F53fe50D8E0cC7cBe",
        abi: RoundFactoryABI,
      },
      {
        address: "0x06A6Cc566c5A88E77B1353Cdc3110C2e6c828e38",
        abi: QuadraticFundingFactoryABI,
      },
    ],
  },
  goerli: {
    rpc: `https://goerli.infura.io/v3/${process.env.INFURA_API_KEY}`,
    subscriptions: [
      {
        address: "0x832c5391dc7931312CbdBc1046669c9c3A4A28d5",
        abi: ProjectRegistryABI,
        fromBlock: 0,
      },
      {
        address: "0x5770b7a57BD252FC4bB28c9a70C9572aE6400E48",
        abi: RoundFactoryABI,
        fromBlock: 0,
      },
      {
        address: "0x0000000000000000000000000000000000000000",
        abi: QuadraticFundingFactoryABI,
        fromBlock: 0,
      },
    ],
  },
  optimism: {
    rpc: `https://opt-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`,
    subscriptions: [],
  },
};

const chainName = process.argv[2];

if (!chainName) {
  console.error("Please provide a chain name to index.");
  process.exit(1);
}

const chain = chains[chainName];

if (!chain) {
  console.error("Chain", chainName, "not supported yet.");
  process.exit(1);
}

async function convertToUSD() {
  return 0;
}

function fullProjectId(projectChainId, projectId, projectRegistryAddress) {
  return ethers.utils.solidityKeccak256(
    ["uint256", "address", "uint256"],
    [projectChainId, projectRegistryAddress, projectId]
  );
}

async function handleEvent(indexer, event) {
  const db = indexer.storage.db;

  switch (event.name) {
    // -- PROJECTS
    case "NewProjectApplication": {
      const project = db
        .prepare("SELECT id FROM Projects WHERE fullId = ?")
        .get(event.args.project);

      db.prepare(
        `INSERT OR IGNORE INTO ProjectApplications (projectId, fullProjectId, roundId)
         VALUES (?, ?, ?)`
      ).run(project?.id ?? null, event.args.project, event.address);

      break;
    }

    case "ProjectCreated": {
      if (event.args.projectID.toNumber() === 0) return;

      const fullId = fullProjectId(
        store.chainId,
        event.args.projectID.toNumber(),
        event.address
      );

      db.prepare("INSERT INTO Projects (id, fullId) VALUES (?, ?)").run(
        event.args.projectID.toNumber(),
        fullId
      );

      db.prepare(
        "INSERT INTO ProjectOwners (address, projectId) VALUES (?, ?)"
      ).run(event.args.owner, event.args.projectID.toNumber());

      break;
    }

    case "MetadataUpdated": {
      db.prepare("UPDATE Projects SET metaPtr = ? WHERE id = ?").run(
        event.args.metaPtr.pointer,
        event.args.projectID.toNumber()
      );
      break;
    }

    case "OwnerAdded": {
      db.prepare(
        "INSERT INTO ProjectOwners (address, projectId) VALUES (?, ?)"
      ).run(event.args.owner, event.args.projectID.toNumber());
      break;
    }

    case "OwnerRemoved": {
      db.prepare(
        "DELETE FROM ProjectOwners WHERE projectID = ? AND address = ?"
      ).run(event.args.projectID.toNumber(), event.args.owner);
      break;
    }

    // --- ROUND
    case "RoundCreated": {
      store.subscribe(event.args.roundAddress, RoundImplementationABI);

      db.prepare(
        `INSERT OR IGNORE INTO Rounds (id, implementationAddress)
         VALUES (?, ?)`
      ).run(event.args.roundAddress, event.args.roundImplementation);

      break;
    }

    // --- Voting Strategy
    case "VotingContractCreated": {
      store.subscribe(
        event.args.votingContractAddress,
        QuadraticFundingImplementationABI
      );
      break;
    }

    // --- Votes
    case "Voted": {
      const amountUSD = await convertToUSD(event.args.token, event.args.amount);
      const project = db
        .prepare("SELECT id FROM Projects WHERE fullId = ?")
        .get(event.args.projectId);

      const voteId = ethers.utils.solidityKeccak256(
        ["string"],
        [`${event.transactionHash}-${event.args.grantAddress}`]
      );

      db.prepare(
        `INSERT OR IGNORE INTO Votes (
          id,
          token,
          voter,
          grantAddress,
          amount,
          amountUSD,
          projectId,
          fullProjectId,
          roundId
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).run(
        voteId,
        event.args.token,
        event.args.voter,
        event.args.grantAddress,
        event.args.amount.toString(),
        amountUSD,
        project?.id ?? null,
        event.args.projectId,
        event.args.roundAddress
      );
      break;
    }

    default:
    // console.log("TODO", event.name, event.args);
  }
}

const provider = new ethers.providers.JsonRpcProvider(chain.rpc);

await provider.getNetwork();

const databaseFile = path.join(
  config.storageDir,
  `${provider.network.chainId}.db`
);

fs.mkdirSync(path.dirname(databaseFile), { recursive: true });

const db = new Database(databaseFile);
const storage = new SqlitePersistence(db);

const migration = fs.readFileSync("migration.sql", "utf8");

db.exec(migration);

const store = await createIndexer(provider, storage, handleEvent);

for (let subscription of chain.subscriptions) {
  store.subscribe(subscription.address, subscription.abi);
}
