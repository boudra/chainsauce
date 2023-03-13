import { ethers } from "ethers";
import { createIndexer, JsonStorage } from "chainsauce";
import path from "node:path";

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

function convertToUSD() {
  return 0;
}

function fullProjectId(projectChainId, projectId, projectRegistryAddress) {
  return ethers.utils.solidityKeccak256(
    ["uint256", "address", "uint256"],
    [projectChainId, projectRegistryAddress, projectId]
  );
}

async function handleEvent(store, event) {
  const db = store.storage;

  switch (event.name) {
    // -- PROJECTS
    case "NewProjectApplication": {
      const project = db
        .collection("projects")
        .findOneWhere((project) => project.fullId == event.args.project);

      if (project) {
        // console.log("APPLICATION", event.args.project);

        db.collection("projectApplications").insert({
          projectId: project.id,
          projectFullId: project.fullId,
          roundId: event.address,
        });
      }

      break;
    }

    case "ProjectCreated": {
      if (event.args.projectID.toNumber() === 0) return;

      db.collection("projects").insert({
        fullId: fullProjectId(
          store.chainId,
          event.args.projectID.toNumber(),
          event.address
        ),
        id: event.args.projectID.toNumber(),
        metaPtr: null,
        votesUSD: 0,
        votes: 0,
        owners: [event.args.owner],
      });

      break;
    }

    case "MetadataUpdated":
      // const metadata = ipfs(event.args.metaPtr.pointer);
      db.collection("projects").updateById(event.args.projectID, (project) => ({
        ...project,
        metaPtr: event.args.metaPtr.pointer,
        // title: metadata.derive((m) => m.title),
        // description: metadata.derive((m) => m.description),
        // updatedAt: blockTimestamp(event.blockNumber),
      }));
      break;

    case "OwnerAdded": {
      db.collection("projects").updateById(event.args.projectID, (project) => ({
        ...project,
        owners: [...project.owners, event.args.owner],
      }));
      break;
    }

    case "OwnerRemoved": {
      db.collection("projects").updateById(event.args.projectID, (project) => ({
        ...project,
        owners: project.owners.filter((o) => o == event.args.owner),
      }));
      break;
    }

    // --- ROUND
    case "RoundCreated": {
      const round = store.subscribe(
        event.args.roundAddress,
        RoundImplementationABI,
        event.blockNumber
      );

      // const applicationMetadata = round.derive(async (round) => {
      //   const metaPtr = await round.applicationMetaPtr();
      //   return ipfs(metaPtr.pointer);
      // });

      db.collection("rounds").insert({
        id: event.args.roundAddress,
        votesUSD: 0,
        votes: 0,
        implementationAddress: event.args.roundImplementation,
        // applicationsStartTime: round.derive(async (r) =>
        //   (await r.applicationsStartTime()).toString()
        // ),
        // roundStartTime: round.derive(async (r) =>
        //   (await r.roundStartTime()).toString()
        // ),
        // roundEndTime: round.derive(async (r) =>
        //   (await r.roundEndTime()).toString()
        // ),
        // applicationsEndTime: round.derive(async (r) =>
        //   (await r.applicationsEndTime()).toString()
        // ),
        // applicationMetadata: applicationMetadata,
      });
      break;
    }

    // --- Voting Strategy
    case "VotingContractCreated": {
      store.subscribe(
        event.args.votingContractAddress,
        QuadraticFundingImplementationABI,
        event.blockNumber
      );
      break;
    }

    // --- Votes
    case "Voted": {
      const amountUSD = convertToUSD(event.args.token, event.args.amount);

      const projectApplicationId = [
        event.args.projectId,
        event.args.roundAddress,
      ].join("-");

      const voteId = ethers.utils.solidityKeccak256(
        ["string"],
        [`${event.transactionHash}-${event.args.grantAddress}`]
      );

      const project = db
        .collection("projects")
        .findOneWhere((project) => project.fullId == event.args.projectId);

      const vote = {
        id: voteId,
        token: event.args.token,
        voter: event.args.voter,
        grantAddress: event.args.grantAddress,
        amount: event.args.amount.toString(),
        amountUSD,
        fullProjectId: event.args.projectId,
        projectId: project?.id ?? null,
        roundAddress: event.args.roundAddress,
        projectApplicationId: projectApplicationId,
      };

      db.collection(`rounds/${event.args.roundAddress}/votes`).insert(vote);
      db.collection(
        `rounds/${event.args.roundAddress}/projects/${event.args.projectId}/votes`
      ).insert(vote);
      break;
    }

    default:
    // console.log("TODO", event.name, event.args);
  }
}

const provider = new ethers.providers.JsonRpcProvider(chain.rpc);
await provider.getNetwork();

const storageDir = path.join(config.storageDir, `${provider.network.chainId}`);
const persistence = new JsonStorage(storageDir);

const indexer = await createIndexer(provider, persistence, handleEvent);

for (let subscription of chain.subscriptions) {
  indexer.subscribe(subscription.address, subscription.abi);
}
