const { MongoClient } = require("mongodb");
const xrpl = require("xrpl");

// Use environment variables for sensitive data
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const DB_NAME = process.env.DB_NAME || "xrpFun";
const COLLECTION_NAME = process.env.COLLECTION_NAME || "tweetsubmissions";
const NFT_COLLECTION_NAME = "nft_collections";

// XRPL server URL
const XRPL_SERVER = "wss://s1.ripple.com";

const REWARD_WALLET = "rhsxg4xH8FtYc3eR53XDSjTGfKQsaAGaqm";

async function fetchAccountLines(client, address) {
  try {
    const response = await client.request({
      command: "account_lines",
      account: address,
      ledger_index: "validated",
    });
    return response.result.lines;
  } catch (error) {
    console.error(
      `Error fetching account lines for ${address}:`,
      error.message
    );
    return [];
  }
}

async function fetchAccountInfo(client, address) {
  try {
    const response = await client.request({
      command: "account_info",
      account: address,
      ledger_index: "validated",
    });
    return response.result;
  } catch (error) {
    console.error(`Error fetching account info for ${address}:`, error.message);
    return null;
  }
}

async function fetchNFTs(client, address, marker = undefined, limit = 100) {
  try {
    const response = await client.request({
      command: "account_nfts",
      account: address,
      ledger_index: "validated",
      limit: limit,
      marker: marker,
    });
    return response.result;
  } catch (error) {
    console.error(`Error fetching NFTs for ${address}:`, error.message);
    return { account_nfts: [], marker: undefined };
  }
}

async function fetchTokenSupply(client, issuer, currencyCode) {
  try {
    const response = await client.request({
      command: "gateway_balances",
      account: issuer,
      strict: true,
    });

    if (
      response.result.obligations &&
      response.result.obligations[currencyCode]
    ) {
      return response.result.obligations[currencyCode];
    }
    return "Not available";
  } catch (error) {
    console.error(`Error fetching token supply for ${issuer}:`, error.message);
    return "Error fetching supply";
  }
}

async function fetchDataFromDB() {
  let mongoClient;
  let xrplClient;
  let totalNFTCount = 0;
  let totalMatchingCollections = 0;
  const walletStats = {};

  try {
    mongoClient = await MongoClient.connect(MONGODB_URI);
    const db = mongoClient.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);
    const nftCollections = db.collection(NFT_COLLECTION_NAME);
    const farmTokensCollection = db.collection("farm_tokens");

    xrplClient = new xrpl.Client(XRPL_SERVER);
    await xrplClient.connect();

    // Fetch reward wallet info (without logging)
    const rewardWalletInfo = await fetchAccountInfo(xrplClient, REWARD_WALLET);

    // Fetch and display token balances for the reward wallet
    const rewardWalletLines = await fetchAccountLines(xrplClient, REWARD_WALLET);
    console.log("\nReward Wallet Token Balances:");
    for (const line of rewardWalletLines) {
      if (parseFloat(line.balance) !== 0) {
        console.log(`  ${line.currency}: ${line.balance}`);
        console.log(`    Issuer: ${line.account}`);
      }
    }

    // Fetch all documents from the collection
    const documents = await collection.find({}).toArray();

    if (documents.length > 0) {
      for (const doc of documents) {
        if (doc.wallet_address) {
          if (!walletStats[doc.wallet_address]) {
            walletStats[doc.wallet_address] = {
              submissions: 0,
              nftCount: 0,
              matchingCollections: 0,
              nftsProcessed: false,
              tokenBalances: {},
            };
          }
          walletStats[doc.wallet_address].submissions++;

          // Fetch account info for the wallet (without logging)
          const accountInfo = await fetchAccountInfo(
            xrplClient,
            doc.wallet_address
          );

          // Fetch and store token balances
          const accountLines = await fetchAccountLines(
            xrplClient,
            doc.wallet_address
          );
          for (const line of accountLines) {
            if (parseFloat(line.balance) !== 0) {
              const farmToken = await farmTokensCollection.findOne({
                issuer: line.account,
                currencyCode: line.currency,
              });

              if (farmToken) {
                walletStats[doc.wallet_address].tokenBalances[farmToken.name] = {
                  balance: line.balance,
                  issuer: line.account,
                };
              }
            }
          }

          // Process NFTs (without logging)
          if (!walletStats[doc.wallet_address].nftsProcessed) {
            let marker;
            let nfts = [];
            do {
              const result = await fetchNFTs(
                xrplClient,
                doc.wallet_address,
                marker
              );
              nfts = nfts.concat(result.account_nfts);
              marker = result.marker;
            } while (marker);

            walletStats[doc.wallet_address].nftCount = nfts.length;

            for (const nft of nfts) {
              const matchingCollection = await nftCollections.findOne({
                issuer: nft.Issuer,
                taxon: nft.NFTokenTaxon.toString(),
              });

              if (matchingCollection) {
                walletStats[doc.wallet_address].matchingCollections++;
              }
            }

            walletStats[doc.wallet_address].nftsProcessed = true;
          }
        }
      }

      // Calculate totals after processing all documents
      for (const stats of Object.values(walletStats)) {
        totalNFTCount += stats.nftCount;
        totalMatchingCollections += stats.matchingCollections;
      }
    }

    // Display only final statistics
    console.log("\n--- Final Statistics ---");
    console.log("\nPer Wallet Statistics:");
    for (const [wallet, stats] of Object.entries(walletStats)) {
      console.log(`\nWallet: ${wallet}`);
      console.log(`  Tweet Submissions: ${stats.submissions}`);
      console.log(`  NFT Count: ${stats.nftCount}`);
      console.log(`  Matching Collections: ${stats.matchingCollections}`);
      if (Object.keys(stats.tokenBalances).length > 0) {
        console.log("  Matched Farm Token Balances:");
        for (const [name, data] of Object.entries(stats.tokenBalances)) {
          console.log(`    ${name}: ${data.balance} (Issuer: ${data.issuer})`);
        }
      }
    }

    console.log("\nOverall Statistics:");
    console.log(`Total Unique Wallets: ${Object.keys(walletStats).length}`);
    console.log(
      `Total Tweet Submissions: ${Object.values(walletStats).reduce(
        (sum, stats) => sum + stats.submissions,
        0
      )}`
    );
    console.log(`Total NFT Count: ${totalNFTCount}`);
    console.log(`Total Matching Collections: ${totalMatchingCollections}`);
  } catch (error) {
    console.error("Error fetching data from the database:", error);
  } finally {
    if (mongoClient) {
      await mongoClient.close();
    }
    if (xrplClient && xrplClient.isConnected()) {
      await xrplClient.disconnect();
    }
  }
}

// Run the function
fetchDataFromDB().catch(console.error);