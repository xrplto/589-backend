const { MongoClient } = require("mongodb");
const xrpl = require("xrpl");

// Use environment variables for sensitive data
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const DB_NAME = process.env.DB_NAME || "xrpFun";
const COLLECTION_NAME = process.env.COLLECTION_NAME || "tweetsubmissions";
const NFT_COLLECTION_NAME = "nft_collections";

// XRPL server URL
const XRPL_SERVER = "wss://s1.ripple.com";

async function fetchNFTs(client, address, marker = undefined, limit = 100) {
  try {
    const response = await client.request({
      command: "account_nfts",
      account: address,
      ledger_index: "validated",
      limit: limit,
      marker: marker
    });
    return response.result;
  } catch (error) {
    console.error(`Error fetching NFTs for ${address}:`, error.message);
    return { account_nfts: [], marker: undefined };
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

    xrplClient = new xrpl.Client(XRPL_SERVER);
    await xrplClient.connect();

    // Fetch all documents from the collection
    const documents = await collection.find({}).toArray();

    if (documents.length > 0) {
      console.log(`Fetched ${documents.length} documents from the database.`);
      
      for (const [index, doc] of documents.entries()) {
        console.log(`\nProcessing Document ${index + 1}:`);
        
        if (doc.wallet_address) {
          if (!walletStats[doc.wallet_address]) {
            walletStats[doc.wallet_address] = {
              submissions: 0,
              nftCount: 0,
              matchingCollections: 0
            };
          }
          walletStats[doc.wallet_address].submissions++;

          let marker;
          let nfts = [];
          do {
            const result = await fetchNFTs(xrplClient, doc.wallet_address, marker);
            nfts = nfts.concat(result.account_nfts);
            marker = result.marker;
          } while (marker);

          walletStats[doc.wallet_address].nftCount = nfts.length;
          totalNFTCount += nfts.length;

          if (nfts.length > 0) {
            for (const nft of nfts) {
              // Match with nft_collections
              const matchingCollection = await nftCollections.findOne({
                issuer: nft.Issuer,
                taxon: nft.NFTokenTaxon.toString()
              });

              if (matchingCollection) {
                walletStats[doc.wallet_address].matchingCollections++;
                totalMatchingCollections++;
              }
            }
          }
        }
      }
    } else {
      console.log("No documents found in the collection");
    }

    // Display all statistics at the end
    console.log("\n--- Final Statistics ---");
    console.log("\nPer Wallet Statistics:");
    for (const [wallet, stats] of Object.entries(walletStats)) {
      console.log(`\nWallet: ${wallet}`);
      console.log(`  Tweet Submissions: ${stats.submissions}`);
      console.log(`  NFT Count: ${stats.nftCount}`);
      console.log(`  Matching Collections: ${stats.matchingCollections}`);
    }

    console.log("\nOverall Statistics:");
    console.log(`Total Unique Wallets: ${Object.keys(walletStats).length}`);
    console.log(`Total Tweet Submissions: ${Object.values(walletStats).reduce((sum, stats) => sum + stats.submissions, 0)}`);
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
