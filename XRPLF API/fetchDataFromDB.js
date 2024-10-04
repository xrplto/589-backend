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
      console.log(`Fetched ${documents.length} documents from the database:`);
      for (const [index, doc] of documents.entries()) {
        console.log(`\nDocument ${index + 1}:`);
        console.log(JSON.stringify(doc, null, 2));

        if (doc.wallet_address) {
          let marker;
          let nfts = [];
          do {
            const result = await fetchNFTs(xrplClient, doc.wallet_address, marker);
            nfts = nfts.concat(result.account_nfts);
            marker = result.marker;
          } while (marker);

          console.log(`NFT count for ${doc.wallet_address}: ${nfts.length}`);
          totalNFTCount += nfts.length;

          let addressMatchingCollections = 0;

          if (nfts.length > 0) {
            console.log(`NFTs for ${doc.wallet_address}:`);
            for (const nft of nfts) {
              console.log(JSON.stringify(nft, null, 2));
              
              // Match with nft_collections
              const matchingCollection = await nftCollections.findOne({
                issuer: nft.Issuer,
                taxon: nft.NFTokenTaxon.toString()
              });

              if (matchingCollection) {
                console.log("Matching collection found:");
                console.log(JSON.stringify(matchingCollection, null, 2));
                addressMatchingCollections++;
                totalMatchingCollections++;
              } else {
                console.log("No matching collection found for this NFT.");
              }
              console.log("---");
            }
          } else {
            console.log(`No NFTs found for ${doc.wallet_address}`);
          }

          console.log(`Total matching collections for ${doc.wallet_address}: ${addressMatchingCollections}`);
        }
      }
    } else {
      console.log("No documents found in the collection");
    }

    console.log(`\nTotal NFT count across all addresses: ${totalNFTCount}`);
    console.log(`Total matching collections across all addresses: ${totalMatchingCollections}`);
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
