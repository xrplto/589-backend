const { MongoClient } = require("mongodb");
const WebSocket = require("ws");
const BigNumber = require("bignumber.js");
const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

// Set BigNumber configuration to avoid scientific notation
BigNumber.config({ EXPONENTIAL_AT: 1e9 });

// MongoDB connection details
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const DB_NAME = process.env.DB_NAME || "xrpFun";
const COLLECTION_NAME = process.env.COLLECTION_NAME || "coins";

// Add these constants at the top of the file
const MAX_CONNECTIONS = 5;
const MAX_MESSAGES_PER_MINUTE = 1000;
const MESSAGE_INTERVAL = 60000 / MAX_MESSAGES_PER_MINUTE; // Milliseconds between messages

// Modify the XRPL_WSS_LIST to prioritize other nodes
const XRPL_WSS_LIST = [
  "wss://s2.ripple.com/",
  "wss://s1.ripple.com/",
  "wss://xrplcluster.com/", // Move this to the end of the list
];

// Create a cached database connection
let cachedClient = null;
let cachedDb = null;

async function connectToDatabase() {
  if (cachedClient && cachedDb) {
    return { client: cachedClient, db: cachedDb };
  }

  const client = await MongoClient.connect(MONGODB_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });

  const db = client.db(DB_NAME);
  cachedClient = client;
  cachedDb = db;

  return { client, db };
}

async function getAMMInfo(asset, asset2) {
  for (const selectedWSS of XRPL_WSS_LIST) {
    try {
      if (selectedWSS.includes("xrplcluster.com")) {
        // Respect the rate limit for xrplcluster.com
        await new Promise(resolve => setTimeout(resolve, MESSAGE_INTERVAL));
      }

      const result = await new Promise((resolve, reject) => {
        const ws = new WebSocket(selectedWSS);

        const timeout = setTimeout(() => {
          ws.close();
          reject(new Error("WebSocket connection timeout"));
        }, 10000); // 10 seconds timeout

        ws.on("open", () => {
          clearTimeout(timeout);
          const request = {
            command: "amm_info",
            asset: asset,
            asset2: asset2,
          };
          ws.send(JSON.stringify(request));
        });

        ws.on("message", (data) => {
          const response = JSON.parse(data);
          ws.close();
          resolve(response.result);
        });

        ws.on("error", (error) => {
          clearTimeout(timeout);
          ws.close();
          reject(new Error(`Node ${selectedWSS} error: ${error.message}`));
        });
      });

      return result;
    } catch (error) {
      console.error(`Error with node ${selectedWSS}:`, error.message);
      // Continue to the next node if there's an error
    }
  }

  throw new Error("Failed to get AMM info from all available nodes");
}

// Function to fetch and update AMM info, prices, market caps, and King of the Hill status
async function updateAMMInfo() {
  const uniqueLabel = `AMM update ${Date.now()}`;
  console.time(uniqueLabel);
  try {
    const { db } = await connectToDatabase();
    const collection = db.collection(COLLECTION_NAME);

    const tokens = await collection
      .find({ ammAddress: { $exists: true, $ne: "" } })
      .toArray();

    console.log(`Starting update for ${tokens.length} tokens`);

    await processTokens(tokens, collection);

    console.log(
      "AMM information, prices, market caps, price changes, and 'King of the Hill' status updated successfully"
    );
  } catch (error) {
    console.error("Error during AMM information update:", error);
  } finally {
    console.timeEnd(uniqueLabel);
  }
}

// Modify the processTokens function to respect the connection limit
async function processTokens(tokens, collection) {
  const batchSize = MAX_CONNECTIONS;
  for (let i = 0; i < tokens.length; i += batchSize) {
    const batch = tokens.slice(i, i + batchSize);
    await Promise.all(batch.map(token => processToken(token, collection)));
    
    // Add a delay between batches to respect the rate limit
    if (i + batchSize < tokens.length) {
      await new Promise(resolve => setTimeout(resolve, MESSAGE_INTERVAL * batchSize));
    }
  }
}

// Function to process a single token
async function processToken(token, collection) {
  const asset = { currency: "XRP" };
  const asset2 = {
    currency: token.currencyCode,
    issuer: token.issuer,
  };

  try {
    const ammInfoResult = await getAMMInfo(asset, asset2);

    if (!ammInfoResult || !ammInfoResult.amm) {
      console.warn(`No AMM info found for ${token.symbol}`);
      return;
    }

    const ammInfo = ammInfoResult.amm;

    // Use BigNumber for precise calculations
    const xrpAmountBN = new BigNumber(ammInfo.amount).dividedBy(1e6);
    const tokenAmountBN = new BigNumber(ammInfo.amount2.value);

    // Calculate spot price (XRP per token)
    const spotPriceBN = xrpAmountBN.dividedBy(tokenAmountBN);

    // Calculate price per XRP (tokens per XRP)
    const pricePerXRPBN = tokenAmountBN.dividedBy(xrpAmountBN);

    // Calculate market cap: spotPrice * totalSupply
    let marketCapBN = new BigNumber(0);
    if (token.totalSupply) {
      marketCapBN = spotPriceBN.multipliedBy(token.totalSupply);
    }

    // Calculate total liquidity in XRP
    const tokenLiquidityInXRP = tokenAmountBN.multipliedBy(spotPriceBN);
    const totalLiquidityXRP = xrpAmountBN.plus(tokenLiquidityInXRP);

    // Check for King of the Hill status
    let kingOfTheHill = token.kingOfTheHill;
    const threshold = new BigNumber(58900); // Set your threshold value here
    if (marketCapBN.isGreaterThanOrEqualTo(threshold) && !kingOfTheHill) {
      kingOfTheHill = {
        label: "King of the hill",
        timestamp: new Date(),
      };
    }

    // Calculate price changes
    const priceChange1h = calculatePriceChange(token.spotPrice, spotPriceBN, 1);
    const priceChange24h = calculatePriceChange(token.spotPrice, spotPriceBN, 24);
    const priceChange7d = calculatePriceChange(token.spotPrice, spotPriceBN, 168); // 7 days * 24 hours

    console.log(`AMM Liquidity for ${token.symbol}:`);
    console.log(`XRP: ${xrpAmountBN.toFixed(6)} XRP`);
    console.log(
      `${token.symbol}: ${tokenAmountBN.toFixed(6)} ${token.symbol}`
    );
    console.log(
      `Spot Price: ${spotPriceBN.toFixed()} XRP per ${token.symbol}`
    );
    console.log(
      `Price per XRP: ${pricePerXRPBN.toFixed(6)} ${token.symbol} per XRP`
    );
    console.log(`Market Cap: ${marketCapBN.toFixed()} XRP`);
    console.log(`Total Liquidity: ${totalLiquidityXRP.toFixed(6)} XRP`);
    if (kingOfTheHill) {
      console.log(`King of the Hill Status: ${kingOfTheHill.label}`);
    }
    console.log(`Price Change 1h: ${priceChange1h.toFixed(2)}%`);
    console.log(`Price Change 24h: ${priceChange24h.toFixed(2)}%`);
    console.log(`Price Change 7d: ${priceChange7d.toFixed(2)}%`);
    console.log("---");

    // Update the token document with all the new information
    const updateFields = {
      ammInfo: ammInfo,
      lastAMMUpdate: new Date(),
      spotPrice: spotPriceBN.toFixed(),
      pricePerXRP: pricePerXRPBN.toFixed(),
      marketCap: marketCapBN.toFixed(),
      totalLiquidity: totalLiquidityXRP.toFixed(6),
      priceChange1h: priceChange1h.toFixed(2),
      priceChange24h: priceChange24h.toFixed(2),
      priceChange7d: priceChange7d.toFixed(2),
      lastUpdated: new Date(), // Always update the lastUpdated field
    };

    if (kingOfTheHill) {
      updateFields.kingOfTheHill = kingOfTheHill;
    }

    await collection.updateOne(
      { _id: token._id },
      {
        $set: updateFields,
      }
    );

    console.log(`Updated AMM info for ${token.symbol}`);
  } catch (error) {
    console.error(
      `Error fetching AMM info for ${token.symbol}:`,
      error.message
    );
    // If the error contains node information, log it
    if (error.message.includes("Node")) {
      console.error(
        `Node having issues for ${token.symbol}:`,
        error.message.split(": ")[0]
      );
    }
  }
}

// Function to calculate price change
function calculatePriceChange(oldPrice, newPrice, hours) {
  if (!oldPrice) return new BigNumber(0);
  const oldPriceBN = new BigNumber(oldPrice);
  if (oldPriceBN.isZero()) return new BigNumber(0);
  
  const priceChange = newPrice.minus(oldPriceBN).dividedBy(oldPriceBN).multipliedBy(100);
  return priceChange;
}

// Export the function
module.exports = {
  updateAMMInfo,
};

// Main function to include more detailed logging and prevent overlapping runs
async function main() {
  try {
    // Ensure fetch is available before starting the update loop
    await fetch;

    console.log("Starting initial AMM update...");
    while (true) {
      await updateAMMInfo();
      console.log("AMM update completed successfully");
      // No wait here, updates will run continuously
    }
  } catch (error) {
    console.error("Error during initial AMM update:", error);
  }
}

// Run the main function if this file is being run directly
if (require.main === module) {
  (async () => {
    await main();
  })();
}