const { MongoClient, ObjectId } = require("mongodb");
const WebSocket = require("ws");
const BigNumber = require("bignumber.js");
const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));
const Bottleneck = require("bottleneck"); // Import Bottleneck

// Set BigNumber configuration to avoid scientific notation
BigNumber.config({ EXPONENTIAL_AT: 1e9 });

// MongoDB connection details
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const DB_NAME = process.env.DB_NAME || "xrpFun";
const COLLECTION_NAME = process.env.COLLECTION_NAME || "coins";

// Rate limiting constants for xrplcluster.com
const XRPL_MAX_CONNECTIONS = 5;
const XRPL_MAX_MESSAGES_PER_MINUTE = 1000;

// Create a Bottleneck limiter for xrplcluster.com
const xrplClusterLimiter = new Bottleneck({
  reservoir: XRPL_MAX_MESSAGES_PER_MINUTE, // Number of messages
  reservoirRefreshAmount: XRPL_MAX_MESSAGES_PER_MINUTE,
  reservoirRefreshInterval: 60 * 1000, // Refresh every minute
  maxConcurrent: XRPL_MAX_CONNECTIONS, // Max concurrent connections
  minTime: Math.ceil(60000 / XRPL_MAX_MESSAGES_PER_MINUTE), // Minimum time between messages
});

// Modify the XRPL_WSS_LIST to prioritize other nodes
const XRPL_WSS_LIST = [
  "wss://s2.ripple.com/",
  "wss://s1.ripple.com/",
  "wss://xrplcluster.com/", // This node has rate limits
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
        // Use the Bottleneck limiter for xrplcluster.com
        const limitedFetch = xrplClusterLimiter.wrap(() =>
          fetchAMMInfo(selectedWSS, asset, asset2)
        );
        const result = await limitedFetch();
        return result;
      } else {
        // For other nodes, proceed without rate limiting
        const result = await fetchAMMInfo(selectedWSS, asset, asset2);
        return result;
      }
    } catch (error) {
      console.error(`Error with node ${selectedWSS}:`, error.message);
      // Continue to the next node if there's an error
    }
  }

  throw new Error("Failed to get AMM info from all available nodes");
}

function fetchAMMInfo(selectedWSS, asset, asset2) {
  return new Promise((resolve, reject) => {
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
      try {
        const response = JSON.parse(data);
        ws.close();
        if (response.result) {
          resolve(response.result);
        } else {
          reject(new Error("Invalid response format"));
        }
      } catch (parseError) {
        ws.close();
        reject(new Error("Failed to parse WebSocket message"));
      }
    });

    ws.on("error", (error) => {
      clearTimeout(timeout);
      ws.close();
      reject(new Error(`Node ${selectedWSS} error: ${error.message}`));
    });
  });
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
      "AMM information, prices, market caps, and 'King of the Hill' status updated successfully"
    );
  } catch (error) {
    console.error("Error during AMM information update:", error);
  } finally {
    console.timeEnd(uniqueLabel);
  }
}

// Modify the processTokens function to maximize throughput while respecting rate limits
async function processTokens(tokens, collection) {
  // Process all tokens concurrently, but limit the concurrency to prevent overwhelming the system
  const concurrency = 50; // Adjust based on your system's capacity
  const queue = [...tokens];
  const promises = [];

  for (let i = 0; i < concurrency; i++) {
    const worker = async () => {
      while (queue.length > 0) {
        const token = queue.shift();
        if (token) {
          await processToken(token, collection);
        }
      }
    };
    promises.push(worker());
  }

  await Promise.all(promises);
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

    // Calculate pool balance
    const xrpPercentage = xrpAmountBN
      .dividedBy(totalLiquidityXRP)
      .multipliedBy(100);
    const tokenPercentage = tokenLiquidityInXRP
      .dividedBy(totalLiquidityXRP)
      .multipliedBy(100);
    const poolImbalance = xrpAmountBN.minus(tokenLiquidityInXRP);
    const poolImbalancePercentage = poolImbalance
      .dividedBy(totalLiquidityXRP)
      .multipliedBy(100)
      .abs();

    // Check for King of the Hill status
    let kingOfTheHill = token.kingOfTheHill;
    const threshold = new BigNumber(58900); // Set your threshold value here
    if (marketCapBN.isGreaterThanOrEqualTo(threshold) && !kingOfTheHill) {
      kingOfTheHill = {
        label: "King of the hill",
        timestamp: new Date(),
      };
    }

    console.log(`AMM Liquidity for ${token.symbol}:`);
    console.log(
      `XRP: ${xrpAmountBN.toFixed(6)} XRP (${xrpPercentage.toFixed(2)}% of pool)`
    );
    console.log(
      `${token.symbol}: ${tokenAmountBN.toFixed(6)} ${token.symbol} (${tokenLiquidityInXRP.toFixed(
        6
      )} XRP, ${tokenPercentage.toFixed(2)}% of pool)`
    );
    console.log(`Total Liquidity: ${totalLiquidityXRP.toFixed(6)} XRP`);
    console.log(
      `Pool Imbalance: ${poolImbalance.toFixed(
        6
      )} XRP (${poolImbalancePercentage.toFixed(2)}%)`
    );
    console.log(
      `Pool Health: ${
        assessPoolHealth(poolImbalancePercentage.toNumber()).status
      } - ${
        assessPoolHealth(poolImbalancePercentage.toNumber()).message
      }`
    );
    console.log(`Spot Price: ${spotPriceBN.toFixed()} XRP per ${token.symbol}`);
    console.log(
      `Price per XRP: ${pricePerXRPBN.toFixed(6)} ${token.symbol} per XRP`
    );
    console.log(`Market Cap: ${marketCapBN.toFixed()} XRP`);
    if (kingOfTheHill) {
      console.log(`King of the Hill Status: ${kingOfTheHill.label}`);
    }
    console.log("---");

    // Update the token document with all the new information
    const updateFields = {
      ammInfo: ammInfo,
      lastAMMUpdate: new Date(),
      spotPrice: spotPriceBN.toFixed(),
      pricePerXRP: pricePerXRPBN.toFixed(),
      marketCap: marketCapBN.toFixed(),
      totalLiquidity: totalLiquidityXRP.toFixed(6),
      xrpLiquidity: {
        amount: xrpAmountBN.toFixed(6),
        percentage: xrpPercentage.toFixed(2),
      },
      tokenLiquidity: {
        amount: tokenAmountBN.toFixed(6),
        xrpValue: tokenLiquidityInXRP.toFixed(6),
        percentage: tokenPercentage.toFixed(2),
      },
      poolImbalance: {
        amount: poolImbalance.toFixed(6),
        percentage: poolImbalancePercentage.toFixed(2),
        health: {
          status: assessPoolHealth(poolImbalancePercentage.toNumber()).status,
          message:
            assessPoolHealth(poolImbalancePercentage.toNumber()).message,
        },
      },
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

// Add this function to assess pool health
function assessPoolHealth(imbalancePercentage) {
  if (imbalancePercentage <= 1) {
    return { status: "Excellent", message: "Pool is well-balanced" };
  } else if (imbalancePercentage <= 3) {
    return { status: "Good", message: "Pool is slightly imbalanced" };
  } else if (imbalancePercentage <= 5) {
    return { status: "Fair", message: "Pool imbalance is noticeable" };
  } else if (imbalancePercentage <= 10) {
    return { status: "Poor", message: "Pool is significantly imbalanced" };
  } else {
    return { status: "Critical", message: "Pool is severely imbalanced" };
  }
}

// Export the function
module.exports = {
  updateAMMInfo,
};

// Main function to include more detailed logging and prevent overlapping runs
let isUpdating = false;

async function main() {
  try {
    // Ensure fetch is available before starting the update loop
    await fetch;

    console.log("Starting continuous AMM updates...");

    while (true) {
      if (!isUpdating) {
        isUpdating = true;
        updateAMMInfo()
          .then(() => {
            isUpdating = false;
            // Immediately proceed to the next update without waiting
          })
          .catch((error) => {
            console.error("Error during AMM update:", error);
            isUpdating = false;
            // Optionally, implement a retry mechanism or delay on error
          });
      }

      // Yield control to allow asynchronous operations
      await new Promise((resolve) => setImmediate(resolve));
    }
  } catch (error) {
    console.error("Error during continuous AMM updates:", error);
    // Optionally, implement a retry mechanism or exit the process
    process.exit(1);
  }
}

// Run the main function if this file is being run directly
if (require.main === module) {
  (async () => {
    await main();
  })();
}
