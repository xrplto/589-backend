const { MongoClient } = require("mongodb");
const WebSocket = require('ws');
const BigNumber = require('bignumber.js');
const Bottleneck = require('bottleneck');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));

// Set BigNumber configuration to avoid scientific notation
BigNumber.config({ EXPONENTIAL_AT: 1e+9 });

// MongoDB connection details
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const DB_NAME = process.env.DB_NAME || "xrpFun";
const COLLECTION_NAME = process.env.COLLECTION_NAME || "coins";

// XRPL WebSocket server
const XRPL_WSS = "wss://s2.ripple.com/";

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
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(XRPL_WSS);

    ws.on('open', () => {
      const request = {
        command: "amm_info",
        asset: asset,
        asset2: asset2
      };

      ws.send(JSON.stringify(request));
    });

    ws.on('message', (data) => {
      const response = JSON.parse(data);
      console.log('AMM Info Response:', JSON.stringify(response, null, 2));
      ws.close();
      resolve(response.result);
    });

    ws.on('error', (error) => {
      ws.close();
      reject(error);
    });
  });
}

// Rate Limiting State
let rateLimit = {
  limit: 600,          // Default limit
  remaining: 600,      // Default remaining
  reset: Date.now() + 60 * 1000, // Default reset time (1 minute from now)
};

// Helper function to wait for a specified number of milliseconds
function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Initialize Bottleneck limiter with default settings
const limiter = new Bottleneck({
  reservoir: rateLimit.remaining, // initial number of requests
  reservoirRefreshAmount: rateLimit.limit,
  reservoirRefreshInterval: 60 * 1000, // 1 minute
  maxConcurrent: 10, // Increased concurrency
  minTime: 100, // Decreased minimum time between requests to 100ms
});

// Function to update rate limit state based on response headers
function updateRateLimit(headers) {
  const limit = parseInt(headers.get('X-Ratelimit-Limit'));
  const remaining = parseInt(headers.get('X-Ratelimit-Remaining'));
  const reset = parseInt(headers.get('X-Ratelimit-Reset')); // Assuming reset is in seconds

  let updated = false;

  if (!isNaN(limit)) {
    rateLimit.limit = limit;
    updated = true;
  }
  if (!isNaN(remaining)) {
    rateLimit.remaining = remaining;
    updated = true;
  }
  if (!isNaN(reset)) {
    rateLimit.reset = Date.now() + reset * 1000;
    updated = true;
  }

  if (updated) {
    const refreshInterval = rateLimit.reset - Date.now();
    limiter.updateSettings({
      reservoir: rateLimit.remaining,
      reservoirRefreshAmount: rateLimit.limit,
      reservoirRefreshInterval: refreshInterval > 0 ? refreshInterval : 60 * 1000, // Fallback to 1 minute
    });

    console.log(`Rate Limit Updated: Limit=${rateLimit.limit}, Remaining=${rateLimit.remaining}, Reset in=${Math.ceil((rateLimit.reset - Date.now()) / 1000)} seconds`);
  }
}

// Function to handle rate limiting dynamically (if needed)
async function handleRateLimiting() {
  const now = Date.now();
  if (rateLimit.remaining <= 0) {
    const waitTime = rateLimit.reset - now;
    if (waitTime > 0) {
      console.warn(`Rate limit exceeded. Waiting for ${Math.ceil(waitTime / 1000)} seconds.`);
      await wait(waitTime);
    }
    // Reset the reservoir after waiting
    limiter.updateSettings({
      reservoir: rateLimit.limit,
      reservoirRefreshAmount: rateLimit.limit,
      reservoirRefreshInterval: 60 * 1000, // 1 minute
    });
  }
}

// Function to fetch and update AMM info, prices, market caps, and King of the Hill status
async function updateAMMInfo() {
  const uniqueLabel = `AMM update ${Date.now()}`;
  console.time(uniqueLabel);
  try {
    const { db } = await connectToDatabase();
    const collection = db.collection(COLLECTION_NAME);

    // Find all tokens with an ammAddress
    const tokens = await collection.find({ ammAddress: { $exists: true, $ne: "" } }).toArray();

    console.log(`Starting update for ${tokens.length} tokens`);

    for (const token of tokens) {
      const asset = { currency: "XRP" };
      const asset2 = {
        currency: token.currencyCode,
        issuer: token.issuer
      };

      try {
        const ammInfoResult = await getAMMInfo(asset, asset2);

        if (!ammInfoResult || !ammInfoResult.amm) {
          console.warn(`No AMM info found for ${token.symbol}`);
          continue;
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

        // Check for King of the Hill status
        let kingOfTheHill = token.kingOfTheHill;
        const threshold = new BigNumber(58900); // Set your threshold value here
        if (marketCapBN.isGreaterThanOrEqualTo(threshold) && !kingOfTheHill) {
          kingOfTheHill = {
            label: "King of the hill",
            timestamp: new Date(),
          };
        }

        // Store historical prices
        const priceHistoryEntry = {
          timestamp: new Date(),
          spotPrice: spotPriceBN.toFixed(), // Use toFixed() to ensure decimal notation
        };

        // Fetch existing price history
        const existingPriceHistory = token.priceHistory || [];

        // Append the new price to the history
        existingPriceHistory.push(priceHistoryEntry);

        // Keep only the necessary history (e.g., last 7 days)
        const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        const updatedPriceHistory = existingPriceHistory.filter(
          (entry) => new Date(entry.timestamp) >= sevenDaysAgo
        );

        // Calculate price changes
        const priceChange1h = calculatePriceChange(existingPriceHistory, 1);
        const priceChange24h = calculatePriceChange(existingPriceHistory, 24);
        const priceChange7d = calculatePriceChange(existingPriceHistory, 168); // 7 days * 24 hours

        console.log(`AMM Liquidity for ${token.symbol}:`);
        console.log(`XRP: ${xrpAmountBN.toFixed(6)} XRP`);
        console.log(`${token.symbol}: ${tokenAmountBN.toFixed(6)} ${token.symbol}`);
        console.log(`Spot Price: ${spotPriceBN.toFixed()} XRP per ${token.symbol}`);
        console.log(`Price per XRP: ${pricePerXRPBN.toFixed(6)} ${token.symbol} per XRP`);
        console.log(`Market Cap: ${marketCapBN.toFixed()} XRP`);
        console.log(`Price Change 1h: ${priceChange1h.toFixed(2)}%`);
        console.log(`Price Change 24h: ${priceChange24h.toFixed(2)}%`);
        console.log(`Price Change 7d: ${priceChange7d.toFixed(2)}%`);
        if (kingOfTheHill) {
          console.log(`King of the Hill Status: ${kingOfTheHill.label}`);
        }
        console.log('---');

        // Update the token document with all the new information
        const updateFields = {
          ammInfo: ammInfo,
          lastAMMUpdate: new Date(), // Ensure this is set only once
          spotPrice: spotPriceBN.toFixed(), // Use toFixed() to ensure decimal notation
          pricePerXRP: pricePerXRPBN.toFixed(), // Use toFixed()
          marketCap: marketCapBN.toFixed(), // Use toFixed()
          priceHistory: updatedPriceHistory,
          priceChange1h: priceChange1h.toFixed(2),
          priceChange24h: priceChange24h.toFixed(2),
          priceChange7d: priceChange7d.toFixed(2),
        };

        if (kingOfTheHill) {
          updateFields.kingOfTheHill = kingOfTheHill;
        }

        // Avoid duplicate 'lastUpdated' fields
        if (token.lastUpdated) {
          updateFields.lastUpdated = token.lastUpdated;
        } else {
          updateFields.lastUpdated = new Date();
        }

        await collection.updateOne(
          { _id: token._id },
          {
            $set: updateFields
          }
        );

        console.log(`Updated AMM info for ${token.symbol}`);
      } catch (error) {
        console.error(`Error fetching AMM info for ${token.symbol}:`, error);
      }

      // Handle rate limiting dynamically
      await handleRateLimiting();
    }

    console.log(
      "AMM information, prices, market caps, price changes, and 'King of the Hill' status updated successfully"
    );
  } catch (error) {
    console.error("Error during AMM information update:", error);
  } finally {
    console.timeEnd(uniqueLabel);
  }
}

// Function to calculate price change over a specified number of hours
function calculatePriceChange(priceHistory, hoursAgo) {
  const now = Date.now();
  const targetTime = now - hoursAgo * 60 * 60 * 1000;

  // Find the oldest price entry after the target time
  const pastPriceEntry = priceHistory.find(
    (entry) => new Date(entry.timestamp).getTime() >= targetTime
  );

  // If we have enough data, calculate the percentage change
  if (pastPriceEntry) {
    const pastPrice = new BigNumber(pastPriceEntry.spotPrice);
    const currentPrice = new BigNumber(priceHistory[priceHistory.length - 1].spotPrice);
    if (pastPrice.isZero()) {
      return new BigNumber(0);
    }
    const priceChange = currentPrice.minus(pastPrice).dividedBy(pastPrice).multipliedBy(100);
    return priceChange;
  } else {
    // Not enough data to calculate price change
    return new BigNumber(0);
  }
}

// Export the function
module.exports = {
  updateAMMInfo
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
