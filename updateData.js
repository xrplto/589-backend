// api/updateData
// GET
// Updates the prices and market caps of all tokens

const { MongoClient, ObjectId } = require("mongodb");
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));
const Bottleneck = require('bottleneck'); // Add Bottleneck for better rate limiting control

// Use environment variables for sensitive data
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const DB_NAME = process.env.DB_NAME || "xrpFun";
const COLLECTION_NAME = process.env.COLLECTION_NAME || "coins";

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

// Rate Limiting State
let rateLimit = {
  limit: 600,          // Default limit
  remaining: 600,      // Default remaining
  reset: Date.now() + 60 * 60 * 1000, // Default reset time (1 hour from now)
};

// Helper function to wait for a specified number of milliseconds
function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Function to update rate limit state based on response headers
function updateRateLimit(headers) {
  const limit = parseInt(headers.get('X-Ratelimit-Limit'));
  const remaining = parseInt(headers.get('X-Ratelimit-Remaining'));
  const reset = parseInt(headers.get('X-Ratelimit-Reset')); // Assuming reset is in seconds

  if (!isNaN(limit)) rateLimit.limit = limit;
  if (!isNaN(remaining)) rateLimit.remaining = remaining;
  if (!isNaN(reset)) {
    rateLimit.reset = Date.now() + reset * 1000;
  }

  console.log(`Rate Limit Updated: Limit=${rateLimit.limit}, Remaining=${rateLimit.remaining}, Reset in=${Math.ceil((rateLimit.reset - Date.now()) / 1000)} seconds`);
}

// Initialize Bottleneck limiter
const limiter = new Bottleneck({
  reservoir: rateLimit.remaining, // initial number of requests
  reservoirRefreshAmount: rateLimit.limit,
  reservoirRefreshInterval: 60 * 1000, // assume rate limit resets every minute; adjust as needed
  maxConcurrent: 5, // adjust based on your needs
  minTime: 200, // minimum time between requests in ms
});

// Function to handle rate limiting dynamically
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
      reservoirRefreshInterval: 60 * 1000, // adjust based on actual reset time
    });
  }
}

// Modified fetchWithRetry to handle rate limiting using headers
async function fetchWithRetry(url, retries = 3, delayMs = 1000) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      await handleRateLimiting();

      const response = await fetch(url);

      // Update rate limit state based on response headers
      updateRateLimit(response.headers);

      if (response.status === 429) { // Too Many Requests
        const retryAfter = response.headers.get('Retry-After');
        const resetSeconds = parseInt(response.headers.get('X-Ratelimit-Reset'));
        let waitTime;

        if (retryAfter) {
          waitTime = parseInt(retryAfter) * 1000;
          console.warn(`Received 429. Retry-After header found. Retrying after ${waitTime / 1000} seconds.`);
        } else if (!isNaN(resetSeconds)) {
          waitTime = resetSeconds * 1000;
          console.warn(`Received 429. X-Ratelimit-Reset found. Retrying after ${resetSeconds} seconds.`);
        } else {
          waitTime = delayMs;
          console.warn(`Received 429. No specific retry time found. Retrying after ${waitTime / 1000} seconds.`);
        }

        await wait(waitTime);
        continue;
      }

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      // Decrement remaining count
      if (rateLimit.remaining > 0) {
        rateLimit.remaining -= 1;
      }

      return response;
    } catch (error) {
      console.error(`Fetch attempt ${attempt + 1} failed:`, error);
      if (attempt < retries - 1) {
        console.log(`Retrying in ${delayMs} ms...`);
        await wait(delayMs);
      } else {
        throw error;
      }
    }
  }
}

// Modify the GET function to be a regular async function
async function fetchTokens(request) {
  try {
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get("limit")) || 50;
    const page = parseInt(searchParams.get("page")) || 1;
    const sort = searchParams.get("sort") || "upvotes";
    const order = searchParams.get("order") === "asc" ? 1 : -1;

    const { db } = await connectToDatabase();
    const collection = db.collection(COLLECTION_NAME);

    const skip = (page - 1) * limit;
    const sortOption = { [sort]: order };

    // Define the fields to be fetched
    const projection = {
      _id: 1,
      name: 1,
      symbol: 1,
      image: 1,
      totalSupply: 1,
      description: 1,
      date: 1,
      creator: 1,
      issuer: 1,
      currencyCode: 1,
      reactions: 1,
      comment: 1,
      md5: 1,
      ammAddress: 1,
      upvotes: 1,
      downvotes: 1,
      price: 1,
      marketCap: 1,
      lastUpdated: 1, // Add this line
      kingOfTheHill: 1, // Add this line
    };

    // Fetch documents with pagination, sorting, and field projection
    const [tokens, totalCount] = await Promise.all([
      collection
        .find({})
        .project(projection)
        .sort(sortOption)
        .skip(skip)
        .limit(limit)
        .toArray(),
      collection.countDocuments(),
    ]);

    const tokensWithPricesAndMarketCap = await Promise.all(
      tokens.map(async (token) => {
        let updatedToken = { ...token };
        if (token.issuer && token.currencyCode) {
          const base = `${token.issuer}_${token.currencyCode}`;
          const counter = "XRP";
          const url = `https://data.xrplf.org/v1/iou/exchange_rates/${base}/${counter}`;

          try {
            // Use the limiter to schedule requests
            const response = await limiter.schedule(() => fetchWithRetry(url));
            const data = await response.json();
            updatedToken.xrpPrice = data.rate;

            // Calculate market cap
            if (updatedToken.xrpPrice && updatedToken.totalSupply) {
              updatedToken.marketCap =
                updatedToken.xrpPrice * updatedToken.totalSupply;
              updatedToken.lastUpdated = new Date(); // Add this line

              // Update the database with the new market cap and last updated time
              await collection.updateOne(
                { _id: new ObjectId(token._id) },
                {
                  $set: {
                    marketCap: updatedToken.marketCap,
                    lastUpdated: updatedToken.lastUpdated, // Add this line
                  },
                }
              );
            }
          } catch (error) {
            console.error(`Error fetching price for ${token.symbol}:`, error);
          }
        }
        return updatedToken;
      })
    );

    return {
      tokens: tokensWithPricesAndMarketCap,
      totalCount,
      currentPage: page,
      totalPages: Math.ceil(totalCount / limit),
    };
  } catch (error) {
    console.error("Error fetching tokens:", error);
    return { error: "Internal Server Error" };
  }
}

// Modify the function for periodic updates to improve performance
async function updatePricesAndMarketCaps() {
  console.time('Database update');
  const { db } = await connectToDatabase();
  const collection = db.collection(COLLECTION_NAME);

  const tokens = await collection.find({}, { projection: { issuer: 1, currencyCode: 1, totalSupply: 1, kingOfTheHill: 1 } }).toArray();
  
  console.log(`Starting update for ${tokens.length} tokens`);

  const batchSize = 50; // Adjust this value based on your system's capabilities
  const batches = Math.ceil(tokens.length / batchSize);

  for (let i = 0; i < batches; i++) {
    const batchTokens = tokens.slice(i * batchSize, (i + 1) * batchSize);
    const updatePromises = batchTokens.map(async (token) => {
      if (token.issuer && token.currencyCode) {
        const base = `${token.issuer}_${token.currencyCode}`;
        const counter = "XRP";
        const url = `https://data.xrplf.org/v1/iou/exchange_rates/${base}/${counter}`;

        try {
          // Use the limiter to schedule requests
          const response = await limiter.schedule(() => fetchWithRetry(url));
          const data = await response.json();
          const xrpPrice = data.rate;

          if (xrpPrice && token.totalSupply) {
            const marketCap = xrpPrice * token.totalSupply;
            const lastUpdated = new Date();

            const updateFields = {
              marketCap,
              lastUpdated,
              xrpPrice,
            };

            if (marketCap >= 58900 && !token.kingOfTheHill) {
              updateFields.kingOfTheHill = {
                label: "King of the hill",
                timestamp: new Date(),
              };
            }

            return {
              updateOne: {
                filter: { _id: token._id },
                update: { $set: updateFields },
              },
            };
          }
        } catch (error) {
          console.error(`Error fetching price for token ${token._id}:`, error);
        }
      }
      return null;
    });

    const updates = (await Promise.all(updatePromises)).filter(Boolean);

    if (updates.length > 0) {
      await collection.bulkWrite(updates);
    }

    console.log(`Processed batch ${i + 1} of ${batches}`);
  }

  console.timeEnd('Database update');
  console.log(
    "Prices, market caps, last updated times, and 'King of the hill' status updated successfully"
  );
}

// Export the functions
module.exports = {
  updatePricesAndMarketCaps,
  fetchTokens,
};

// Modify the main function to include more detailed logging
async function main() {
  try {
    // Ensure fetch is available before starting the update loop
    await fetch;
    
    console.log("Starting initial update...");
    await updatePricesAndMarketCaps();
    console.log("Initial update completed successfully");

    // Set up interval to run every 15 seconds
    setInterval(async () => {
      try {
        console.log("Starting periodic update...");
        await updatePricesAndMarketCaps();
        console.log("Periodic update completed successfully");
      } catch (error) {
        console.error("Error during periodic update:", error);
      }
    }, 15000); // 15000 milliseconds = 15 seconds

    console.log("Update loop started. Will run every 15 seconds.");
  } catch (error) {
    console.error("Error during initial update:", error);
  }
}

// Run the main function if this file is being run directly
if (require.main === module) {
  (async () => {
    await main();
  })();
}
