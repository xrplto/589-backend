// api/updateData
// GET
// Updates the prices and market caps of all tokens

const { MongoClient, ObjectId } = require("mongodb");
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

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
            const response = await fetch(url);
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
// Add a global rate limit tracker
let requestsMade = 0;
let rateLimitResetTime = Date.now() + 3600000; // 1 hour from now

// Modify the fetchWithRetry function
async function fetchWithRetry(url, retries = 3, delay = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      // Check global rate limit
      if (requestsMade >= 10000 && Date.now() < rateLimitResetTime) {
        const waitTime = rateLimitResetTime - Date.now();
        console.log(`Global rate limit reached. Waiting for ${waitTime}ms before next request.`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
        requestsMade = 0;
        rateLimitResetTime = Date.now() + 3600000;
      }

      const response = await fetch(url, {
        headers: {
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      // Update global rate limit tracker
      requestsMade++;

      // Handle rate limiting headers
      const remainingRequests = parseInt(response.headers.get('x-ratelimit-remaining'));
      const resetTime = parseInt(response.headers.get('x-ratelimit-reset'));

      if (remainingRequests <= 1) {
        console.log(`Rate limit nearly reached. Waiting for ${resetTime} seconds.`);
        await new Promise(resolve => setTimeout(resolve, resetTime * 1000));
      }

      return response;
    } catch (error) {
      if (error.message.includes('429')) {
        console.log(`Rate limit exceeded. Retrying after ${delay}ms.`);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      if (i === retries - 1) throw error;
      await new Promise(resolve => setTimeout(resolve, delay * (i + 1))); // Exponential backoff
    }
  }
}

// Modify the updatePricesAndMarketCaps function
async function updatePricesAndMarketCaps() {
  console.time('Database update');
  const { db } = await connectToDatabase();
  const collection = db.collection(COLLECTION_NAME);

  const tokens = await collection.find({}, { projection: { issuer: 1, currencyCode: 1, totalSupply: 1, kingOfTheHill: 1 } }).toArray();
  
  console.log(`Starting update for ${tokens.length} tokens`);

  const batchSize = 50;
  const batches = Math.ceil(tokens.length / batchSize);

  for (let i = 0; i < batches; i++) {
    const batchTokens = tokens.slice(i * batchSize, (i + 1) * batchSize);
    const updatePromises = batchTokens.map(async (token) => {
      if (token.issuer && token.currencyCode) {
        const base = `${token.issuer}_${token.currencyCode}`;
        const counter = "XRP";
        const url = `https://data.xrplf.org/v1/iou/exchange_rates/${base}/${counter}`;

        try {
          const response = await fetchWithRetry(url);
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

    // Add a delay between batches to avoid hitting rate limits
    if (i < batches - 1) {
      await new Promise(resolve => setTimeout(resolve, 1000)); // 1 second delay
    }
  }

  console.timeEnd('Database update');
  console.log("Prices, market caps, last updated times, and 'King of the hill' status updated successfully");
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