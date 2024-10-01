// Fetches  volume from XRPLF

const { MongoClient } = require("mongodb");
const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));
const Bottleneck = require("bottleneck");

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
  limit: 600,
  remaining: 600,
  reset: Date.now() + 60 * 1000,
};

// Helper function to wait for a specified number of milliseconds
function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Initialize Bottleneck limiter
const limiter = new Bottleneck({
  reservoir: rateLimit.remaining,
  reservoirRefreshAmount: rateLimit.limit,
  reservoirRefreshInterval: 60 * 1000,
  maxConcurrent: 10,
  minTime: 100,
});

// Function to update rate limit state based on response headers
function updateRateLimit(headers) {
  const limit = parseInt(headers.get('X-Ratelimit-Limit'));
  const remaining = parseInt(headers.get('X-Ratelimit-Remaining'));
  const reset = parseInt(headers.get('X-Ratelimit-Reset'));

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
      reservoirRefreshInterval: refreshInterval > 0 ? refreshInterval : 60 * 1000,
    });

    console.log(`Rate Limit Updated: Limit=${rateLimit.limit}, Remaining=${rateLimit.remaining}, Reset in=${Math.ceil((rateLimit.reset - Date.now()) / 1000)} seconds`);
  }
}

// Updated function to fetch volume data
async function fetchVolumeData(base, interval, start, end) {
  const url = `https://data.xrplf.org/v1/iou/volume_data/${base}?interval=${interval}&start=${start}&end=${end}&exclude_amm=false`;
  try {
    await handleRateLimiting();
    const response = await limiter.schedule(() => fetchWithRetry(url));
    if (!response) {
      throw new Error('No response received from fetchWithRetry');
    }
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    updateRateLimit(response.headers);
    return response.json();
  } catch (error) {
    console.error(`Error fetching volume data for ${base}:`, error);
    throw error;
  }
}

// Modified fetchWithRetry to handle rate limiting using headers and implement exponential backoff
async function fetchWithRetry(url, retries = 3, delayMs = 1000) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetch(url);

      updateRateLimit(response.headers);

      if (response.status === 429) {
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

      if (rateLimit.remaining > 0) {
        rateLimit.remaining -= 1;
      }

      return response;
    } catch (error) {
      console.error(`Fetch attempt ${attempt + 1} failed:`, error);
      if (attempt < retries - 1) {
        const backoffTime = delayMs * Math.pow(2, attempt);
        console.log(`Retrying in ${backoffTime} ms...`);
        await wait(backoffTime);
      } else {
        throw error;
      }
    }
  }
}

// Function to handle rate limiting dynamically
async function handleRateLimiting() {
  const now = Date.now();
  if (rateLimit.remaining <= 0) {
    const waitTime = rateLimit.reset - now;
    if (waitTime > 0) {
      console.warn(`Rate limit exceeded. Waiting for ${Math.ceil(waitTime / 1000)} seconds.`);
      await wait(waitTime);
    }
    limiter.updateSettings({
      reservoir: rateLimit.limit,
      reservoirRefreshAmount: rateLimit.limit,
      reservoirRefreshInterval: 60 * 1000,
    });
  }
}

async function updateVolumeData() {
  const uniqueLabel = `Volume data update ${Date.now()}`;
  console.time(uniqueLabel);
  let updatedCount = 0;
  let errorCount = 0;

  try {
    const { db } = await connectToDatabase();
    const collection = db.collection(COLLECTION_NAME);

    const tokens = await collection
      .find({}, { projection: { issuer: 1, currencyCode: 1 } })
      .toArray();

    console.log(`Starting volume update for ${tokens.length} tokens`);

    const batchSize = 50;
    const batches = Math.ceil(tokens.length / batchSize);

    for (let i = 0; i < batches; i++) {
      const batchTokens = tokens.slice(i * batchSize, (i + 1) * batchSize);
      const updatePromises = batchTokens.map(async (token) => {
        if (token.issuer && token.currencyCode) {
          const base = `${token.issuer}_${token.currencyCode}`;
          const end = new Date().toISOString();
          const start = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(); // Request 7 days

          try {
            await handleRateLimiting();
            const volumeData = await fetchVolumeData(base, "1d", start, end);
            if (volumeData && Array.isArray(volumeData) && volumeData.length >= 1) {
              // Assuming data is ordered from oldest to newest, take last 7 entries
              const last7DaysData = volumeData.slice(-7);

              const lastDayData = last7DaysData[last7DaysData.length - 1];
              const totalVolume7d = last7DaysData.reduce((sum, day) => sum + (isNaN(day.volume) ? 0 : day.volume), 0);
              const totalExchanges7d = last7DaysData.reduce((sum, day) => sum + (isNaN(day.exchanges) ? 0 : day.exchanges), 0);
              const totalDistinctPairs7d = last7DaysData.reduce((sum, day) => sum + (isNaN(day.distinct_pairs) ? 0 : day.distinct_pairs), 0);

              await collection.updateOne(
                { _id: token._id },
                {
                  $set: {
                    volume24h: lastDayData.volume,
                    exchanges24h: lastDayData.exchanges,
                    distinctPairs24h: lastDayData.distinct_pairs,
                    volume7d: totalVolume7d,
                    exchanges7d: totalExchanges7d,
                    distinctPairs7d: totalDistinctPairs7d,
                  },
                }
              );
              updatedCount++;
              console.log(`Updated volume data for ${token.currencyCode}`);
            } else {
              console.warn(`Insufficient volume data for ${token.currencyCode}`);
            }
          } catch (error) {
            console.error(
              `Error fetching volume data for token ${token.currencyCode}:`,
              error
            );
            errorCount++;
          }
        }
      });

      await Promise.all(updatePromises);
      console.log(`Processed batch ${i + 1} of ${batches}`);
    }

    console.log(
      `Volume data update completed. Updated: ${updatedCount}, Errors: ${errorCount}`
    );
  } catch (error) {
    console.error("Error during volume data update:", error);
  } finally {
    console.timeEnd(uniqueLabel);
  }
}

// Export the function
module.exports = {
  updateVolumeData,
};

// Main function to include more detailed logging and prevent overlapping runs
async function main() {
  try {
    await fetch;

    console.log("Starting initial volume update...");
    await updateVolumeData();
    console.log("Initial volume update completed successfully");

    const scheduleNextUpdate = async () => {
      try {
        console.log("Starting periodic volume update...");
        await updateVolumeData();
        console.log("Periodic volume update completed successfully");
      } catch (error) {
        console.error("Error during periodic volume update:", error);
      } finally {
        setImmediate(scheduleNextUpdate);
      }
    };

    scheduleNextUpdate();
    console.log(
      "Volume update loop started. Will run updates continuously without delay."
    );
  } catch (error) {
    console.error("Error during initial volume update:", error);
  }
}

// Run the main function if this file is being run directly
if (require.main === module) {
  (async () => {
    await main();
  })();
}
