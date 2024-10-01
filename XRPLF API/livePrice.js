const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));
const { MongoClient } = require('mongodb');

// MongoDB connection details
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const DB_NAME = process.env.DB_NAME || "xrpFun";
const COLLECTION_NAME = process.env.COLLECTION_NAME || "coins";

async function updateMarketData() {
  const client = new MongoClient(MONGODB_URI);

  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    // Fetch all tokens from the database
    const tokens = await collection.find({}).toArray();

    // Fetch data from XRPLF API
    const response = await fetch('https://data.xrplf.org/v1/iou/ticker_data/all?interval=1m&only_amm=false');
    const data = await response.json();

    console.log(`Fetched ${data.length} items from XRPLF API`);

    for (const token of tokens) {
      // Use currencyCode directly as it's already in hex
      const baseIdentifier = `${token.issuer}_${token.currencyCode}`;

      console.log(`Searching for token: ${token.symbol}`);
      console.log(`Base identifier: ${baseIdentifier}`);

      // Find matching market data with XRP as counter
      const marketData = data.find(item => 
        item.base === baseIdentifier && item.counter === "XRP"
      );

      if (marketData) {
        console.log("Full market data object found:");
        console.log(JSON.stringify(marketData, null, 2));

        const xrpPrice = marketData.last;
        const marketCap = xrpPrice * parseFloat(token.totalSupply);

        // Prepare update object
        const updateFields = {
          xrpPrice,
          marketCap,
          lastUpdated: new Date(),
          base_volume: marketData.base_volume,
          base_volume_buy: marketData.base_volume_buy,
          base_volume_sell: marketData.base_volume_sell,
          counter_volume: marketData.counter_volume,
          counter_volume_buy: marketData.counter_volume_buy,
          counter_volume_sell: marketData.counter_volume_sell,
          open: marketData.first,
          high: marketData.high,
          low: marketData.low,
          close: marketData.last,
          exchanges: marketData.exchanges,
          unique_buyers: marketData.unique_buyers,
          unique_sellers: marketData.unique_sellers,
          timestamp: new Date(marketData.date_to)
        };

        // Check for "King of the Hill" status
        if (marketCap >= 58900 && !token.kingOfTheHill) {
          updateFields.kingOfTheHill = {
            label: "King of the hill",
            timestamp: new Date()
          };
        }

        // Update the token in the database
        await collection.updateOne(
          { _id: token._id },
          { $set: updateFields }
        );

        console.log(`Updated ${token.symbol}: Price=${xrpPrice} XRP, Market Cap=${marketCap} XRP`);
      } else {
        console.log(`No XRP market data found for ${token.symbol} (${baseIdentifier})`);
        
        // Log all entries for this token's baseIdentifier
        const allEntries = data.filter(item => item.base === baseIdentifier);
        if (allEntries.length > 0) {
          console.log(`Found ${allEntries.length} entries for this token, but none with XRP as counter:`);
          console.log(JSON.stringify(allEntries, null, 2));
        } else {
          console.log("No entries found for this token's baseIdentifier");
        }

        // Check if there's any close match with XRP as counter
        const closeMatches = data.filter(item => 
          (item.base.includes(token.issuer) || item.base.includes(token.currencyCode)) && item.counter === "XRP"
        );
        if (closeMatches.length > 0) {
          console.log("Close matches found with XRP as counter:");
          console.log(JSON.stringify(closeMatches, null, 2));
        }
      }
    }

    console.log("Market data update completed");
  } catch (error) {
    console.error("Error updating market data:", error);
  } finally {
    await client.close();
  }
}

// Run the update function
updateMarketData();
