const fetch = (...args) =>
  import('node-fetch').then(({ default: fetch }) => fetch(...args));
const { MongoClient } = require('mongodb');

// MongoDB connection details
const MONGODB_URI =
  process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017';
const DB_NAME = process.env.DB_NAME || 'xrpFun';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'coins';

// Function to encode currency codes to 40-character hex strings if necessary
function encodeCurrencyCode(currencyCode) {
  // Check if the currency code is exactly 3 uppercase letters
  if (/^[A-Z]{3}$/.test(currencyCode)) {
    // Use currency code as-is for standard codes
    return currencyCode;
  } else if (/^[A-F0-9]{40}$/i.test(currencyCode)) {
    // Currency code is already a 40-character hex string
    return currencyCode.toUpperCase();
  } else {
    // Convert to hex and pad to 40 characters
    let hexCode = Buffer.from(currencyCode, 'ascii').toString('hex').toUpperCase();
    while (hexCode.length < 40) {
      hexCode += '0';
    }
    return hexCode;
  }
}

async function updateMarketData() {
  const client = new MongoClient(MONGODB_URI);

  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    // Fetch all tokens from the database
    const tokens = await collection.find({}).toArray();

    // Intervals to fetch data for
    const intervals = [
      '1m',
      '3m',
      '5m',
      '15m',
      '30m',
      '45m',
      '1h',
      '2h',
      '3h',
      '4h',
      '1d', // Added '1d' interval for daily data
    ];

    // Define the start date for historical data (e.g., 30 days ago)
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 30);
    const formattedStartDate = startDate.toISOString().split('.')[0] + 'Z';

    for (const token of tokens) {
      // Encode currency code if necessary
      const currencyCodeEncoded = encodeCurrencyCode(token.currencyCode);
      const baseIdentifier = `${token.issuer}_${currencyCodeEncoded}`;
      console.log(`Processing token: ${token.symbol}`);
      console.log(`Base identifier: ${baseIdentifier}`);

      // Object to store chart data for different intervals
      const chartData = {};

      for (const interval of intervals) {
        console.log(`Fetching historical data for ${token.symbol} at interval ${interval}`);
        const apiUrl = `https://data.xrplf.org/v1/iou/ticker_data/${baseIdentifier}/XRP?interval=${interval}&date=${formattedStartDate}&only_amm=false`;

        const response = await fetch(apiUrl);

        if (!response.ok) {
          console.error(`Error fetching data for ${token.symbol} at interval ${interval}: ${response.statusText}`);
          continue;
        }

        const data = await response.json();

        if (data.length > 0) {
          // Store data in chartData for this interval
          chartData[interval] = data;
        } else {
          console.log(`No data found for ${token.symbol} at interval ${interval}`);
        }
      }

      // Use the latest data point from '1m' interval to update xrpPrice and King of the Hill
      if (chartData['1m'] && chartData['1m'].length > 0) {
        const latestDataPoint = chartData['1m'][chartData['1m'].length - 1];

        const xrpPrice = latestDataPoint.last;
        const marketCap = xrpPrice * parseFloat(token.totalSupply);

        // Prepare update object
        const updateFields = {
          xrpPrice,
          marketCap,
          lastUpdated: new Date(),
          base_volume: latestDataPoint.base_volume,
          counter_volume: latestDataPoint.counter_volume,
          open: latestDataPoint.first,
          high: latestDataPoint.high,
          low: latestDataPoint.low,
          close: latestDataPoint.last,
          exchanges: latestDataPoint.exchanges,
          timestamp: new Date(latestDataPoint.date_to),
        };

        // Check for "King of the Hill" status
        if (marketCap >= 58900 && !token.kingOfTheHill) {
          updateFields.kingOfTheHill = {
            label: 'King of the hill',
            timestamp: new Date(),
          };
        }

        // Update the token in the database
        await collection.updateOne(
          { _id: token._id },
          {
            $set: updateFields,
          }
        );

        console.log(
          `Updated ${token.symbol}: Price=${xrpPrice} XRP, Market Cap=${marketCap} XRP`
        );
      } else {
        console.log(`No 1m market data found for ${token.symbol} (${baseIdentifier})`);
      }

      // Update the token with historical chart data
      if (Object.keys(chartData).length > 0) {
        await collection.updateOne(
          { _id: token._id },
          {
            $set: { chartData },
          }
        );
        console.log(`Updated chart data for ${token.symbol}`);
      }
    }

    console.log('Market data update completed');
  } catch (error) {
    console.error('Error updating market data:', error);
  } finally {
    await client.close();
  }
}

// Run the update function
updateMarketData();
