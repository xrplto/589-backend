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
    let hexCode = Buffer.from(currencyCode, 'ascii')
      .toString('hex')
      .toUpperCase();
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
      '5m',
      '15m',
      '30m',
      '1h',
      '4h',
      '1d',
      '1M',
    ];

    // Mapping of intervals to maximum days of history
    const intervalHistoryDays = {
      '1m': 1,      // last 1 day
      '5m': 3,      // last 3 days
      '15m': 7,     // last 7 days
      '30m': 14,    // last 14 days
      '1h': 30,     // last 30 days
      '4h': 90,     // last 90 days
      '1d': 365,    // last 1 year
      '1M': 1825,   // last 5 years
    };

    // Define the end date as now
    const endDate = new Date();
    const formattedEndDate = endDate.toISOString().split('.')[0] + 'Z';

    for (const token of tokens) {
      // Encode currency code if necessary
      const currencyCodeEncoded = encodeCurrencyCode(token.currencyCode);

      // Construct base identifier
      let base;
      if (token.currencyCode === 'XRP') {
        base = 'XRP';
      } else {
        base = `${token.issuer}_${currencyCodeEncoded}`;
      }

      // Counter currency
      const counter = 'XRP';

      console.log(`Processing token: ${token.symbol}`);
      console.log(`Base identifier: ${base}`);

      // Object to store chart data for different intervals
      const chartData = {};

      for (const interval of intervals) {
        console.log(
          `Fetching historical data for ${token.symbol} at interval ${interval}`
        );

        // Calculate start date based on interval
        const historyDays = intervalHistoryDays[interval] || 30; // default to last 30 days
        const startDate = new Date();
        startDate.setDate(endDate.getDate() - historyDays);
        const formattedStartDate = startDate.toISOString().split('.')[0] + 'Z';

        // Initialize variables for pagination
        let skip = 0;
        const limit = 500; // Adjust as necessary
        let hasMoreData = true;
        chartData[interval] = [];

        while (hasMoreData) {
          // Construct API URL with pagination
          const apiUrl = `https://data.xrplf.org/v1/iou/market_data/${base}/${counter}?interval=${interval}&start=${formattedStartDate}&end=${formattedEndDate}&exclude_amm=false&descending=false&skip=${skip}&limit=${limit}`;
          console.log(`Fetching data from URL: ${apiUrl}`);

          try {
            const response = await fetch(apiUrl);

            if (!response.ok) {
              console.error(
                `Error fetching data for ${token.symbol} at interval ${interval}: ${response.statusText}`
              );
              hasMoreData = false;
              break;
            }

            const data = await response.json();

            if (Array.isArray(data) && data.length > 0) {
              // Append data to chartData for this interval
              chartData[interval] = chartData[interval].concat(data);
              skip += data.length;
            } else {
              hasMoreData = false;
            }

            // If the number of returned entries is less than the limit, we've reached the end
            if (!Array.isArray(data) || data.length < limit) {
              hasMoreData = false;
            }
          } catch (error) {
            console.error(
              `Error fetching data for ${token.symbol} at interval ${interval}:`,
              error
            );
            hasMoreData = false;
          }
        }

        if (chartData[interval].length === 0) {
          console.log(
            `No data found for ${token.symbol} at interval ${interval}`
          );
          delete chartData[interval]; // Remove empty intervals
        }
      }

      // Use the latest data point from '1m' interval to update xrpPrice and King of the Hill
      if (chartData['1m'] && chartData['1m'].length > 0) {
        // Select the latest data point
        const latestDataPoint =
          chartData['1m'][chartData['1m'].length - 1];

        const xrpPrice = latestDataPoint.close;
        const marketCap = xrpPrice * parseFloat(token.totalSupply);

        // Prepare update object
        const updateFields = {
          xrpPrice,
          marketCap,
          lastUpdated: new Date(),
          base_volume: latestDataPoint.base_volume,
          counter_volume: latestDataPoint.counter_volume,
          open: latestDataPoint.open,
          high: latestDataPoint.high,
          low: latestDataPoint.low,
          close: latestDataPoint.close,
          exchanges: latestDataPoint.exchanges,
          timestamp: new Date(latestDataPoint.timestamp),
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
          `Updated ${token.symbol}: Price=${xrpPrice}, Market Cap=${marketCap}`
        );
      } else {
        console.log(`No 1m market data found for ${token.symbol} (${base})`);
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
