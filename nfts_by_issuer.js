const xrpl = require('xrpl');

async function getNFTsByIssuer(issuer) {
  const nodes = [
    'wss://xrplcluster.com',
    'wss://s1.ripple.com',
    'wss://s2.ripple.com'
  ];

  for (const node of nodes) {
    const client = new xrpl.Client(node);

    try {
      await client.connect();
      console.log(`Connected to XRPL node: ${node}`);

      let allNFTs = [];
      let marker = null;
      let pageCount = 0;

      do {
        const request = {
          method: 'nfts_by_issuer',
          issuer: issuer,
          limit: 100 // You can adjust this limit as needed
        };

        if (marker) {
          request.marker = marker;
        }

        const response = await client.request(request);
        pageCount++;

        if (response.result.nfts) {
          allNFTs = allNFTs.concat(response.result.nfts);
          console.log(`Retrieved ${response.result.nfts.length} NFTs on page ${pageCount}. Total: ${allNFTs.length}`);
        }

        marker = response.result.marker;

      } while (marker);

      console.log(`Total NFTs retrieved: ${allNFTs.length}`);
      console.log('All NFTs:', JSON.stringify(allNFTs, null, 2));

      // If we reach this point without errors, we can break the loop
      break;

    } catch (error) {
      console.error(`Error with node ${node}:`, error.message);
      if (error.data && error.data.error === 'unknownCmd') {
        console.log(`The 'nfts_by_issuer' method is not supported by this node. Trying next node...`);
      } else {
        console.log('Unexpected error. Trying next node...');
      }
    } finally {
      await client.disconnect();
      console.log(`Disconnected from XRPL node: ${node}`);
    }
  }
}

const issuerAddress = 'rJeBz69krYh8sXb8uKsEE22ADzbi1Z4yF2';
getNFTsByIssuer(issuerAddress);