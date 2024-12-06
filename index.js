const fs = require('fs');
const csv = require('csv-parser');

const paths = {
  plans: './slcsp/plans.csv',
  zips: './slcsp/zips.csv',
  slcp: './slcsp/slcsp.csv',
  output: './slcsp/slcsp_complete.csv',
};

async function main() {
  try {
    // Load data
    const silverPlanData = await parsePlans(paths.plans);
    const zipRateAreaData = await parseZips(paths.zips);

    // Preprocess silver plans (sort the Set values into arrays)
    preprocessPlanRates(silverPlanData);

    // Process SLCSP data
    const slcspData = await processSLCSP(paths.slcp, silverPlanData, zipRateAreaData);

    // Write the SLCSP output
    writeCSVFile(slcspData, paths.output);

  } catch (error) {
    console.error('Error in main process:', error);
  }
}

/**
 * Reads the plans CSV and maps Silver plans to rate areas.
 */
function parsePlans(filePath) {
  return parseCSV(filePath, (row, data) => {
    const { metal_level, rate, rate_area } = row;
    if (metal_level === "Silver") {
      const area = parseInt(rate_area);
      const parsedRate = parseFloat(rate);

      if (!data[area]) {
        data[area] = new Set();
      }
      data[area].add(parsedRate);
    }
  });
}

/**
 * Reads the zips CSV and maps zip codes to rate areas, sets ensure no duplicate rate areas stored.
 */
function parseZips(filePath) {
  return parseCSV(filePath, (row, data) => {
    const { zipcode, rate_area } = row;
    const area = parseInt(rate_area);

    if (!data[zipcode]) {
      data[zipcode] = new Set();
    }
    data[zipcode].add(area);
  });
}

/**
 * Generalized CSV parser
 */
function parseCSV(filePath, processRow) {
  return new Promise((resolve, reject) => {
    const data = {};
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => processRow(row, data))
      .on('end', () => resolve(data))
      .on('error', reject);
  });
}

/**
 * Preprocesses the plan data by converting `Set` values to sorted arrays.
 * Set values ensure that the second index is the second lowest cost plan in the event of duplicate cost plans 
 */
function preprocessPlanRates(planData) {
  Object.keys(planData).forEach((key) => {
    planData[key] = Array.from(planData[key]).sort((a, b) => a - b);
  });
}

/**
 * Reads and processes the SLCSP CSV, picks second lowest cost option if parameters are met:
 * 1. The zipcode has only 1 associated rate area 
 * 2. The the rate area has more than one plan cost 
 * 
 * Data is console logged as it is processed in the expected output CSV format 
 */
function processSLCSP(filePath, planData, zipData) {
  console.log("zipcode, rate")
  return parseCSV(filePath, (row, data) => {
    const zipcode = row.zipcode;
    const rateAreas = zipData[zipcode];
    let rate = -1; // Initialize as "null" value 

    if (rateAreas && rateAreas.size === 1) {
      const [rateArea] = rateAreas; // Extract single rate area
      if (planData[rateArea] && planData[rateArea].length > 1) {
        rate = planData[rateArea][1].toFixed(2); // Get the second lowest rate
      }
    }
    
    console.log(`${zipcode}, ${rate === -1 ? "" : rate}`)
    data[zipcode] = rate;
  });
}

/**
 * Writes the final SLCSP data to a new CSV file.
 */
function writeCSVFile(data, filePath) {
  const header = 'zipcode,rate';
  const rows = Object.entries(data).map(
    ([zipcode, rate]) => `${zipcode},${rate === -1 ? '' : rate}`
  );
  const csvContent = [header, ...rows].join('\n');

  fs.writeFileSync(filePath, csvContent, 'utf8');
  console.log(`CSV file has been created at ${filePath}`);
}

main();