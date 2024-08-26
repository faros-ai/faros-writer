import {
  batchMutation,
  FarosClient,
  Mutation,
  QueryBuilder,
} from 'faros-js-client';

import {csvReadRows, farosReadNodes} from './utils';

const faros_api_url = process.env.FAROS_API_URL || 'https://prod.api.faros.ai';
const faros_api_key = process.env.FAROS_API_KEY || '<key>';
const graph = process.env.FAROS_GRAPH || 'default';
const origin = process.env.FAROS_ORIGIN || 'faros-writer';
const maxBatchSize = Number(process.env.FAROS_BATCH_SIZE) || 500;
const debug = (process.env.FAROS_DEBUG || 'true') === 'true';

function getUid(row) {
  if (row['identifier'] !== '') {
    return row['identifier'];
  }
  return row['Severity'] + '-' + row['Impact Window'];
}

function getEndDate(startDateStr, endDateStr) {
  // Original date object
  const originalDate = new Date(startDateStr);

  // New time you want to use (e.g., 09:45:00)
  const [hours, minutes] = endDateStr.split(':').map(Number);

  // Extract date parts from the original date
  const year = originalDate.getFullYear();
  const month = originalDate.getMonth(); // Note: getMonth() returns 0-indexed month
  const day = originalDate.getDate();

  // Create a new Date object with the original date and the new time
  const updatedDate = new Date(year, month, day, hours, minutes, 0, 0);
  
  return updatedDate;
}

function getStartEndDates(str) {
  if (str.includes('→')) {
    const [startDateStr, endDateStr] = str.split('→').map(s => s.trim());
    const start = new Date(startDateStr);
    let end;
    if (/^\d{1,2}:\d{2}$/.test(endDateStr)) {
      end = getEndDate(startDateStr, endDateStr);
    } else {
      // Parse the end date
      end = new Date(endDateStr);
    }

    return { start, end };
  } 
  
  return { start: new Date(str), end: null };
}

async function* mutations(faros: FarosClient): AsyncGenerator<Mutation> {
  // The QueryBuilder manages the origin for you
  const qb = new QueryBuilder(origin);

  // EXAMPLE 2: Iterate across all rows in a CSV file, yielding mutations...
  for await (const row of csvReadRows('../resources/incidents.csv')) {
    const {start, end} = getStartEndDates(row['Impact Window']);

    const ims_Incident = {
      uid: getUid(row),
      severity: row.Severity,
      status: row.Status,
      startedAt: start.toUTCString(),
      url: row['Asana Link'],
    };

    if (end !== null) {
      ims_Incident['resolvedAt'] = end.toUTCString();
    }

    yield qb.upsert({ims_Incident});
  }
}

async function sendToFaros(
  faros: FarosClient,
  graph,
  batch: Mutation[]
): Promise<void> {
  if (debug) {
    console.log(batchMutation(batch));
  } else {
    console.log(`Sending...`);
    await faros.sendMutations(graph, batch);
    console.log(`Done.`);
  }
}

async function main(): Promise<void> {
  console.log(`Debug: ${debug ? 'Enabled' : 'Disabled'}`)
  console.log(`URL: ${faros_api_url}, Graph: ${graph}, Origin: ${origin}`);

  const faros = new FarosClient({
    url: faros_api_url,
    apiKey: faros_api_key,
  });

  let batchNum = 1;
  let batch: Mutation[] = [];
  for await (const mutation of mutations(faros)) {
    if (batch.length >= maxBatchSize) {
      console.log(`------ Batch ${batchNum} - Size: ${batch.length} ------`);
      await sendToFaros(faros, graph, batch);
      batchNum++;
      batch = [];
    }
    batch.push(mutation);
  }

  if (batch.length > 0) {
    console.log(`------ Batch ${batchNum} - Size: ${batch.length} ------`);
    await sendToFaros(faros, graph, batch);
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.log(`Failed to send to Faros ${err}`);
  });
}
