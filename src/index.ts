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

async function* mutations(faros: FarosClient): AsyncGenerator<Mutation> {
  // The QueryBuilder manages the origin for you
  const qb = new QueryBuilder(origin);

  // EXAMPLE 1: Construct your models manually, yielding mutations...
  const compute_Application = {
    name: '<application_name>',
    platform: '<application_platform>',
  };
  const cicd_Deployment = {
    uid: '<deployment_uid',
    source: '<deployment_source>',
    // Fields that reference another model need to be refs
    application: qb.ref({compute_Application}),
    status: {
      category: 'Success',
      detail: '<status_detail>',
    },
  };
  // Yield mutations, Batching will be handled for you
  yield qb.upsert({compute_Application});
  yield qb.upsert({cicd_Deployment});

  // EXAMPLE 2: Iterate across all rows in a CSV file, yielding mutations...
  for await (const row of csvReadRows('../resources/example.csv')) {
    const compute_Application = {
      name: row.application,
      platform: '',
    };
    const cicd_Deployment = {
      uid: row.deployment_id,
      source: row.deployment_source,
      application: qb.ref({compute_Application}),
      status: {
        category: row.status,
        detail: '',
      },
    };
    yield qb.upsert({compute_Application});
    yield qb.upsert({cicd_Deployment});
  }

  // EXAMPLE 3: Iterate across a Faros graph query, yielding mutations...
  const query = `
  {
    cicd_Deployment {
      uid
      source
      application {
        name
        platform
      }
      status
    }
  }
  `;
  const farosNodes = farosReadNodes(faros, graph, query);
  for await (const node of farosNodes) {
    const compute_Application = {
      name: node.application.name,
      platform: node.application.platform,
    };
    const cicd_Deployment = {
      uid: node.uid,
      source: node.source,
      application: qb.ref({compute_Application}),
      status: {
        category: node.status.category,
        detail: node.status.detail,
      },
    };
    yield qb.upsert({compute_Application});
    yield qb.upsert({cicd_Deployment});
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
