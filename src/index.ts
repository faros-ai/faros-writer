import {
  batchMutation,
  FarosClient,
  Mutation,
  QueryBuilder,
} from 'faros-js-client';

import {csvReadRows, farosReadNodes} from './utils';

const faros_api_url = process.env.FAROS_API_URL || 'https://prod.api.faros.ai';
const faros_api_key = process.env.FAROS_API_KEY || '<key>';
const graph = process.env.FAROS_GRAPH || 'control-tower';
const origin = process.env.FAROS_ORIGIN || 'faros-writer';
const maxBatchSize = Number(process.env.FAROS_BATCH_SIZE) || 500;
const debug = (process.env.FAROS_DEBUG || 'true') === 'true';

async function* mutations(faros: FarosClient): AsyncGenerator<Mutation> {
  // The QueryBuilder manages the origin for you
  const qb = new QueryBuilder(origin);

  const faros_MetricDefinition = {
    uid: 'monthly-story-points',
    name: 'Monthly Story Points',
    description: 'The number of story points delivered in a specific calendar month',
    valueType: {
      category: 'Numeric',
    },
    scorecardCompatible: true,
  };
  yield qb.upsert({faros_MetricDefinition});

  const faros_MetricThresholdGroup = {
    uid: 'monthly-story-points-threshold-group',
    name: 'Monthly Story Points Threshold Group',
    definition: qb.ref({faros_MetricDefinition}),
  }
  yield qb.upsert({faros_MetricThresholdGroup});

  const faros_MetricThreshold_low = {
    uid: 'monthly-story-points-threshold-low',
    name: 'Monthly Story Points Threshold (Low)',
    lower: '0',
    upper: '20',
    rating: {
      category: 'low',
    },
    thresholdGroup: qb.ref({faros_MetricThresholdGroup}),
  }
  yield qb.upsert({faros_MetricThreshold:faros_MetricThreshold_low});

  const faros_MetricThreshold_medium = {
    uid: 'monthly-story-points-threshold-medium',
    name: 'Monthly Story Points Threshold (Medium)',
    lower: '20',
    upper: '40',
    rating: {
      category: 'medium',
    },
    thresholdGroup: qb.ref({faros_MetricThresholdGroup}),
  }
  yield qb.upsert({faros_MetricThreshold:faros_MetricThreshold_medium});

  const faros_MetricThreshold_high = {
    uid: 'monthly-story-points-threshold-high',
    name: 'Monthly Story Points Threshold (High)',
    lower: '40',
    rating: {
      category: 'high',
    },
    thresholdGroup: qb.ref({faros_MetricThresholdGroup}),
  }
  yield qb.upsert({faros_MetricThreshold:faros_MetricThreshold_high});

  const faros_Tag = {
    key: 'Threshold Group',
    uid: 'default-monthly-story-points-thresholds',
    value: 'Default',
  }
  yield qb.upsert({faros_Tag});

  const org_TeamTag = {
    tag: qb.ref({faros_Tag}),
    team: qb.ref({ org_Team: { uid: 'all_teams' } }),
  }
  yield qb.upsert({org_TeamTag});

  for await (const row of csvReadRows('../resources/example.csv')) {
    const org_Team = {
      uid: row.team
    }

    const faros_MetricValue = {
      definition: qb.ref({faros_MetricDefinition}),
      value: row.value,
      computedAt: row.computedAt,
      uid: `${row.team}-${row.computedAt}`,
    };

    const org_TeamMetric = {
      team: qb.ref({org_Team}),
      value: qb.ref({faros_MetricValue}),
    }
    
    yield qb.upsert({org_Team});
    yield qb.upsert({faros_MetricValue});
    yield qb.upsert({org_TeamMetric});
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
