import {
  batchMutation,
  FarosClient,
  Mutation,
  QueryBuilder,
} from 'faros-js-client';

import {csvReadRows, farosReadNodes} from './utils';

const faros_api_url = process.env.FAROS_API_URL || 'https://prod.api.faros.ai';
const faros_api_key = process.env.FAROS_API_KEY || '<key>';
const graph = process.env.FAROS_GRAPH || 'test';
const origin = process.env.FAROS_ORIGIN || 'faros-writer';
const maxBatchSize = Number(process.env.FAROS_BATCH_SIZE) || 500;
const debug = (process.env.FAROS_DEBUG || 'true') === 'true';

const projectToTeam = {
  'AI POD': 'team-uid',
}

async function* mutations(faros: FarosClient): AsyncGenerator<Mutation> {
  // The QueryBuilder manages the origin for you
  const qb = new QueryBuilder(origin);

  for await (const row of csvReadRows('../resources/ai-tokens.csv')) {
    const identity_Identity = {
      uid: row.Username,
    }
    yield qb.upsert({identity_Identity});


    const vcs_User = {
      uid: row.Username,
    }
    yield qb.upsert({vcs_User});

    const vcs_UserIdentity = {
      identity: qb.ref({identity_Identity}),
      vcsUser: qb.ref({vcs_User}),
    }
    yield qb.upsert({vcs_UserIdentity});

    const vcs_Team = {
      uid: projectToTeam[row['Project Name']] || row['Project Name'],
    }
    yield qb.upsert({vcs_Team});

    const vcs_AssistantMetricRequestDWPromptTokens = {
      uid: `${row.Date}-${row.Username}-RequestDWPromptTokens`,
      value: row.RequestDWPromptTokens,
      startedAt: row.Date,
      endedAt: row.Date,
      user: qb.ref({vcs_User}),
      team: qb.ref({vcs_Team}),
    };
    yield qb.upsert({vcs_AssistantMetric: vcs_AssistantMetricRequestDWPromptTokens});

    const vcs_AssistantMetricRequestDWCompletionTokens = {
      uid: `${row.Date}-${row.Username}-RequestDWCompletionTokens`,
      value: row.RequestDWCompletionTokens,
      startedAt: row.Date,
      endedAt: row.Date,
      user: qb.ref({vcs_User}),
      team: qb.ref({vcs_Team}),
    };
    yield qb.upsert({vcs_AssistantMetric: vcs_AssistantMetricRequestDWCompletionTokens});
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
