import {
  batchMutation,
  FarosClient,
  Mutation,
  QueryBuilder,
} from 'faros-js-client';

import {csvReadRows, farosReadNodes} from './utils';
import { resolve } from 'path';

const faros_api_url = process.env.FAROS_API_URL || 'https://prod.api.faros.ai';
const faros_api_key = process.env.FAROS_API_KEY || '<key>';
const graph = process.env.FAROS_GRAPH || 'default';
const origin = process.env.FAROS_ORIGIN || 'helpdesk';
const maxBatchSize = Number(process.env.FAROS_BATCH_SIZE) || 500;
const debug = (process.env.FAROS_DEBUG || 'true') === 'true';

async function* mutations(faros: FarosClient): AsyncGenerator<Mutation> {
  // The QueryBuilder manages the origin for you
  const qb = new QueryBuilder(origin);

  // EXAMPLE 2: Iterate across all rows in a CSV file, yielding mutations...
  for await (const row of csvReadRows('../resources/helpdesk.csv')) {

    const ims_Incident = {
      uid: `${row.author}-${row.created_at}`,
      priority: row.priority,
      createdAt: row.created_at,
      title: row.title,
      description: row.content,
      source: 'helpdesk'
    };

    yield qb.upsert({ims_Incident});

    if (row.category !== '') {
      const ims_Label = {
        name: row.category
      };

      yield qb.upsert({ims_Label});

      const ims_IncidentTag = {
        incident: qb.ref({ims_Incident}),
        label: qb.ref({ims_Label})
      };

      yield qb.upsert({ims_IncidentTag});
    }

    if (row.subcategory !== '') {
      const ims_Label = {
        name: row.subcategory
      };

      yield qb.upsert({ims_Label});

      const ims_IncidentTag = {
        incident: qb.ref({ims_Incident}),
        label: qb.ref({ims_Label})
      };

      yield qb.upsert({ims_IncidentTag});
    }

    if (row.team !== '') {
      const ims_Team = {
        uid: row.team,
        name: row.team
      };

      yield qb.upsert({ims_Team});

      const ims_TeamIncidentAssociation = {
        team: qb.ref({ims_Team}),
        incident: qb.ref({ims_Incident})
      };
      
      yield qb.upsert({ims_TeamIncidentAssociation});
    }

    if (row.assignee !== '') {
      const ims_User = {
        uid: row.assignee    
      };

      yield qb.upsert({ims_User});

      const ims_IncidentAssignment = {
        incident: qb.ref({ims_Incident}),
        assignee: qb.ref({ims_User})
      };
      
      yield qb.upsert({ims_IncidentAssignment});
    }
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
