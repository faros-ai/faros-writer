import {
  batchMutation,
  FarosClient,
  Mutation,
  QueryBuilder,
} from 'faros-js-client';

import {csvReadRows, farosReadNodes} from './utils';

const faros_api_url = process.env.FAROS_API_URL || 'https://prod.api.faros.ai';
const faros_api_key_shared = process.env.FAROS_API_KEY_SHARED || '<key>';
const faros_api_key_main = process.env.FAROS_API_KEY_MAIN || '<key>';
const graph = process.env.FAROS_GRAPH || 'default';
const origin = process.env.FAROS_ORIGIN || 'faros-writer';
const maxBatchSize = Number(process.env.FAROS_BATCH_SIZE) || 500;
const debug = (process.env.FAROS_DEBUG || 'true') === 'true';

function getTeamId(farosMainNodesList, name) {
  for (const node of farosMainNodesList) {
    if (node.name === name) {
      return node.id;
    }
  }
  return null;
}

async function* mutations(farosShared: FarosClient, farosMain: FarosClient): AsyncGenerator<Mutation> {
  const qb = new QueryBuilder(origin);

  const farosSharedNodes = farosReadNodes(farosShared, graph, `
  {
    org_BoardOwnership {
      board {
        id
        name
        uid
      }
      team {
        id
        name
        uid
      }
    }
  }
  `);

  const farosSharedNodesList = [];
  for await (const node of farosSharedNodes) {
    farosSharedNodesList.push(node);
  }

  const farosMainNodes = farosReadNodes(farosMain, graph, `
    {
      org_Team {
        uid
        name
        id
        lead {
          identity {
            fullName
          }
        }
      }
    }
  `);

  const farosMainNodesList = [];
  for await (const node of farosMainNodes) {
    farosMainNodesList.push(node);
  }

  
  for (const node of farosSharedNodesList) {
    const teamId = getTeamId(farosMainNodesList, node.team.name);
    if (teamId != null && node.board != null) {
      const org_BoardOwnership = {
        boardId: node.board.id,
        teamId: teamId
      };

      yield qb.upsert({ org_BoardOwnership });
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

  const farosShared = new FarosClient({
    url: faros_api_url,
    apiKey: faros_api_key_shared,
  });
  const farosMain = new FarosClient({
    url: faros_api_url,
    apiKey: faros_api_key_main,
  });

  let batchNum = 1;
  let batch: Mutation[] = [];
  for await (const mutation of mutations(farosShared, farosMain)) {
    if (batch.length >= maxBatchSize) {
      console.log(`------ Batch ${batchNum} - Size: ${batch.length} ------`);
      await sendToFaros(farosMain, graph, batch);
      batchNum++;
      batch = [];
    }
    batch.push(mutation);
  }

  if (batch.length > 0) {
    console.log(`------ Batch ${batchNum} - Size: ${batch.length} ------`);
    await sendToFaros(farosMain, graph, batch);
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.log(`Failed to send to Faros ${err}`);
  });
}
