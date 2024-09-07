import {
  batchMutation,
  FarosClient,
  Mutation,
  QueryBuilder,
} from 'faros-js-client';

import {csvReadRows, farosReadNodes} from './utils';
import { hardware } from './hardware-data';

const faros_api_url = process.env.FAROS_API_URL || 'https://prod.api.faros.ai';
const faros_api_key = process.env.FAROS_API_KEY || '<key>';
const graph = process.env.FAROS_GRAPH || 'default';
const origin = process.env.FAROS_ORIGIN || 'faros-writer';
const maxBatchSize = Number(process.env.FAROS_BATCH_SIZE) || 500;
const debug = (process.env.FAROS_DEBUG || 'true') === 'true';


function selectAuthor(node) {
  const members =
    node.deployments[0].application.ownership.team.members;

  // Filter out members with titles containing 'Manager', 'Director', or 'VP'
  const eligibleMembers = members.filter((member) => {
    const title = member.member.identity.title || '';
    return !/Manager|Director|VP/i.test(title);
  });

  // Randomly select a member from the eligible members
  if (eligibleMembers.length === 0) {
    return null; // or handle the case where no eligible members are found
  }

  const randomIndex = Math.floor(Math.random() * eligibleMembers.length);
  return eligibleMembers[randomIndex].member.identity;
}

function getMachineType(id, date) {
    const machine = hardware.find((machine) => {
        return machine['ID'] === id && new Date(machine['Start']) <= date && date <= new Date(machine['End']);
    });
    return machine ? machine['Machine'] : null;
}

function getNewEndedAt(startedAt, endedAt, machineType) {
    const interval = endedAt - startedAt;

    if (machineType === 'M1') {
        return new Date(startedAt.getTime() + interval * 2);
    } else if (machineType === 'M2') {
        return new Date(startedAt.getTime() + interval * 1.5);
    } else if (machineType === 'M3') {
        return new Date(startedAt.getTime() + interval * 1);
    }

    return endedAt;
}

async function* mutations(faros: FarosClient): AsyncGenerator<Mutation> {
  // The QueryBuilder manages the origin for you
  const qb = new QueryBuilder(origin);
  const query = `
    {
        cicd_Build {
            startedAt
            endedAt
            uid
            status
            refreshedAt
            pipelineId
            id
            deployments {
            application {
                ownership {
                team {
                    members {
                    member {
                        identity {
                        fullName
                        id
                        uid
                        employee {
                            title
                        }
                        }
                    }
                    }
                }
                }
            }
            }
        }
    }
    `;
  const farosNodes = farosReadNodes(faros, graph, query);
  for await (const node of farosNodes) {
    if (node.deployments.length < 1) {
        continue;
    }

    const author = selectAuthor(node);
    const machineType = getMachineType(author.id, new Date(node.startedAt));
    const newEndedAt = getNewEndedAt(new Date(node.startedAt), new Date(node.endedAt), machineType);

    const cicd_Agent = {
        uid: author.id,
        name: `${author.uid}-${machineType}-machine`,
    }

    const cicd_Build = {
      startedAt: node.startedAt,
      endedAt: newEndedAt.toISOString(),
      uid: node.uid + '-local',
      status: node.status,
      pipelineId: node.pipelineId,
      agent: qb.ref({cicd_Agent}),
    };

    yield qb.upsert({cicd_Agent});
    yield qb.upsert({cicd_Build});

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
  console.log(`Debug: ${debug ? 'Enabled' : 'Disabled'}`);
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
