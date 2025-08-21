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
 
  // First, query all teams with their parent relationships
  const teamsQuery = `
  {
    org_Team {
      uid
      name
      parentTeam {
        uid
      }
    }
  }
  `;
  
  // Then query existing parent team tags
  const tagsQuery = `
  {
    org_TeamTag {
      team {
        uid
      }
      tag {
        uid
        key
        value
      }
    }
  }
  `;
  
  // Build a map of team UIDs to their latest parent team tags
  const teamTagsMap = new Map<string, any>();
  const tagNodes = farosReadNodes(faros, graph, tagsQuery);
  for await (const teamTag of tagNodes) {
    if (teamTag.tag?.key === 'parent_team_uid') {
      const teamUid = teamTag.team.uid;
      const existingTag = teamTagsMap.get(teamUid);
      
      // Extract timestamp from tag UID (format: parent-team-{teamUid}-{timestamp})
      const getTimestampFromUid = (uid: string) => {
        const parts = uid.split('-');
        const timestamp = parts[parts.length - 1];
        return new Date(timestamp).getTime();
      };
      
      // Keep only the latest tag based on timestamp in UID
      if (!existingTag || getTimestampFromUid(teamTag.tag.uid) > getTimestampFromUid(existingTag.uid)) {
        teamTagsMap.set(teamUid, teamTag.tag);
      }
    }
  }
  
  // Now process teams and create tags only when needed
  const teamNodes = farosReadNodes(faros, graph, teamsQuery);
  for await (const team of teamNodes) {
    // Only process if the team has a parent
    if (team.parentTeam?.uid) {
      const currentParentUid = team.parentTeam.uid;
      const latestTag = teamTagsMap.get(team.uid);
      
      // Only create a new tag if parent has changed or no tag exists
      const shouldCreateNewTag = !latestTag || latestTag.value !== currentParentUid;
      
      // Only create a new tag if parent has changed or no tag exists
      if (shouldCreateNewTag) {
        const currentTimestamp = new Date().toISOString();
        
        // Create a Faros tag with parent team UID as value
        const faros_Tag = {
          // Include timestamp in UID to track parent changes over time
          uid: `parent-team-${team.uid}-${currentTimestamp}`,
          key: 'parent_team_uid',
          value: currentParentUid,
        };
        
        // Reference to the team this tag belongs to
        const org_Team = {
          uid: team.uid,
        };
        
        // Create the association between team and tag
        const org_TeamTag = {
          team: qb.ref({org_Team}),
          tag: qb.ref({faros_Tag}),
        };
        
        yield qb.upsert({faros_Tag});
        yield qb.upsert({org_TeamTag});
      }
    }
  }
}

async function sendToFaros(
  faros: FarosClient,
  graph: string,
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
