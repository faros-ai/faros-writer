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
const recordLimit = Number(process.env.FAROS_RECORD_LIMIT) || 0;

async function* mutations(faros: FarosClient): AsyncGenerator<Mutation> {
  // The QueryBuilder manages the origin for you
  const qb = new QueryBuilder(origin);

  // Define system teams
  const allTeamsRef = {
    org_Team: {
      uid: 'all_teams',
    },
  };
  
  const unassignedTeamRef = {
    org_Team: {
      uid: 'unassigned',
    },
  };

  // Process pull request contributor data
  console.log('Processing pull request contributor data...');
  let contributorCount = 0;
  for await (const row of csvReadRows('../resources/pullrequest_contributor_summary_sample.csv')) {
    if (recordLimit > 0 && contributorCount >= recordLimit) break;
    contributorCount++;
    // Create team from organization name or use unassigned
    if (row.OrganizationName) {
      const org_Team = {
        uid: row.OrganizationName,
        name: row.OrganizationName,
        parentTeam: qb.ref(allTeamsRef),
      };
      yield qb.upsert({org_Team});
    }
    
    // Determine which team to use for the employee
    const teamRef = row.OrganizationName 
      ? { org_Team: { uid: row.OrganizationName } }
      : unassignedTeamRef;

    // Create address
    const addressUid = `${row.LocationAreaCode || 'unknown'}_${row.BuildingStateProvince || 'unknown'}_${row.BuildingCountry || 'unknown'}`;
    const geo_Address = {
      uid: addressUid,
      city: row.LocationAreaCode || 'Unknown',
      state: row.BuildingStateProvince || 'Unknown',
      country: row.BuildingCountry || 'Unknown',
    };
    yield qb.upsert({geo_Address});

    // Create location
    const geo_Location = {
      uid: addressUid,
      name: `${row.LocationAreaCode || 'Unknown'}, ${row.BuildingStateProvince || 'Unknown'}, ${row.BuildingCountry || 'Unknown'}`,
      address: qb.ref({geo_Address}),
    };
    yield qb.upsert({geo_Location});

    // Create employee based on combination of attributes
    const employeeUid = `${row.OrganizationName || 'unknown'}_${row.CareerStageName || 'unknown'}_${row.LocationAreaCode || 'unknown'}_${row.BuildingCountry || 'unknown'}_${row.BuildingStateProvince || 'unknown'}_${row.JobTitleName || 'unknown'}`;
    
    // Create identity to link user and employee
    const identity_Identity = {
      uid: employeeUid,
      fullName: row.JobTitleName || 'Unknown',
      primaryEmail: `${employeeUid}@example.com`,
    };
    yield qb.upsert({identity_Identity});

    // Extract numeric level from career stage (e.g., "IC4" -> 4)
    let level = null;
    if (row.CareerStageName) {
      const match = row.CareerStageName.match(/\d+/);
      if (match) {
        level = parseInt(match[0]);
      }
    }
    
    const org_Employee = {
      uid: employeeUid,
      title: row.JobTitleName || 'Unknown',
      level: level,
      location: qb.ref({geo_Location}),
      identity: qb.ref({identity_Identity}),
    };
    yield qb.upsert({org_Employee});

    // Create team membership
    const org_TeamMembership = {
      member: qb.ref({org_Employee}),
      team: qb.ref(teamRef),
    };
    yield qb.upsert({org_TeamMembership});

    // Create user
    const vcsUserUid = `${row.Source}_${row.ContributorUserType}_${row.PullRequestId}_${row.IsAuthor}`;
    const vcs_User = {
      uid: vcsUserUid,
      source: row.Source,
      name: row.JobTitleName || 'Unknown',
      type: {
        category: row.ContributorUserType === 'person' ? 'User' : 'Bot',
        detail: row.ContributorUserType,
      },
    };
    yield qb.upsert({vcs_User});
    
    // Link user to identity
    const vcs_UserIdentity = {
      vcsUser: qb.ref({vcs_User}),
      identity: qb.ref({identity_Identity}),
    };
    yield qb.upsert({vcs_UserIdentity});

    // Create repository
    const vcs_Repository = {
      name: row.RepositoryName,
      fullName: row.RepositoryName,
    };
    yield qb.upsert({vcs_Repository});

    // Update pull request with author if this is the author
    const prNumber = Number(row.PullRequestId);
    // Skip if PR number is too large (exceeds int32 max: 2147483647)
    if (prNumber > 2147483647) {
      console.warn(`Skipping PR reference ${row.PullRequestId} - number exceeds integer bounds`);
      continue;
    }
    
    if (row.IsAuthor === '1') {
      const vcs_PullRequest = {
        number: prNumber,
        repository: qb.ref({vcs_Repository}),
        authorId: vcsUserUid,
      };
      yield qb.upsert({vcs_PullRequest});
    }

    // Create pull request review if this is a reviewer
    if (row.IsAuthor !== '1' && row.FinalVote) {
      const vcs_PullRequestReview = {
        pullRequest: qb.ref({
          vcs_PullRequest: {
            number: prNumber,
            repository: qb.ref({vcs_Repository}),
          },
        }),
        reviewerId: vcsUserUid,
        state: row.FinalVote === 'Approve' ? 'approved' : 'commented',
        submittedAt: row.LastVoteDate,
      };
      yield qb.upsert({vcs_PullRequestReview});
    }
  }

  // Process pull request summary data
  console.log('Processing pull request summary data...');
  let summaryCount = 0;
  for await (const row of csvReadRows('../resources/pullrequest_summary_sample.csv')) {
    if (recordLimit > 0 && summaryCount >= recordLimit) break;
    summaryCount++;
    // Create repository
    const vcs_Repository = {
      name: row.RepositoryName,
      fullName: row.RepositoryName,
      htmlUrl: row.Url,
    };
    yield qb.upsert({vcs_Repository});

    // Create pull request
    const prNumber = Number(row.PullRequestId);
    // Skip if PR number is too large (exceeds int32 max: 2147483647)
    if (prNumber > 2147483647) {
      console.warn(`Skipping PR ${row.PullRequestId} - number exceeds integer bounds`);
      continue;
    }
    const vcs_PullRequest = {
      number: prNumber,
      htmlUrl: row.Url,
      title: `PR #${row.PullRequestId}`,
      state: row.Status === 'completed' ? 'closed' : 'open',
      createdAt: row.CreationDate,
      updatedAt: row.PublishedDate,
      mergedAt: row.MergeStatus === 'succeeded' && row.ClosedDate && row.ClosedDate.trim() !== '' ? row.ClosedDate : null,
      commentCount: Number(row.TotalCommentCount) || 0,
      commitCount: 0,
      diffStats: {
        filesChanged: Number(row.FileChangeCount) || 0,
      },
      repository: qb.ref({vcs_Repository}),
    };
    yield qb.upsert({vcs_PullRequest});
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
    try {
      console.log(`Sending ${batch.length} mutations...`);
      await faros.sendMutations(graph, batch);
      console.log(`Done.`);
    } catch (error) {
      console.error('Failed to send batch:', error);
      if (error.response && error.response.data) {
        console.error('Server response:', JSON.stringify(error.response.data, null, 2));
      }
      throw error;
    }
  }
}

async function main(): Promise<void> {
  console.log(`Debug: ${debug ? 'Enabled' : 'Disabled'}`)
  console.log(`URL: ${faros_api_url}, Graph: ${graph}, Origin: ${origin}`);
  if (recordLimit > 0) {
    console.log(`Record limit: ${recordLimit} records per CSV file`);
  }

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
    console.error(`Failed to send to Faros:`, err);
    if (err.jse_info && err.jse_info.res && err.jse_info.res.data) {
      console.error('Server error details:', JSON.stringify(err.jse_info.res.data, null, 2));
    }
    if (err.response) {
      console.error('Error response:', err.response.data || err.response);
    }
  });
}
