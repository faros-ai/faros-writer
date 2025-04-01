import {QueryBuilder} from 'faros-js-client';

export const code = async (inputs) => {
  const origin = 'control-tower';
  const qb = new QueryBuilder(origin);
  const mutations = [];
  const faros_MetricDefinition = {
    uid: 'monthly-avg-pr-merge-rate',
    name: 'Monthly Average PR Merge Rate',
    description:
      'Average number of pull requests merged per team member per month',
    valueType: {
      category: 'Numeric',
    },
    scorecardCompatible: true,
  };
  mutations.push(qb.upsert({faros_MetricDefinition}));

  const faros_MetricThresholdGroup = {
    uid: 'monthly-avg-pr-merge-rate-threshold-group',
    name: 'Monthly Average PR Merge Rate Threshold Group',
    definition: qb.ref({faros_MetricDefinition}),
  };
  mutations.push(qb.upsert({faros_MetricThresholdGroup}));

  const faros_MetricThreshold_low = {
    uid: 'monthly-avg-pr-merge-rate-threshold-low',
    name: 'Monthly Average PR Merge Rate Threshold (Low)',
    lower: '0',
    upper: '4',
    rating: {
      category: 'low',
    },
    thresholdGroup: qb.ref({faros_MetricThresholdGroup}),
  };
  mutations.push(qb.upsert({faros_MetricThreshold: faros_MetricThreshold_low}));

  const faros_MetricThreshold_medium = {
    uid: 'monthly-avg-pr-merge-rate-threshold-medium',
    name: 'Monthly Average PR Merge Rate Threshold (Medium)',
    lower: '4',
    upper: '6',
    rating: {
      category: 'medium',
    },
    thresholdGroup: qb.ref({faros_MetricThresholdGroup}),
  };
  mutations.push(
    qb.upsert({faros_MetricThreshold: faros_MetricThreshold_medium})
  );

  const faros_MetricThreshold_high = {
    uid: 'monthly-avg-pr-merge-rate-threshold-high',
    name: 'Monthly Average PR Merge Rate Threshold (High)',
    lower: '6',
    upper: '8',
    rating: {
      category: 'high',
    },
    thresholdGroup: qb.ref({faros_MetricThresholdGroup}),
  };
  mutations.push(
    qb.upsert({faros_MetricThreshold: faros_MetricThreshold_high})
  );

  const faros_MetricThreshold_elite = {
    uid: 'monthly-avg-pr-merge-rate-threshold-elite',
    name: 'Monthly Average PR Merge Rate Threshold (Elite)',
    lower: '8',
    rating: {
      category: 'elite',
    },
    thresholdGroup: qb.ref({faros_MetricThresholdGroup}),
  };
  mutations.push(
    qb.upsert({faros_MetricThreshold: faros_MetricThreshold_elite})
  );

  const faros_Tag = {
    key: 'Threshold Group',
    uid: 'default-monthly-avg-pr-merge-rate-thresholds',
    value: 'Default',
  };
  mutations.push(qb.upsert({faros_Tag}));

  const org_TeamTag = {
    tag: qb.ref({faros_Tag}),
    team: qb.ref({org_Team: {uid: 'all_teams'}}),
  };
  mutations.push(qb.upsert({org_TeamTag}));

  for await (const row of inputs.data) {
    const team = row[0];
    const computedAt = row[1]; 
    const value = `${row[2]}`;

    if (!team || !computedAt || !value) {
      continue;
    }

    const org_Team = {
      uid: team
    }

    const faros_MetricValue = {
      definition: qb.ref({faros_MetricDefinition}),
      value: value,
      computedAt: computedAt,
      uid: `${team}-${computedAt}`,
    };

    const org_TeamMetric = {
      team: qb.ref({org_Team}),
      value: qb.ref({faros_MetricValue}),
    }
    
    mutations.push(qb.upsert({org_Team}));
    mutations.push(qb.upsert({faros_MetricValue}));
    mutations.push(qb.upsert({org_TeamMetric}));
  }

  return mutations;
};
