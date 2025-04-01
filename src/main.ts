import {batchMutation} from 'faros-js-client';
import {code} from './code';

async function main(): Promise<void> {
  const data = require('../resources/card.json');
  const mutations = await code({data: data.rows});
  console.log(batchMutation(mutations));
}

main();
