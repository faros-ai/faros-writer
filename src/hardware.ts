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

const idMap = [
  {
    id: '0fe007d139efcf882dd24227a3aed86255468bc2',
    identityId: 'e9d8d80beae967d12ac6d886d356fdca1df681f2',
  },
  {
    id: '11eba0d7a8adddcf1279d883cadf5d0eaaf40e52',
    identityId: '5a8d9a2cc618d37d58179421273b5bc2a6617fdd',
  },
  {
    id: '13d105f8403a833225b35ccafba1703922443aca',
    identityId: '5efd136d506182254862808adf6e4e8bc916d2d8',
  },
  {
    id: '1735218e52c344b13f0406dae9856745f894b76c',
    identityId: '886bce535d4a52efabcc85dbbb1af2225101be07',
  },
  {
    id: '1a09f0d6e7b0693cea07cb608c626df3acbc1316',
    identityId: '7f58cd586bf63a10401829457b6878bc8db65507',
  },
  {
    id: '26aa43040c801055ff729056f96cfd645e4f0932',
    identityId: '919b51e1246776b0a5351fb1d69cdb758a752a32',
  },
  {
    id: '2c317350b7657023fcec3613bb6a87561bfae598',
    identityId: 'f286d3c1922f826cbe0c5ed6aadbd74c758f2964',
  },
  {
    id: '2cad645c1e05dca01ed5e3766c6b71242bb38b84',
    identityId: '1c760b8e77840800b50b01afb02725d65e4c96b0',
  },
  {
    id: '313e66396cea49c25196c4c858a3e61eb25d2618',
    identityId: '9752ad3c93c53f488e775a3375e24d1d43aada94',
  },
  {
    id: '3285946b970567d6eca24d32f6b50173049d3309',
    identityId: 'cb83e7b6cd29fa1f3bb79edc74eb2de77914a85a',
  },
  {
    id: '34f0b4277d99eabf64049b3f2efda3b7b4bd19fe',
    identityId: '993295c2f94abc963a0460bdbf6a20a5d365ea6b',
  },
  {
    id: '37554678c399eaa2c238793c38782c2ff0d430ec',
    identityId: '3834ccd107ab3610eaaa2ea50004b18defef9427',
  },
  {
    id: '38ef737c06f72e760a398f975c7bacef2b0e823a',
    identityId: 'e521c3985862c409b037a79ba31473815062f836',
  },
  {
    id: '3d75b2dae4eb9417850c673d4eca03ad09f0d952',
    identityId: 'cb61f860375879116148db6253c00259d92cad20',
  },
  {
    id: '3ed05d7af4bb66285ca547cf81f473f5c21876b6',
    identityId: '5e7c033abd4807103350be566c81c2d3683dd06a',
  },
  {
    id: '40faab4b19805ba3f29dd702c014a362a6c18541',
    identityId: '44ad9c942a837b09759ee1c4eed538dc0de240a3',
  },
  {
    id: '4384a94008850f658b5796f592a5d16db6de72b0',
    identityId: '1ab1e3870aa87307d6091ad2f25d64a6018f0252',
  },
  {
    id: '45909fd444935a5eeb22f5dfb4cd4083396fc6e9',
    identityId: 'dffb60de98d9283f76f4268122cc3b2d4c7467bd',
  },
  {
    id: '46b96d6c842341c2294e2ab6f544df60806b091d',
    identityId: '50be5782dfaeeb28b870b6334ab81e698d87a59b',
  },
  {
    id: '49ace47507c9edf4486af60bdb8deb3c8e178378',
    identityId: '6e8a35efddd410b3fbd18fada755d5db4a44226d',
  },
  {
    id: '553ecd3db5a497033e19292a72f1833a34246716',
    identityId: '8d636998af48193cf78d87631854cf07f19aa2a3',
  },
  {
    id: '57a95cc6d7992063908610134c6d96d3e39b338c',
    identityId: 'e1967ed49711a6dc5f3e5aaa9be87e469fca708b',
  },
  {
    id: '58d145fb4f5db8d0ba0d48e2d2a04b56d1239d87',
    identityId: '4ac132293b6937f081881b3c2e1e00623966a236',
  },
  {
    id: '6057baef76919e5bf0f40a489798ecba4f7c9dbd',
    identityId: 'f2184b0f2507de30c6a88538353dceae3a18b5a9',
  },
  {
    id: '61d446732a471f379cafa5f5acab48f5b0243f82',
    identityId: 'a25a3fe38ef97a27d3bd33741b2a376b6d4a72df',
  },
  {
    id: '643a741899e6418318e97de46e8c9eb7752be2fa',
    identityId: '3bd7d87c4de970cbc20936ea5991e3278ef069c0',
  },
  {
    id: '6e323dc19b06fac9b4e8d531b4e2aec013d05227',
    identityId: '5966bc31987e30c639dd6560ee70943364386b68',
  },
  {
    id: '6f04aaa30212125a3796c4485fb4f8fb1c812288',
    identityId: '99dc107041bd02a28e555b089a11985860fc06a0',
  },
  {
    id: '7f78f8c3eb2e54946cb9cd36eb71e0b40002f09e',
    identityId: 'f399efd12dfe1045f37a605a297ab9a99bffcc71',
  },
  {
    id: '8e0a3f7b2c7556a27cc934f57b2ba76955a3ed94',
    identityId: '61c453f3627c66e5c49a5ee2560ec1afe949c758',
  },
  {
    id: '9581c7f40278bcbecd7a459babbc0a67aa6618b4',
    identityId: '4bb0fb4f9c0513ecdc8ac2027f0f84917908bbf9',
  },
  {
    id: '983cddc1f41bfa9ac77d8c5f13388ce088422199',
    identityId: null,
  },
  {
    id: '9eb3218fe8613a3de4cd8fc5849109d1d5182171',
    identityId: '92c7edf4e66dcfd179cd00183ff888fc0b772bea',
  },
  {
    id: '9f492623816be5b2cee092dc7e545d2213a14d76',
    identityId: 'c50323da07dd4ae492d3200627aab2cd9307a70b',
  },
  {
    id: 'a4ab41bc65023a13e915bc702718d24b6ad566ce',
    identityId: 'a0dffe6f1a6a6c3937b6b8992e946234b12e7a29',
  },
  {
    id: 'a527c252d4567315630d9fc7e723929e97a2382e',
    identityId: '7da80cbcc747e6b06f9b4c355a8b667c0172cf0f',
  },
  {
    id: 'a5376549502e9ba55565cca42a980a07878c5828',
    identityId: 'd504485e44e844d28704a5dbfae37bb66e078a9b',
  },
  {
    id: 'a540e8d88ce0b02f11100e9f515c8f5c63654802',
    identityId: '4ebc5e6716f7d998f459ef5109197afde00453de',
  },
  {
    id: 'b1ac3455aa95fbe9585ca51e09eed5dcaba364ad',
    identityId: '608f4b1ba13cd20c9cc5588c7ecd903a5c930fe1',
  },
  {
    id: 'b6345c52b3498ce90216c74aa81fbaf170e67490',
    identityId: 'bfbf5042a14b5ccce70d959a87def59d8f8cfbe0',
  },
  {
    id: 'cc06c90014dc3a62aeec0ad8932becebdaa6d363',
    identityId: '922eef3e8f08913ce17380ef50514c86ee600ba2',
  },
  {
    id: 'cc1cb9c8d8b5998086c9894b6b9a4a30c185c98e',
    identityId: 'b4c8d10082ede5929e00ba1e3bb466fe061cc08f',
  },
  {
    id: 'ce2c12d5dd45afd11e1201c3a01800cd86f93409',
    identityId: 'c47175104bbc846e52705d6f45c5e7f2db0737e2',
  },
  {
    id: 'd76777c005685cc59e6c32c304a0310a36637504',
    identityId: null,
  },
  {
    id: 'd9dfde4ddeb1f7da8a276adf49c9d78503f488c0',
    identityId: 'e8fe5c829e7b59a9cc1b1ede10b112f556ab97b8',
  },
  {
    id: 'daf4cbeb360a314b5164b5a60481da095399dbca',
    identityId: 'b38fa411cdc6e05c1493d926aa6fc28d13e54f3e',
  },
  {
    id: 'dfbac621d551292ac381da452c929fb4c946fd52',
    identityId: '6c017fc45fc9ae23112d13137ce7faba7e00f4ba',
  },
  {
    id: 'e2790b0bd71754718eb585dbd993da2113ae1e8a',
    identityId: '4baf90abe838038d59477ebeb511331a025c1c1e',
  },
  {
    id: 'e53d3eec2865f89ce3b35151055edf4170aeaa4b',
    identityId: 'dd4516e5fd701748174c5051038cc9e1003696b8',
  },
  {
    id: 'ecbc87c7ae8d199c569e00cc8fd6f62579d9d895',
    identityId: '75cf2512d4a7bc41b78415bf62016c2c88565a87',
  },
  {
    id: 'f209a2937c261eab534b518e088b322ea60056c0',
    identityId: 'fdfc42363067a8f8310a85a263bf7328802368cd',
  },
  {
    id: 'fc435fb13ed13b657aaf41a5b471b52b3d58f3e0',
    identityId: '6f39dd73032d23d0b1fe283701956a2bb12182ea',
  },
  {
    id: 'fd2807abe08f216d19da5ce5862ec107316d5e9d',
    identityId: 'fde35cf115e4199b0ab406fada1e0a710d5b083d',
  },
  {
    id: 'fdac78f268290c882daf5295cc5bcff172f93b9b',
    identityId: '11c321e3fb6521451a1ce0e945389ce2ecd8384b',
  },
];

async function* mutations(faros: FarosClient): AsyncGenerator<Mutation> {
  // The QueryBuilder manages the origin for you
  const qb = new QueryBuilder(origin);

  for await (const row of csvReadRows('../resources/hardware.csv')) {
    const org_EmployeeTool = {
      employeeId: idMap.find((x) => x.identityId === row.ID)?.id,
      tool: {
        detail: row.Machine,
        category: 'Machine',
      },
      activatedAt: row.Start,
      deactivatedAt: row.End,
    };
    yield qb.upsert({org_EmployeeTool});
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
