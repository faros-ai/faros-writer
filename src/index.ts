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

const employeeIds = {
  "data": {
    "employees": [
      {
        "id": "aecd7f6feb81219dbf2fa165b79afc75f9b5b51e"
      },
      {
        "id": "ec17fd36759548f550cf2a39f43b53c38c9c021f"
      },
      {
        "id": "d0e2d92b85a910f007a3f53c6aa14643046972fb"
      },
      {
        "id": "8c2319152efff710d60e47d8cc14393da3a9defa"
      },
      {
        "id": "02bcb35eeb170618a3a4102e1be6a0b0f2b0db8a"
      },
      {
        "id": "f3a9fec1d3b1421895bef7f2a364e65ed8d1b223"
      },
      {
        "id": "31697e0e2c7a6f8c3541f0bf87ada94ec38731ed"
      },
      {
        "id": "ade6db4964b8e859d53c661c284dfff7e3ffe85e"
      },
      {
        "id": "04d24da24f0987869e9c96d24d191a2937e5285c"
      },
      {
        "id": "e3e2e0911837b70adf735050f5e62942362e2e2b"
      },
      {
        "id": "6a14127bd7a49641175decec444556b97a22dd8f"
      },
      {
        "id": "a0fefa286f126be91b9837aa6d9525323f29ba01"
      },
      {
        "id": "86bcd511b4e515702eca416de22c4fd06489f456"
      },
      {
        "id": "8b36fc9e1e64c3be1fc1c7bea385022fc79d8324"
      },
      {
        "id": "6f5083c466dcb99dcb4a1d154d321da862659010"
      },
      {
        "id": "76a371adaf32f11430c779eeb485b5d9067b1723"
      },
      {
        "id": "9fe99d3be217590616b373f01f18198ab4782d65"
      },
      {
        "id": "3118e9860d9893f01b2dee2e294c39859551facc"
      },
      {
        "id": "769bf2e2e1449b09e6d6628057f7aa386bf3e043"
      },
      {
        "id": "f0c2ea4da72d5ae12678ba02cc63295b9c62884b"
      },
      {
        "id": "03127c13df7a0319934aec46b4876be718d0e036"
      },
      {
        "id": "3493ea8bdaf4ddb478f84ab360b023904f1975d9"
      },
      {
        "id": "1c980b07eec26426d18008a30508b9ee6fa45971"
      },
      {
        "id": "94b272902769143e0d04f12ae97542fd028bc1b6"
      },
      {
        "id": "54d17a68cf834cc258a078675860e34f37ff0a76"
      },
      {
        "id": "6bf249825b1e630ad6e434afb5804fa1e348a022"
      },
      {
        "id": "dd869ef14ad302a52afa0caba1cb8ea51e5c1194"
      },
      {
        "id": "a5e8eb4e143e856a5f0f755e08ffba21b3c9b77f"
      },
      {
        "id": "a0bf620453ee4703d3e771efb25d3c66ab2f8f7b"
      },
      {
        "id": "e122604b849a736da724ce389f5bc0048e9478b0"
      },
      {
        "id": "982dbf4ee34fc23181c25afbf93d27f2463ad07d"
      },
      {
        "id": "be8df726d89b80c1d3e38b1faf40d9cb115b4728"
      },
      {
        "id": "e1647e26efa6be9d8f0d61a1db5ec0160b20a0a5"
      },
      {
        "id": "47f30c6b451ad89e52458955806bc5f7721e7244"
      },
      {
        "id": "9eb1d7458ecafe1d6898e45166596c3063574bd8"
      },
      {
        "id": "5d1cc315f4b421bba275e2e8cb8da2184d6f1202"
      },
      {
        "id": "8de504e9948c760f59c074c939de75f1b7558ba1"
      },
      {
        "id": "f814c9ee72fd53449720886369529d8d3cfd0751"
      },
      {
        "id": "49bae7a9a757756eb059cc331ceb563215243c04"
      },
      {
        "id": "8924c38ce400240517f71604f194445b9b4ca147"
      },
      {
        "id": "4907a3f1b1aaa7efcb87583bbc8938ae3b2527ef"
      },
      {
        "id": "3e8758125313b74479073089d9704de6f97bd366"
      },
      {
        "id": "60b82ae0b4569539dfcce64b62dedd27465287cf"
      },
      {
        "id": "144274f0b1e8772bfa015c427673e2bb8bf3bc89"
      },
      {
        "id": "3c84ecbbae1aac732855674403ee0c7e61d071f9"
      },
      {
        "id": "dd1ec546cb25d564e7847bae6697cd598cdcf513"
      },
      {
        "id": "9173239c2991241ffea1dbb1ef5aa397a5cc71b0"
      },
      {
        "id": "2c5d7fcd05a3e7a080e99002efc94f004a067939"
      },
      {
        "id": "73f9d60caea199129544863bc46ecec53b4f6478"
      },
      {
        "id": "3c1c0c1d19609902f063bd8e52466771b0339d97"
      },
      {
        "id": "42688bd28b763bc40389a9574df1d896d7c18d7f"
      },
      {
        "id": "48b2ce4dc7c30210e08df06cebf92405e95fc4bc"
      },
      {
        "id": "81f068c25d632d4b4d591e9579f671c765688b8d"
      },
      {
        "id": "f6af92444aa87e6ab8a3889cd533821004f21a70"
      },
      {
        "id": "8b6dbd342f9a3668538be05885e91e06ef6feaed"
      },
      {
        "id": "850b7a1d4ba9eee0a79ef990235b1af30b5fa84c"
      },
      {
        "id": "16229e7c022232413ec20f43347efbfb26824f76"
      },
      {
        "id": "715c6dc3cb7f527b65d5be3c227be9d8dc7e2cd2"
      },
      {
        "id": "3dd2533e3e59b333895730b28eb1b06a2f5f63e8"
      },
      {
        "id": "e03d6cf6378e244b99f690b85a7ea34731f143c9"
      },
      {
        "id": "0e965ed1f0ee2da43ce468f6e04fbd95f1cf1ec7"
      },
      {
        "id": "1ec3b6f25e83490798977c0a1c13ba03fe51b901"
      },
      {
        "id": "e952ca1a2ba23bce9d701dc081680aa0066dd191"
      },
      {
        "id": "80812da813c10abbabc1f0fbe3a132df3e5a739a"
      },
      {
        "id": "fc5edc261167e70ed52b02c888ad7b3a3e031b23"
      },
      {
        "id": "a816f6c47aebafe4223662d3c9f13272ae32ca6e"
      },
      {
        "id": "2409e05bff6902f4f61bd8605a72857e633b6c65"
      },
      {
        "id": "58f48084a7e7cff56d1fc7860268cc4b58bed55f"
      },
      {
        "id": "a35f38054f6ac9d736f55fc4d7b66669967bb35d"
      },
      {
        "id": "91ad355931e65dc5302fff097f5a160c53ffcb87"
      },
      {
        "id": "432fc6c0b3c0d3df3b7a19ba6d26cf7dd88d06f3"
      },
      {
        "id": "f4d0939e8887cf37ba3b3547b78286e7fa34b2ea"
      },
      {
        "id": "52f526f98566c59f055b92caad848c2abaa5e239"
      },
      {
        "id": "c5673c4ca5ec5c128c98036b73acc212708e4e1f"
      },
      {
        "id": "5d1c9fb9f043342106b9424d56fcb6388e9a048b"
      },
      {
        "id": "4d3b09b71cca42453c7a0abf0badfa863707fa92"
      },
      {
        "id": "9c55a2db6e4cf60518c5f412891ba0c8a33f8ab6"
      },
      {
        "id": "ef65af2e8796fc44cbe33154a7e309bea908a907"
      },
      {
        "id": "8ba3f26df43e7b19749772b5536ec6c440fe79b5"
      },
      {
        "id": "5a20b22317d175176af0090968b4345f1ae2ad6e"
      },
      {
        "id": "760ad02f8f8e50494264cc1f6840eec90218cb59"
      },
      {
        "id": "70f968faa29ad55c7b7f99af404be6a687919db4"
      },
      {
        "id": "f60e3c0a92f248c737fabbe5bbd8eabdebd493ed"
      },
      {
        "id": "c614613aa2a4c374c4efc601490211a0a6cb0581"
      },
      {
        "id": "aad327f41bd8f71007c79b8b54d702c71191be97"
      },
      {
        "id": "8062e663fa1ac21146a3e8234259d11f7a1d217f"
      },
      {
        "id": "b6a3c719b73df7dfc7149100af7194613af965b6"
      },
      {
        "id": "9ac3f58c807b876dafbfc39b2dad64c6d8e7fb6a"
      },
      {
        "id": "9b2b949b7436589674d0951fe4d638c16844c851"
      },
      {
        "id": "ff8e16f9b5ceb93b0b6ec7f07f03793fdc2aa6fd"
      },
      {
        "id": "51d42d96392449ea4161f5e7df7895f610cc06c6"
      },
      {
        "id": "28f401d72ffcafb198d514205404eb5697c65ee9"
      },
      {
        "id": "995e92394254e1fd15588e67b1c44f90e0336fd8"
      },
      {
        "id": "f8fc0328b00e0e678db35be8746c6b5f2d5a4fc0"
      },
      {
        "id": "96ef35486ee32438aa237e315bac2f7091470508"
      },
      {
        "id": "cb66753cc7ae7a2012951c4401f4d921ab848eab"
      },
      {
        "id": "88644b189227faa9cb1c111620161312b9680601"
      },
      {
        "id": "17f4c02b63201398292ab2b413ca531e52ae4109"
      },
      {
        "id": "0afe30f43b98aa3af22f2d88623bce5c8e4ccec7"
      },
      {
        "id": "8ca669fd674e85bd7fe350bded708b4929f3ff9a"
      },
      {
        "id": "eccbc373cb8ceca56f6f3b100c9df05b62592e1c"
      },
      {
        "id": "7e8c9498e9b2eb2633c294d583ae81a5aa370e2e"
      },
      {
        "id": "4cdba285f691ec0b1f5cc224c696bc1f93053c09"
      },
      {
        "id": "d37582624ba2726dfc4ae9f07ed77ede10355de9"
      },
      {
        "id": "02ec3ff6009f9896cb4a189fe20dd63d5b32cbd4"
      },
      {
        "id": "944a9eca99794d8dca74cb01c31ff0ba2f492518"
      },
      {
        "id": "047e750c3effbfd7acd6fa3f25fc8f7dbec05999"
      },
      {
        "id": "9ae5cdf524832207da9e02ae62e8381fb0be13d6"
      },
      {
        "id": "b9f66a00507e5e57601ad8aeb980b99aad777617"
      },
      {
        "id": "508c88070ecd6f0ab3ff45d71ada1a3e9118aa05"
      },
      {
        "id": "27ae5c1957d4d7c2077da7774467793e91da2fd1"
      },
      {
        "id": "0beb0ce86dc4d7d19a05be1b43ee72105e86af67"
      }
    ]
  }
}

async function* mutations(faros: FarosClient): AsyncGenerator<Mutation> {
  // The QueryBuilder manages the origin for you
  const qb = new QueryBuilder(origin);

  // EXAMPLE 2: Iterate across all rows in a CSV file, yielding mutations...
  for await (const emp of employeeIds.data.employees) {

    const faros_OrgEmployeeStatus = {
      status: "Ignored",
      employeeId: emp.id,
    };

    yield qb.upsert({faros_OrgEmployeeStatus});
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
