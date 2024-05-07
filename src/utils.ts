import {parse} from 'csv-parse';
import {FarosClient, paginatedQueryV2} from 'faros-js-client';
import {createReadStream} from 'fs';
import path from 'path';

export async function* csvReadRows(file: string): AsyncGenerator<any> {
  console.log(`Loading file: ${file}`);

  const parser = parse({delimiter: ',', columns: true});
  createReadStream(path.join(__dirname, file)).pipe(parser);

  for await (const row of parser) {
    yield row;
  }
}

export async function* farosReadNodes(
  faros: FarosClient,
  graph: string,
  query: string,
  args?: Map<string, any>
): AsyncGenerator<any> {
  for await (const node of faros.nodeIterable(
    graph,
    query,
    undefined,
    paginatedQueryV2,
    args
  )) {
    yield node;
  }
}
