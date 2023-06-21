import fs from "fs";
import zlib from "zlib";

//@ts-ignore
import Hyperswarm from "hyperswarm";
//@ts-ignore
import Corestore from "corestore";
//@ts-ignore
import Hyperbee from "hyperbee";
//@ts-ignore
import goodbye from "graceful-goodbye";
//@ts-ignore
import b4a from "b4a";
import { createHash } from "crypto";

export class SQLParser {
  query: string;
  constructor(query: string) {
    this.query = query;
  }

  parse() {
    const queryWords = this.query.split(" ");
    const queryType = queryWords[0].toLowerCase();
    // Check the query type and call the appropriate function
    switch (queryType) {
      case "create":
        this.create(queryWords);
        break;
      case "insert":
        this.addRow(queryWords);
        break;
      // case "select":
      //   getRows(query);
      //   break;
      // case "update":
      //   updateRow(query);
      //   break;
      default:
        console.log("Unsupported SQL query type.");
    }
  }

  private create(queryWords: string[]) {
    switch (queryWords[1]) {
      case "table":
        this.createTable(queryWords);
        break;
      case "database":
        this.createDatabase(queryWords);
        break;
      default:
        console.log("Unsupported SQL query type.");
    }
  }

  private createTable(queryWords: string[]) {}

  private createDatabase(queryWords: string[]) {}

  private addRow(queryWords: string[]) {}
}
export class Schema {
  definition: SchemaDefinition;
  lastIndex = 0;
  constructor(definition: SchemaDefinition) {
    this.definition = definition;
  }
}

export type SchemaDefinition = { [key: string]: SchemaValueType };
export type SchemaValueType = number | string | Array<any> | Object;

const topic = "test-topic";
const topicHex = createHash("sha256").update(topic).digest("hex");
const topicBuffer = b4a.from(topicHex, "hex");

const name = process.argv[2];
const store = new Corestore(`./${name}`);

const swarm = new Hyperswarm();
goodbye(() => swarm.destroy());
swarm.join(topicBuffer);

const conns: any[] = [];

swarm.on("connection", (conn: any) => {
  store.replicate(conn);
  const name = b4a.toString(conn.remotePublicKey, "hex");
  console.log("* got a connection from:", name, "*");
  conns.push(conn);
  for (const conn of conns) {
    conn.write("line");
  }
  conn;
});

const core = store.get({ name: "my-bee-core" });
const bee = new Hyperbee(core, {
  keyEncoding: "utf-8",
  valueEncoding: "utf-8",
});

bee.feed.ready().then(function () {
  console.log("Feed key: " + bee.feed.key.toString("hex"));
  // swarm.join(bee.feed.discoveryKey);
});

await core.ready();
const discovery = swarm.join(topicBuffer);

// Only display the key once the Hyperbee has been announced to the DHT
discovery.flushed().then(() => {
  console.log("bee key:", b4a.toString(core.key, "hex"));
  console.log("joined topic:", topic);
});

// // Only import the dictionary the first time this script is executed
// // The first block will always be the Hyperbee header block
// if (core.length <= 1) {
//   console.log("importing dictionary...");
//   const dict = await loadDictionary();
//   const batch = bee.batch();
//   for (const { key, value } of dict) {
//     console.log(key, value);
//     await batch.put(key, value);
//   }
//   await batch.flush();
// } else {
//   // Otherwise just seed the previously-imported dictionary
//   console.log("seeding dictionary...");
// }

// await createTable("teacher", new Schema({ name: "string", id: "number" }));
// await insertRowInTable("teacher", { name: "parag", id: 6 });
// await getData("student", {});
await getData("teacher", {});

async function updateRows(
  table: string,
  data: { [name: string]: CellDataType }
) {
  const { seq, key, value } = await bee.get(name, { wait: true, update: true });
  if (!value) {
    console.log("Table not found");
  }
  const parsedTableStucture = JSON.parse(value);
  const tableDef = parsedTableStucture.definition;
  const lastIndex = parsedTableStucture.lastIndex;
  const batch = bee.batch();
}

async function getData(name: string, query: { [column: string]: any }) {
  const { seq, key, value } = await bee.get(name, { wait: true, update: true });
  if (!value) {
    console.log("Table not found");
  }
  const parsedTableStucture = JSON.parse(value);
  const tableDef = parsedTableStucture.definition;
  const lastIndex = parsedTableStucture.lastIndex;
  const batch = bee.batch();
  const response = [];
  const sub = bee.sub(name);
  for (let index = 1; index <= lastIndex; index++) {
    response.push(await sub.get(`${index}`));
  }
  const resp = await batch.flush();
  console.log(response);
}
async function insertRowInTable(
  name: string,
  data: { [name: string]: CellDataType }
) {
  const { seq, key, value } = await bee.get(name, { wait: true, update: true });
  if (!value) {
    console.log("Table not found");
  }
  const parsedTableStucture = JSON.parse(value);
  const tableDef = parsedTableStucture.definition;
  const lastIndex = parsedTableStucture.lastIndex;
  const columnNames = Object.keys(tableDef);
  for (let column of columnNames) {
    const columnData = data[column];

    if (!columnData) {
      console.log("Please pass data for all columns");
      // throw Error("Please pass data for all columns");
    }
    if (typeof data[column] != tableDef[column]) {
      console.log(data[column], tableDef[column]);
      console.log(`Pass correct data type for column, ${column}`);
      // throw Error(`Pass correct data type for column, ${column}`);
    }
  }
  const sub = bee.sub(name);
  sub.put(`${lastIndex + 1}`, JSON.stringify(data));
  const schema = { ...parsedTableStucture, lastIndex: lastIndex + 1 };
  bee.put(name, JSON.stringify(schema));
}

async function createTable(name: string, schema: Schema) {
  console.log(`creating table, ${name}, ${JSON.stringify(schema)}`);
  bee.put(name, JSON.stringify(schema));
}

const stream = bee.createReadStream();
// for await (const entry of stream) {
//   console.log(entry);
// }

// async function loadDictionary() {
//   const compressed = await fs.promises.readFile("./dict.json.gz");
//   return new Promise((resolve, reject) => {
//     zlib.unzip(compressed, (err, dict) => {
//       if (err) return reject(err);
//       return resolve(JSON.parse(b4a.toString(dict)));
//     });
//   });
// }

export type CellDataType = string | number;
