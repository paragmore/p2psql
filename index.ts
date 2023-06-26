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
import crypto, { createHash } from "crypto";
//@ts-ignore
import ram from "random-access-memory";
//@ts-ignore
import Autobase from "autobase";

export class SQLParser {
  query: string;
  queryWords: string[];
  constructor(query: string) {
    this.query = query.toLowerCase();
    this.queryWords = this.query.split(" ");
  }

  parseSQLQuery() {
    const result: {
      queryType?: string;
      tableName?: string;
      columns?: string[];
      values?: string[];
      whereClause?: string;
    } = {};

    // CREATE TABLE query
    if (this.query.toUpperCase().startsWith("CREATE TABLE")) {
      result.queryType = "CREATE_TABLE";
      const regex = /CREATE TABLE (\w+)\s*\((.*)\);/i;
      const match = this.query.match(regex);
      if (match) {
        result.tableName = match[1];
        result.columns = match[2].split(",").map((column) => column.trim());
      }
    }
    // INSERT query
    else if (this.query.toUpperCase().startsWith("INSERT")) {
      result.queryType = "INSERT";
      const regex = /INSERT INTO (\w+)\s*\((.*)\)\s*VALUES\s*\((.*)\);/i;
      const match = this.query.match(regex);
      if (match) {
        result.tableName = match[1];
        result.columns = match[2].split(",").map((column) => column.trim());
        result.values = match[3].split(",").map((value) => value.trim());
      }
    }
    // SELECT query
    else if (this.query.toUpperCase().startsWith("SELECT")) {
      result.queryType = "SELECT";
      const regex = /SELECT (.*) FROM (\w+)(?: WHERE (.*))?;/i;
      const match = this.query.match(regex);
      if (match) {
        result.columns = match[1].split(",").map((column) => column.trim());
        result.tableName = match[2];
        if (match[3]) {
          result.whereClause = match[3];
        }
      }
    }

    return JSON.stringify(result);
  }
}

const parseer = new SQLParser(
  "CREATE TABLE Persons (PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255));"
);

console.log("PARSED", parseer.parseSQLQuery());
export class Schema {
  definition: SchemaDefinition;
  lastIndex = 0;
  constructor(definition: SchemaDefinition) {
    this.definition = definition;
  }
}

class HyperSQL {
  store;
  swarm: any;
  autobase: any;
  bee: any;
  name: string = "PMS SWARM";
  constructor() {
    this.store = new Corestore("./storage");
    this.swarm = null;
    this.autobase = null;
    this.bee = null;
  }

  async start() {
    const writer = this.store.get({ name: "writer" });
    const viewOutput = this.store.get({ name: "view-output" });
    await writer.ready();
    this.autobase = new Autobase({
      inputs: [writer],
      localInput: writer,
      outputs: [viewOutput],
    });

    await this.autobase.ready();

    // console.log("Joining swarm");
    // const topic = Buffer.from(sha256(this.name), "hex");
    // this.swarm = new Hyperswarm();
    // this.swarm.on("connection", (socket: any) => this.store.replicate(socket));
    // this.swarm.join(topic);
    // await this.swarm.flush();
    // console.log("Joined swarm", this.name);

    this.autobase.start({
      unwrap: true,
      apply: this.applyAutobeeBatch,
      view: (core: any) => {
        return new Hyperbee(core.unwrap(), {
          // .unwrap() might become redundant if https://github.com/holepunchto/autobase/pull/33 gets merged
          extension: false,
          keyEncoding: "utf-8",
          valueEncoding: "json",
        });
      },
    });
    await this.autobase.view.update();

    this.bee = this.autobase.view;
  }

  async applyAutobeeBatch(bee: any, batch: any) {
    const b = bee.batch({ update: false });
    for (const node of batch) {
      const op = JSON.parse(node.value.toString());
      // TODO: Handle deletions
      if (op.type === "createTable") {
        const hash = sha256(op.data.schema);
        await b.put(op.data.name, { schema: op.data.schema, hash });
      }
      if (op.type === "insertIntoTable") {
        const data = op.data.data;
        const name = op.data.name;
        const hash = sha256(op.data.data);

        const { seq, key, value } = await b.get(name, {
          wait: true,
          update: true,
        });
        if (!value) {
          console.log("Table not found");
        }
        console.log("VALUE", value);
        const parsedTableStucture = JSON.parse(value.schema);
        console.log("parsedTableStucture", parsedTableStucture);
        const tableDef = parsedTableStucture.definition;
        const lastIndex = parsedTableStucture.lastIndex;
        const columnNames = Object.keys(tableDef);
        console.log(tableDef, lastIndex, columnNames, data);
        const parsedData = JSON.parse(data);
        for (let column of columnNames) {
          const columnData = parsedData[column];
          if (!columnData) {
            console.log("Please pass data for all columns");
            // throw Error("Please pass data for all columns");
          }
          if (typeof parsedData[column] != tableDef[column]) {
            console.log(parsedData[column], tableDef[column]);
            console.log(`Pass correct data type for column, ${column}`);
            // throw Error(`Pass correct data type for column, ${column}`);
          }
        }
        console.log(JSON.stringify(data));
        try {
          await b.put(`${name}/${lastIndex + 1}`, JSON.stringify(data));
        } catch (error) {
          console.log(error);
        }
        const schema = { ...parsedTableStucture, lastIndex: lastIndex + 1 };
        console.log("sub put");
        const tableHash = sha256(JSON.stringify(schema));
        await b.put(name, { schema: JSON.stringify(schema), hash: tableHash });
        console.log("inserted", name, schema);
      }
    }
    await b.flush();
  }

  async updateRows(table: string, data: { [name: string]: CellDataType }) {
    const { seq, key, value } = await this.bee.get(name, {
      wait: true,
      update: true,
    });
    if (!value) {
      console.log("Table not found");
    }
    const parsedTableStucture = JSON.parse(value);
    const tableDef = parsedTableStucture.definition;
    const lastIndex = parsedTableStucture.lastIndex;
    const batch = this.bee.batch();
  }

  async getData(name: string, query: { [column: string]: any }) {
    const { seq, key, value } = await this.bee.get(name, {
      wait: true,
      update: true,
    });
    console.log("DATA", seq, key, value);
    if (!value) {
      console.log("Table not found");
    }
    const parsedTableStucture = JSON.parse(value.schema);
    const tableDef = parsedTableStucture.definition;
    const lastIndex = parsedTableStucture.lastIndex;
    const batch = this.bee.batch();
    const response = [];
    console.log("LAST", lastIndex);
    for (let index = 1; index <= lastIndex; index++) {
      console.log("SUB GET", await this.bee.get(`${name}/${index}`));
      response.push(await this.bee.get(`${name}/${index}`));
    }
    const resp = await batch.flush();
    console.log(response);
  }
  async insertRowInTable(name: string, data: { [name: string]: CellDataType }) {
    console.log(`inserting into table, ${name}, ${JSON.stringify(data)}`);
    const hash = sha256(JSON.stringify(JSON.stringify(data)));
    console.log(this.bee.length);
    await this.autobase.append(
      JSON.stringify({
        type: "insertIntoTable",
        data: { name, data: JSON.stringify(data) },
        hash,
      })
    );
    // await this.autobase.view.get(0);
    await this.bee.update();
  }

  async createTable(name: string, schema: Schema) {
    console.log(`creating table, ${name}, ${JSON.stringify(schema)}`);
    const hash = sha256(JSON.stringify(schema));
    console.log(this.bee.length);
    await this.autobase.append(
      JSON.stringify({
        type: "createTable",
        data: { name, schema: JSON.stringify(schema) },
        hash,
      })
    );
    // await this.autobase.view.get(0);
    await this.bee.update();
  }

  async getall() {
    for await (const data of this.autobase.createReadStream()) {
      console.log("STREAM", data.value.toString());
    }
    for await (const data of this.bee.createReadStream()) {
      console.log("STREAM BE", data);
    }
    // console.log(this.bee.length);
    // for (let i = 0; i < this.bee.length; i++) {
    //   const node = await this.bee.get(i);
    //   console.log("Value", node.value.toString());
    // }
  }
}

const hyperSql = new HyperSQL();
console.log("STARTING HYPERSQL");
// await hyperSql.start();
console.log("STARTED HYPERSQL");

// await hyperSql.createTable(
//   "student",
//   new Schema({ name: "string", id: "number" })
// );
// await hyperSql.createTable(
//   "teacher",
//   new Schema({ name: "string", id: "number" })
// );

// await hyperSql.getall();

// await hyperSql.getData("student", {});
// await hyperSql.insertRowInTable("student", { name: "parag", id: 7 });

export type SchemaDefinition = { [key: string]: SchemaValueType };
export type SchemaValueType = number | string | Array<any> | Object;

// const topic = "test-topic";
// const topicHex = createHash("sha256").update(topic).digest("hex");
// const topicBuffer = b4a.from(topicHex, "hex");

// const name = process.argv[2];
// const store = new Corestore(`./${name}`);

// const swarm = new Hyperswarm();
// goodbye(() => swarm.destroy());
// swarm.join(topicBuffer);

// const conns: any[] = [];

// swarm.on("connection", (conn: any) => {
//   store.replicate(conn);
//   const name = b4a.toString(conn.remotePublicKey, "hex");
//   console.log("* got a connection from:", name, "*");
//   conns.push(conn);
//   for (const conn of conns) {
//     conn.write("line");
//   }
//   conn;
// });

// const core = store.get({ name: "my-bee-core" });
// const bee = new Hyperbee(core, {
//   keyEncoding: "utf-8",
//   valueEncoding: "utf-8",
// });

// bee.feed.ready().then(function () {
//   console.log("Feed key: " + bee.feed.key.toString("hex"));
//   // swarm.join(bee.feed.discoveryKey);
// });

// await core.ready();
// const discovery = swarm.join(topicBuffer);

// // Only display the key once the Hyperbee has been announced to the DHT
// discovery.flushed().then(() => {
//   console.log("bee key:", b4a.toString(core.key, "hex"));
//   console.log("joined topic:", topic);
// });

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
// await getData("teacher", {});

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
function sha256(inp: any) {
  return crypto.createHash("sha256").update(inp).digest("hex");
}
