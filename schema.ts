export class Schema {
  definition: SchemaDefinition;
  constructor(definition: SchemaDefinition) {
    this.definition = definition;
  }
}

export type SchemaDefinition = { [key: string]: SchemaValueType };
export type SchemaValueType = number | string | Array<any> | Object;
