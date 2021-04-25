import driver from "pg-promise";
import { sql } from "typedtext";
import Postgres from "./postgres";

export interface Document<T> {
  id: number;
  created: Date;
  updated: Date;
  body: T;
}

export interface Options {
  host: string;
  user: string;
  password: string;
  database: string;
  onLost?: (err: any) => any;
}

export type Prop<T> = keyof T;

export class Database {
  private pg: Postgres;

  static async connect({ host, user, password, database, onLost }: Options): Promise<Database> {
    const conn = driver()({ host, user, password, database });
    const pg = await conn.connect({ onLost });
    return new Database(pg);
  }

  private constructor(pg: Postgres) {
    this.pg = pg;
  }

  async get<Body>(collection: string, id: number, props?: Prop<Body>[]): Promise<Document<Body>> {
    const result = await this.tryGet<Body>(collection, id, props);
    if (result === null) {
      throw new MissingRecordError(collection, { id });
    }
    return result;
  }

  async tryGet<Body>(collection: string, id: number, props?: Prop<Body>[]): Promise<Document<Body> | null> {
    const columns = propsToColumns(props);
    const results = await this.queryCollection<Document<Body>>(collection, { id }, sql`
      select id, created, updated, ${columns}
      from ${collection}
      where id = $[id]
      limit 1
    `);
    return results.length ? results[0] : null;
  }

  async all<Body>(collection: string): Promise<Document<Body>[]> {
    return await this.queryCollection(collection, {}, sql`
      select *
      from ${collection}
      order by id asc
    `);
  }

  async find<Body>(collection: string, criteria: Partial<Body>): Promise<Document<Body>[]> {
    return await this.queryCollection(collection, { criteria }, sql`
      select *
      from ${collection}
      where body @> $[criteria]
      order by id asc
    `);
  }

  async findOne<Body>(collection: string, criteria: Partial<Body>): Promise<Document<Body>> {
    const result = await this.tryFindOne(collection, criteria);
    if (result === null) {
      throw new MissingRecordError(collection, criteria);
    }
    return result;
  }

  async tryFindOne<Body>(collection: string, criteria: Partial<Body>): Promise<Document<Body> | null> {
    const results = await this.queryCollection<Document<Body>>(collection, { criteria }, sql`
      select *
      from ${collection}
      where body @> $[criteria]
      limit 1
    `);
    return results.length ? results[0] : null;
  }

  async insert<Body>(collection: string, body: Body): Promise<Document<Body>> {
    const results = await this.changeCollection<Document<Body>>(collection, { body }, sql`
      insert into ${collection} (body)
      values ($[body])
      returning *;
    `);
    return results[0];
  }

  async update<Body>(collection: string, id: number, body: Body): Promise<Document<Body>> {
    const results = await this.raw<Document<Body>>({ id, body }, sql`
      update ${collection}
      set body = ($[body]),
          updated = now()
      where id = $[id]
      returning *;
    `);
    return results[0];
  }

  async raw<Result>(params: any, query: string) {
    return this.pg.query<Result[]>(query, params);
  }

  /** Creates collection on demand. */
  private async changeCollection<Result>(collection: string, params: any, query: string): Promise<Result[]> {
    try {
      return await this.pg.query(query, params);
    }
    catch (e) {
      if (e.code !== Postgres.tableDoesNotExist) {
        throw e;
      }
      await this.createCollection(collection);
      return await this.pg.query(query, params);
    }
  }

  /** Recovers if collection does not yet exist. */
  private async queryCollection<Result>(collection: string, params: any, query: string): Promise<Result[]> {
    try {
      return await this.pg.query(query, params);
    }
    catch (e) {
      if (e.code !== Postgres.tableDoesNotExist) {
        throw e;
      }
      return [];
    }
  }

  private async createCollection(collection: string) {
    await this.pg.query(sql`
      create table ${collection} (
        id serial primary key,
        body jsonb not null,
        created timestamptz not null default now(),
        updated timestamptz not null default now()
      );
      create index ${collection}_body_idx
      on ${collection}
      using gin (body jsonb_path_ops);
    `);
  }
}

function propsToColumns<Body>(props: undefined | Prop<Body>[]) {
  return !props ? "*" : `jsonb_build_object(${props.map(prop => `
    '${prop}', body->'${prop}'
  `).join(", ")}) "body"`;
}

export class MissingRecordError extends Error {
  collection: string;
  criteria: any;

  constructor(collection: string, criteria?: any) {
    super(`Could not find record in collection "${collection}".`);
    this.collection = collection;
    this.criteria = criteria;
  }
}

export default Database;
