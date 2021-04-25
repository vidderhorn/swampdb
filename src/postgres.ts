import driver from "pg-promise";

export type Postgres = driver.IConnected<{}, any>;

export module Postgres {
  export const tableDoesNotExist = "42P01";
}

export default Postgres;
