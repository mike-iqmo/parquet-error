import duckdb from "duckdb";

import { parquetReadObjects, asyncBufferFromFile } from "hyparquet";

var db = new duckdb.Database(":memory:");
console.log("created db");
db.all(
  `
    CREATE TABLE range AS FROM range(10000);

    COPY range
    TO 'v2.parquet'
    (PARQUET_VERSION v2);
    `,
  async function (err, res) {
    if (err) {
      console.warn(err);
    }
    try {
      const asyncBuffer = await asyncBufferFromFile("v2.parquet");
      const objects = await parquetReadObjects({ file: asyncBuffer });
      
    } catch (err) {
      console.warn("Error loading parquet file", err);
    }
  }
);
