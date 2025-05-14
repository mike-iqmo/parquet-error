import duckdb from "duckdb";

import { parquetReadObjects, asyncBufferFromFile } from "hyparquet";

var db = new duckdb.Database(":memory:");
console.log("created db");
db.all(
  `
    CREATE TABLE range AS FROM range(1250);
    CREATE TABLE range2 as 
    select range::VARCHAR as range_varchar from range;

    COPY range
    TO 'duckdb_delta_binary_packed.parquet'
    (PARQUET_VERSION v2);


    COPY range2
    TO 'duckdb_delta_length_byte_array.parquet'
    (PARQUET_VERSION v2);
    `,
  async function (err, res) {
    if (err) {
      console.warn(err);
    }
    try {
      const asyncBuffer = await asyncBufferFromFile("duckdb_delta_binary_packed.parquet");
      const objects = await parquetReadObjects({ file: asyncBuffer });
      console.log("Created",objects)
    } catch (err) {
      console.warn("Error loading parquet file", err);
    }

    try {
        const asyncBuffer = await asyncBufferFromFile("duckdb_delta_length_byte_array.parquet");
        const objects = await parquetReadObjects({ file: asyncBuffer });
        console.log("Created",objects)
      } catch (err) {
        console.warn("Error loading parquet file", err);
      }
  }
);
