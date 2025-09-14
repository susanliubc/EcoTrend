-- create_partitioned table
CREATE OR REPLACE TABLE ecopulse_bq_dw.ecopulse_merged_partitioned
PARTITION BY
  TIMESTAMP_TRUNC(date, DAY) AS
SELECT * FROM ecopulse_bq_dw.ecopulse_merged;