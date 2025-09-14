-- create_partitioned_clustered table
CREATE OR REPLACE TABLE ecopulse_bq_dw._clustered
PARTITION 
  TIMESTAMP_TRUNC(date, DAY) 
CLUSTER BY SP500_daily_change_category
AS
SELECT * FROM ecopulse_bq_dw.ecopulse_merged;