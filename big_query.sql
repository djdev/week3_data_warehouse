SELECT station_id, name FROM
  bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `influential-bit-412922.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://week3-data-ware-house/yellow/yellow_tripdata_2019-*.parquet', 'gs://week3-data-ware-house/yellow/yellow_tripdata_2020-*.parquet']
);


-- Check yellow trip data
SELECT * REPLACE (
  CAST(0 AS FLOAT64) AS airport_fee
) FROM `influential-bit-412922.ny_taxi.external_yellow_tripdata` limit 10;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `influential-bit-412922.ny_taxi.yellow_tripdata_non_partitoned` AS
SELECT * REPLACE (
  CAST(0 AS FLOAT64) AS airport_fee
) FROM `influential-bit-412922.ny_taxi.external_yellow_tripdata`;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `influential-bit-412922.ny_taxi.yellow_tripdata_partitoned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * REPLACE (
  CAST(0 AS FLOAT64) AS airport_fee
) FROM `influential-bit-412922.ny_taxi.external_yellow_tripdata`;


-- Impact of partition
-- Scanning 1.63 GB of data
SELECT DISTINCT(VendorID)
FROM `influential-bit-412922.ny_taxi.yellow_tripdata_non_partitoned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';


-- Scanning ~106.37 MB of DATA
SELECT DISTINCT(VendorID)
FROM `influential-bit-412922.ny_taxi.yellow_tripdata_partitoned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';


-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `influential-bit-412922.ny_taxi.yellow_tripdata_partitoned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * REPLACE (
  CAST(0 AS FLOAT64) AS airport_fee
) FROM `influential-bit-412922.ny_taxi.external_yellow_tripdata`;


-- Query scans 1.07 GB
SELECT count(*) as trips
FROM `influential-bit-412922.ny_taxi.yellow_tripdata_partitoned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31' AND VendorID=1;


-- Query scans 887.44 MB
SELECT count(*) as trips
FROM `influential-bit-412922.ny_taxi.yellow_tripdata_partitoned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31' AND VendorID=1;