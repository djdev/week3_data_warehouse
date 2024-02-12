-- Create an external table using the Green Taxi Trip Records Data for 2022 referring to gcs path
-- Create a table in BQ using the Green Taxi Trip Records for 2022
CREATE OR REPLACE EXTERNAL TABLE `influential-bit-412922.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://week3-data-ware-house/green/green_tripdata_2022-*.parquet']
);


-- Count of records for the 2022 Green Taxi Data
SELECT COUNT(1) FROM `influential-bit-412922.ny_taxi.external_green_tripdata`;


-- Query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT COUNT(DISTINCT(PULocationID))
FROM `influential-bit-412922.ny_taxi.external_green_tripdata`;


-- Records that have a fare_amount of 0
SELECT COUNT(1) FROM `influential-bit-412922.ny_taxi.external_green_tripdata` WHERE fare_amount=0.00;


-- Size of the tables
CREATE OR REPLACE TABLE `influential-bit-412922.ny_taxi.green_tripdata_non_partitoned` AS
SELECT * FROM `influential-bit-412922.ny_taxi.external_green_tripdata`;

CREATE OR REPLACE TABLE `influential-bit-412922.ny_taxi.green_tripdata_partitoned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `influential-bit-412922.ny_taxi.external_green_tripdata`
);

SELECT COUNT(DISTINCT PULocationID)
FROM `influential-bit-412922.ny_taxi.green_tripdata_non_partitoned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30';

SELECT COUNT(DISTINCT PULocationID)
FROM `influential-bit-412922.ny_taxi.green_tripdata_partitoned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30';