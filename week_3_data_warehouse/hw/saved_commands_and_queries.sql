CREATE OR REPLACE EXTERNAL TABLE `agile-polymer-376104.fhv_taxi_data.external_fhv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://de-taxi-bucket/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE `agile-polymer-376104.fhv_taxi_data.internal_fhv` AS
SELECT * FROM `agile-polymer-376104.fhv_taxi_data.external_fhv`;

SELECT COUNT(*) FROM agile-polymer-376104.fhv_taxi_data.internal_fhv;

-- SELECT COUNT(*) FROM agile-polymer-376104.fhv_taxi_data.external_fhv; # don't use this, it will process the whole data

SELECT COUNT(*)
  FROM agile-polymer-376104.fhv_taxi_data.internal_fhv
  WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

SELECT DISTINCT(affiliated_base_number)
  FROM agile-polymer-376104.fhv_taxi_data.internal_fhv
  WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

CREATE OR REPLACE TABLE agile-polymer-376104.fhv_taxi_data.fhv_partitioned_clustered
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY 
  affiliated_base_number AS
SELECT * FROM agile-polymer-376104.fhv_taxi_data.internal_fhv;

SELECT DISTINCT(affiliated_base_number)
  FROM agile-polymer-376104.fhv_taxi_data.fhv_partitioned_clustered
  WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
