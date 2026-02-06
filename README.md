# data-engineering-course
Repository to upload the code for the data engineering course

# HOMEWORK 1

# Question 3
import pandas as pd
from sqlalchemy import create_engine

df = pd.read_parquet('green_tripdata_2025-11.parquet')

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

df.to_sql(name='green_tripdata_2025', con=engine, if_exists='replace', index=False)

select count(*) from green_tripdata_2025 where trip_distance <= 1;

# Question 4

select lpep_pickup_datetime from green_tripdata_2025 where trip_distance <= 100 order by trip_distance desc;

# Question 5


select zpu."Zone", count(*) as "Number_of_trips" from green_tripdata_2025 t  
JOIN
zones zpu ON t."PULocationID" = zpu."LocationID" 
JOIN
zones zdo ON t."DOLocationID" = zdo."LocationID"
where t.trip_distance <= 100 and DATE_PART('day', lpep_pickup_datetime) = 18 group by zpu."Zone" order by 2 desc;

# Question 6

select zdo."Zone", max(tip_amount) from green_tripdata_2025 t  
JOIN
zones zpu ON t."PULocationID" = zpu."LocationID" 
JOIN
zones zdo ON t."DOLocationID" = zdo."LocationID"
where zpu."Zone" = 'East Harlem North' 
group by zdo."Zone"
order by 2 desc;


# HOMEWORK 2

The flows for the assigment task is in pipeline/kestra_flows

# Question 1

We can achieve this easily from kestra. We execute the flow 08_gcp_taxi with the following input parameters Taxi type Yellow, Year 2020 and Month 12. Then We go to big query in GC and check the generated file output size of yellow_tripdata_2020-12.csv.

# Question 2

file = {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv

The inputs.taxi variable takes the value of green

The inputs.year variable takes the value of 2020

The inputs.month variable takes the value of 04

Once the variable is rendered the file variable is green_tripdata_2020-04.csv

# Question 3

We have to execute the flow 09_gcp_taxi_scheduled by using the backfill execution on the yellow taxi trigger and introduce the range of dates from 2020-01 to 2020-31 so the flow will be executed 12 times one for each month. Then we go to GC and check in Big Query section the number of rows of the table yellow_tripdata.

# Question 4

We do the same than in the above exercise but on the green taxi.

# Question 5

We execute the flow 08_gcp_taxi with Green, 2021, and 03 as parameters. The we check the number of rows for the table yellow_tripdata_2021_03 in GC.

# Question 6

We can add a property timezone: America/New_York within the trigger section.

# HOMEWORK 3

# Question 1

CREATE OR REPLACE EXTERNAL TABLE `nytaxi.yellow_tripdata_2024_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['https://storage.googleapis.com/kestra-course-bucket/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `nytaxi.yellow_tripdata_2024_materialized`
AS (
  SELECT * FROM `nytaxi.yellow_tripdata_2024_external`
);

select count(*) from  `nytaxi.yellow_tripdata_2024_materialized`

# Question 2

SELECT DISTINCT COUNT(PULocationID)  FROM `kestra-course-485912.nytaxi.yellow_tripdata_2024_external`

SELECT DISTINCT COUNT(PULocationID)  FROM `kestra-course-485912.nytaxi.yellow_tripdata_2024_materialized`

# Question 3

SELECT PULocationID  FROM `kestra-course-485912.nytaxi.yellow_tripdata_2024_materialized`

SELECT PULocationID,DOLocationID  FROM `kestra-course-485912.nytaxi.yellow_tripdata_2024_materialized`

# Question 4

SELECT COUNT(*)  FROM `kestra-course-485912.nytaxi.yellow_tripdata_2024_materialized` WHERE fare_amount = 0

# Question 5

CREATE OR REPLACE TABLE `nytaxi.yellow_taxi_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS (
  SELECT * FROM `kestra-course-485912.nytaxi.yellow_tripdata_2024_materialized`
);

# Question 6

SELECT DISTINCT VendorID  FROM `kestra-course-485912.nytaxi.yellow_tripdata_2024_materialized` WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'

SELECT DISTINCT VendorID  FROM `kestra-course-485912.nytaxi.yellow_taxi_optimized` WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'








