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

select * from green_tripdata_2025 where trip_distance <= 100 order by trip_distance desc;

# Question 5

select zpu."Zone", count(*) from green_tripdata_2025 t  RIGHT JOIN
zones zpu ON t."PULocationID" = zpu."LocationID" where t.trip_distance <= 100 and DATE_PART('day', lpep_pickup_datetime) = 18 group by zpu."Zone" order by 2 desc;

# Question 6

select zpu."Zone", max(trip_distance) from green_tripdata_2025 t  
JOIN
zones zpu ON t."PULocationID" = zpu."LocationID" 
JOIN
zones zdo ON t."DOLocationID" = zdo."LocationID"
where zpu."Zone" = 'East Harlem North' 
group by zpu."Zone";




