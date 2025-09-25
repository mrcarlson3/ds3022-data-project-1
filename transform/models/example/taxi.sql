-- File: transform/models/taxi.sql
{{ config(materialized='table') }}

with yellow_taxi as (
    select
        'yellow' as taxi_type,
        *,
        
        -- CO2 per trip in kilograms (yellow taxis typically have higher emissions)
        (trip_distance * 0.192) as co2_per_trip_kg,
        
        -- Average MPH per trip
        case 
            when extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0
            then (trip_distance / (extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600))
            else null
        end as avg_mph_per_trip,
        
        -- Trip hour (from pickup time)
        extract(hour from tpep_pickup_datetime) as trip_hour,
        
        -- Trip day of week (1=Sunday, 7=Saturday)
        extract(dow from tpep_pickup_datetime) + 1 as trip_day_of_week,
        
        -- Week number (1-52)
        extract(week from tpep_pickup_datetime) as week_number,
        
        -- Month (1-12)
        extract(month from tpep_pickup_datetime) as month

    from "emissions"."yellow"

),

green_taxi as (
    select
        'green' as taxi_type,
        *,
        
        -- CO2 per trip in kilograms (green taxis typically have lower emissions)
        (trip_distance * 0.156) as co2_per_trip_kg,
        
        -- Average MPH per trip
        case 
            when extract(epoch from (lpep_dropoff_datetime - lpep_pickup_datetime)) > 0
            then (trip_distance / (extract(epoch from (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600))
            else null
        end as avg_mph_per_trip,
        
        -- Trip hour (from pickup time)
        extract(hour from lpep_pickup_datetime) as trip_hour,
        
        -- Trip day of week (1=Sunday, 7=Saturday)
        extract(dow from lpep_pickup_datetime) + 1 as trip_day_of_week,
        
        -- Week number (1-52)
        extract(week from lpep_pickup_datetime) as week_number,
        
        -- Month (1-12)
        extract(month from lpep_pickup_datetime) as month

    from "emissions"."green"

)

select * from yellow_taxi
union all
select * from green_taxi