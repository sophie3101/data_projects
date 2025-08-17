{{ config(materialized='incremental') }}

WITH trip_cte AS
(
    SELECT *, 'Green' as service_type
    FROM {{ref('stg_green_trips')}}
    UNION ALL 
    SELECT *, 'Yellow' as service_type
    FROM {{ref('stg_yellow_trips')}}
),
zone_cte AS
(
    SELECT
    locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone 
    FROM {{ref("taxi_zone_lookup")}}
    WHERE borough != 'N/A'
)
SELECT 
    trip_cte.tripid, 
    trip_cte.vendorid, 
    trip_cte.service_type,
    trip_cte.ratecodeid, 
    trip_cte.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trip_cte.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trip_cte.pickup_datetime, 
    trip_cte.dropoff_datetime, 
    trip_cte.store_and_fwd_flag, 
    trip_cte.passenger_count, 
    trip_cte.trip_distance, 
    trip_cte.trip_type, 
    trip_cte.fare_amount, 
    trip_cte.extra, 
    trip_cte.mta_tax, 
    trip_cte.tip_amount, 
    trip_cte.tolls_amount, 
    trip_cte.ehail_fee, 
    trip_cte.improvement_surcharge, 
    trip_cte.total_amount, 
    trip_cte.payment_type, 
    trip_cte.payment_type_description
FROM trip_cte
INNER JOIN zone_cte as pickup_zone
ON trip_cte.pickup_locationid = pickup_zone.locationid
INNER JOIN zone_cte as dropoff_zone
ON trip_cte.dropoff_locationid = dropoff_zone.locationid

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where pickup_datetime >= (select max(pickup_datetime) from {{ this }} )

{% endif %}