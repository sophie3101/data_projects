WITH weather_cte AS (
SELECT 
    "year", 
    EXTRACT(month FROM "timestamp") as "month",
    EXTRACT(day FROM "timestamp") as "day",
    EXTRACT(hour FROM "timestamp") as "hour",
    temperature,
    weather
FROM {{ ref('stg_weather') }}
),bike_cte AS (
    SELECT *
    FROM {{ref('stg_citibike')}}
)

SELECT  bike.year,
        bike.month,
        bike.started_day,
        bike.started_hour,
        bike.ended_day,
        bike.ended_hour,
        CASE 
            WHEN dom=1 THEN 'monday'
            WHEN dom=2 THEN 'tuesday'
            WHEN dom=3 THEN 'wednesday'
            WHEN dom=4 THEN 'thursday'
            WHEN dom=5 THEN 'friday'
            WHEN dom=6 THEN 'saturday'
            WHEN dom=7 THEN 'sunday'
        END AS weekday,
        start_station_name, 
        start_station_id,
        start_lat,
        start_lng,
        end_station_name, 
        end_station_id,
        end_lat,
        end_lng,
        trip_duration,
        rideable_type, 
        member_casual,
        weather.temperature,
        weather.weather
FROM bike_cte AS bike 
JOIN weather_cte AS weather 
    ON bike.started_day=weather.day AND bike.started_hour = weather.hour AND bike.year = weather.year AND bike.month = weather.month 
ORDER BY year,month, started_day, started_hour