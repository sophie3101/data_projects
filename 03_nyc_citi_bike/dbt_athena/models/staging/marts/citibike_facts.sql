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

SELECT  bike.*, 
        weather.temperature,
        weather.weather
FROM bike_cte AS bike 
JOIN weather_cte AS weather 
    ON bike.started_day=weather.day AND bike.started_hour = weather.hour AND bike.year = weather.year AND bike.month = weather.month 
ORDER BY year,month, started_day, started_hour