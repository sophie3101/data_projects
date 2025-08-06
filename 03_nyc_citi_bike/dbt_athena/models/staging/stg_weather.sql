SELECT 
    CAST("timestamp" AS timestamp) as "timestamp",
    CAST("year" AS integer) AS year,
    temperature,
    weather
FROM {{source('raw_data', 'raw_weather')}}
-- WHERE year='2024'