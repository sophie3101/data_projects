
-- What are the most popular pick-up locations across the city for NY Citi Bike rental?
select start_station_name, COUNT(start_station_name) as pickup_count
FROM citibike_facts
GROUP BY start_station_name
ORDER BY pickup_count DESC
LIMIT 5
/* #	start_station_name	pickup_count
1	W 21 St & 6 Ave	119183
2	8 Ave & W 31 St	99340
3	University Pl & E 14 St	95323
4	Broadway & W 58 St	94954
5	Lafayette St & E 8 St	94792 */



-- What are the most popular drop-off locations across the city for NY Citi Bike rental?
select end_station_name, COUNT(end_station_name) as dropoff_count
FROM citibike_facts
GROUP BY end_station_name
ORDER BY  dropoff_count DESC
LIMIT 5


--bike rental avg duration among member types
SELECT "month",
    COUNT(*)FILTER(WHERE member_casual='casual') AS non_member_count,
    AVG(trip_duration) FILTER(WHERE member_casual='casual') as avg_non_member_tripduration,
    COUNT(*)FILTER(WHERE member_casual='member') AS member_count,
    AVG(trip_duration) FILTER(WHERE member_casual='member')as avg_member_tripduration
FROM citibike_facts
GROUP BY "month"
ORDER BY "month"


-- bike rental among type of ride
SELECT "month", 
COUNT(*)FILTER(WHERE rideable_type='electric') AS electric_ride_count,
AVG(trip_duration) FILTER(WHERE rideable_type='electric') as avg_electric_tripduration,
COUNT(*)FILTER(WHERE rideable_type='classic') AS classic_ride_count,
AVG(trip_duration) FILTER(WHERE rideable_type='classic') as avg_classic_tripduration
FROM citibike_facts
GROUP BY "month"
ORDER BY "month"


--use distribution by hour
select started_hour, COUNT(*) as ride_count 
from citibike_facts
GROUP BY started_hour
ORDER BY ride_count DESC

WITH cte AS (
select 
    month, started_hour, weather,
    COUNT(*) as ride_count,
    AVG(temperature) as avg_temp,
    RANK()OVER(PARTITION BY month ORDER BY COUNT(*) DESC ) as rnk
from citibike_facts
GROUP BY month, started_hour, weather

-- ORDER BY month, ride_count DESC
)
select month, started_hour as peak_hour, weather, ride_count, avg_temp
FROM cte 
where rnk <=5
ORDER BY month
