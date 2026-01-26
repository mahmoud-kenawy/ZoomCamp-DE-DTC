--Q3
SELECT count(*)
FROM green_taxi_data
WHERE lpep_pickup_datetime::date >= '2025-11-01'
  AND lpep_pickup_datetime::date < '2025-12-01'
  AND trip_distance::numeric <= 1;

--Q4
SELECT
    lpep_pickup_datetime::date AS pickup_day,
    MAX(trip_distance::numeric) AS longest_trip
FROM green_taxi_data
WHERE trip_distance::numeric < 100
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

--Q5
select t1.pulocationid,t2.zone, sum(total_amount::float) from green_taxi_data t1 join taxi_zone_lookup t2
on t1.pulocationid::int = t2.locationid::int
where Date(t1.lpep_pickup_datetime) = '2025-11-18'
group by 1,2
order by 3 desc
limit 1

--Q6
SELECT
    z_do."zone" AS dropoff_zone,
    MAX(gt.tip_amount::numeric) AS max_tip
FROM green_taxi_data gt
JOIN taxi_zone_lookup z_pu ON gt."pulocationid"::int = z_pu."locationid"::int
JOIN taxi_zone_lookup z_do ON gt."dolocationid"::int = z_do."locationid"::int
WHERE z_pu."zone" = 'East Harlem North'
  AND lpep_pickup_datetime::timestamp >= '2025-11-01'
  AND lpep_pickup_datetime::timestamp < '2025-12-01'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;
