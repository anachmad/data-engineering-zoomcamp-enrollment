/*
 * No 1 */
select count(*) 
from green_tripdata gt 
where 
    date(lpep_dropoff_datetime) = '2019-01-15' 
    and date(lpep_pickup_datetime) = '2019-01-15'
;

/*
 * No 4 */
select date(lpep_pickup_datetime) pickup, trip_distance 
from green_tripdata gt 
where trip_distance = 
	(select max(trip_distance) from green_tripdata)
;

/*
 * No 5 */
select count(*) from green_tripdata gt 
where 
	date(lpep_pickup_datetime) = '2019-01-01'
	and passenger_count = 2
union all 
select count(*) from green_tripdata gt 
where 
	date(lpep_pickup_datetime) = '2019-01-01'
	and passenger_count = 3
;

/*
 * No 6 */
with pickup_astoria as (
select 
	date(lpep_pickup_datetime) pickup,
	tz."Zone" pickup_zone,
	tz2."Zone" destination_zone,
	gt.tip_amount,
	rank() over(order by tip_amount desc) ranking
	from green_tripdata gt 
join zones tz on gt."PULocationID" = tz."LocationID" 
join zones tz2 on gt."DOLocationID"  = tz2."LocationID"  
where tz."Zone" = 'Astoria'
)
select * from pickup_astoria where ranking = 1
;