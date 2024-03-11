# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary><b>Solution</b></summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

<details>

<summary><b>Step 1</b></summary>

```sql
CREATE MATERIALIZED VIEW trip_time AS
SELECT
	AVG(trip_data.tpep_dropoff_datetime-trip_data.tpep_pickup_datetime) avg_trip_time
	,MIN(trip_data.tpep_dropoff_datetime-trip_data.tpep_pickup_datetime) min_trip_time
	,MAX(trip_data.tpep_dropoff_datetime-trip_data.tpep_pickup_datetime) max_trip_time
	,pu_zone.zone pickup_zone
	,do_zone.zone dropoff_zone

FROM
	trip_data
LEFT JOIN
	taxi_zone pu_zone
ON 
	trip_data.pulocationid = pu_zone.location_id
LEFT JOIN
	taxi_zone do_zone
ON 
	trip_data.dolocationid = do_zone.location_id
GROUP BY
	4, 5;
```
</details>

Note that we consider the do not consider `a->b` and `b->a` as the same trip pair.
So as an example, you would consider the following trip pairs as different pairs:
```plaintext
Yorkville East -> Steinway
Steinway -> Yorkville East
```

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

<details>
<summary><b>Solution</b></summary>

```sql
WITH t1 AS
(
	SELECT
	MAX(avg_trip_time) max_avg
	FROM trip_time
)

SELECT
	avg_trip_time
	,CONCAT(pickup_zone,' -> ',dropoff_zone) zone_pair
FROM
	t1, trip_time
WHERE
	avg_trip_time = max_avg;
```


```plaintext
 avg_trip_time |         zone_pair          
---------------+----------------------------
 23:59:33      | Yorkville East -> Steinway
(1 row)
```
</details>



Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.



Options:
- [x] Yorkville East, Steinway
- [ ] Murray Hill, Midwood
- [ ] East Flatbush/Farragut, East Harlem North
- [ ] Midtown Center, University Heights/Morris Heights

p.s. The trip time between taxi zones does not take symmetricity into account, i.e. `A -> B` and `B -> A` are considered different trips. This applies to subsequent questions as well.

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

<details>
<summary><b>Solution</b></summary>

```sql
DROP MATERIALIZED VIEW trip_time;
CREATE MATERIALIZED VIEW trip_time AS
SELECT
	AVG(trip_data.tpep_dropoff_datetime-trip_data.tpep_pickup_datetime) avg_trip_time
	,MIN(trip_data.tpep_dropoff_datetime-trip_data.tpep_pickup_datetime) min_trip_time
	,MAX(trip_data.tpep_dropoff_datetime-trip_data.tpep_pickup_datetime) max_trip_time
	,COUNT(*) number_of_trips
	,pu_zone.zone pickup_zone
	,do_zone.zone dropoff_zone

FROM
	trip_data
LEFT JOIN
	taxi_zone pu_zone
ON 
	trip_data.pulocationid = pu_zone.location_id
LEFT JOIN
	taxi_zone do_zone
ON 
	trip_data.dolocationid = do_zone.location_id
GROUP BY
	pickup_zone, dropoff_zone;
```


```sql
CREATE MATERIALIZED VIEW highest_avg_time_count AS
WITH t1 AS
(
	SELECT
	MAX(avg_trip_time) max_avg
	FROM trip_time
)

SELECT
	avg_trip_time
	,number_of_trips
	,CONCAT(pickup_zone,' -> ',dropoff_zone) zone_pair
FROM
	t1, trip_time
WHERE
	trip_time.avg_trip_time = t1.max_avg;
```

```plaintext
 avg_trip_time | number_of_trips |         zone_pair          
---------------+-----------------+----------------------------
 23:59:33      |               1 | Yorkville East -> Steinway
(1 row)
```

</details>


Options:
- [ ] 5
- [ ] 3
- [ ] 10
- [x] 1

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

<details>
<summary><b>Solution</b></summary>

```sql
CREATE MATERIALIZED VIEW busiest_zones_17_hours AS
WITH t1 AS
	(
		SELECT 
			max(tpep_pickup_datetime) max_pickup_time 
		FROM 
			trip_data
	)
SELECT
	pu_zone.zone
	,count(*) number_of_pickups
FROM t1,
	trip_data
LEFT JOIN
	taxi_zone pu_zone
ON 
	trip_data.pulocationid = pu_zone.location_id
WHERE 
	trip_data.tpep_pickup_datetime >= t1.max_pickup_time - INTERVAL '20 hours'
GROUP BY
	1
ORDER BY
	2 DESC, 1 DESC
LIMIT
	3;
```

```plaintext
        zone         | count 
---------------------+-------
 LaGuardia Airport   |    19
 Lincoln Square East |    17
 JFK Airport         |    17
(3 rows)
```

</details>



Options:
- [ ] Clinton East, Upper East Side North, Penn Station
- [X] LaGuardia Airport, Lincoln Square East, JFK Airport
- [ ] Midtown Center, Upper East Side South, Upper East Side North
- [ ] LaGuardia Airport, Midtown Center, Upper East Side North