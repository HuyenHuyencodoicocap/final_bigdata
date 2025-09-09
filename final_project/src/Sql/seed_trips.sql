WITH s AS (
  SELECT
    gs AS i,
    (ARRAY[1,2,6,7])[1 + (random()*3)::int]::smallint                         AS vendorid,
    now() - (gs * interval '10 minutes')                                       AS pu,
    now() - (gs * interval '10 minutes')
      + ((5 + (random()*50))::int || ' min')::interval                         AS do_ts,  -- < sửa tên
    (1 + (random()*3))::int                                                    AS passenger_count,
    round((random()*10)::numeric, 3)                                           AS trip_distance,
    (ARRAY[1,2,3,4,5,6,99])[1 + (random()*6)::int]::smallint                   AS ratecodeid,
    (CASE WHEN random() < 0.05 THEN 'Y' ELSE 'N' END)::char(1)                 AS store_and_fwd_flag,
    (1 + (random()*263)::int)                                                  AS pulocationid,
    (1 + (random()*263)::int)                                                  AS dolocationid,
    (ARRAY[0,1,2,3,4,5,6])[1 + (random()*6)::int]::smallint                    AS payment_type,
    round((5 + random()*35)::numeric, 2)                                       AS fare_amount,
    round((CASE WHEN random()<0.2 THEN 0.5 ELSE 0 END)::numeric, 2)            AS extra,
    0.50::numeric(10,2)                                                        AS mta_tax,
    round((CASE WHEN random()<0.6 THEN random()*5 ELSE 0 END)::numeric, 2)     AS tip_amount,
    round((CASE WHEN random()<0.3 THEN random()*8 ELSE 0 END)::numeric, 2)     AS tolls_amount,
    0.30::numeric(10,2)                                                        AS improvement_surcharge,
    2.75::numeric(10,2)                                                        AS congestion_surcharge,
    round((CASE WHEN random()<0.1 THEN 1.25 ELSE 0 END)::numeric, 2)           AS airport_fee,
    round((CASE WHEN random()<0.4 THEN 1.00 ELSE 0 END)::numeric, 2)           AS cbd_congestion_fee
  FROM generate_series(1,200) AS gs
)
INSERT INTO public.yellow_trips (
  vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
  ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
  total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee
)
SELECT
  vendorid, pu, do_ts,  -- < dùng do_ts
  passenger_count, trip_distance,
  ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
  round(
    fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge
    + congestion_surcharge + airport_fee + cbd_congestion_fee
  , 2) AS total_amount,
  congestion_surcharge, airport_fee, cbd_congestion_fee
FROM s;
