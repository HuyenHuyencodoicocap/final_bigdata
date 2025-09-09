CREATE DATABASE IF NOT EXISTS nyc_taxi;

USE nyc_taxi;

CREATE TABLE IF NOT EXISTS yellow_trips
(
    -- Các cột từ message Debezium
    trip_id Int64,
    VendorID Nullable(Int16),
    tpep_pickup_datetime DateTime64(6, 'UTC'),  -- Debezium MicroTimestamp -> DateTime64(6)
    tpep_dropoff_datetime DateTime64(6, 'UTC'), -- Debezium MicroTimestamp -> DateTime64(6)
    passenger_count Nullable(Int16),
    trip_distance Nullable(Float64),
    RatecodeID Nullable(Int16),
    store_and_fwd_flag Nullable(String),
    PULocationID Nullable(Int32),
    DOLocationID Nullable(Int32),
    payment_type Nullable(Int16),
    fare_amount Nullable(Float64),
    extra Nullable(Float64),
    mta_tax Nullable(Float64),
    tip_amount Nullable(Float64),
    tolls_amount Nullable(Float64),
    improvement_surcharge Nullable(Float64),
    total_amount Nullable(Float64),
    congestion_surcharge Nullable(Float64),
    Airport_fee Nullable(Float64),
    cbd_congestion_fee Nullable(Float64),

    -- Các cột để xử lý CDC
    -- sign Int8,
    -- version UInt64 -- Sử dụng ts_ms từ source của Debezium

)
ENGINE = ReplacingMergeTree()
PRIMARY KEY (trip_id)
ORDER BY (trip_id);