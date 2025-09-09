-- BẢNG NYC Yellow Taxi Trips (đầy đủ cột theo data dictionary)
-- Nếu dùng Debezium: đảm bảo bảng ở schema public và đã add vào table.include.list

CREATE TABLE IF NOT EXISTS public.yellow_trips (
  -- Khóa chính kỹ thuật (dữ liệu gốc không có id duy nhất)
  trip_id                     BIGSERIAL PRIMARY KEY,

  -- Từ điển cột chính
  "VendorID"                  SMALLINT,                      -- 1=CMT, 2=Curb, 6=Myle, 7=Helix
  tpep_pickup_datetime        TIMESTAMP NOT NULL,            -- thời điểm bật đồng hồ
  tpep_dropoff_datetime       TIMESTAMP NOT NULL,            -- thời điểm tắt đồng hồ
  passenger_count             SMALLINT,                      -- số khách
  trip_distance               NUMERIC(9,3) CHECK (trip_distance >= 0), -- quãng đường (mile)

  "RatecodeID"                SMALLINT,                      -- 1=Standard,2=JFK,3=Newark,4=Nassau/Westchester,5=Negotiated,6=Group,99=Unknown
  store_and_fwd_flag          CHAR(1) CHECK (store_and_fwd_flag IN ('Y','N')), -- lưu đệm khi mất mạng

  "PULocationID"              INTEGER,                       -- TLC Zone pick-up
  "DOLocationID"              INTEGER,                       -- TLC Zone drop-off

  payment_type                SMALLINT,                      -- 0=Flex Pay,1=Credit,2=Cash,3=No charge,4=Dispute,5=Unknown,6=Voided

  fare_amount                 NUMERIC(10,2),                 -- tiền cước (meter)
  extra                       NUMERIC(10,2),                 -- phụ phí khác
  mta_tax                     NUMERIC(10,2),                 -- thuế MTA
  tip_amount                  NUMERIC(10,2),                 -- tiền tip (thẻ tự động populate)
  tolls_amount                NUMERIC(10,2),                 -- tiền cầu đường
  improvement_surcharge       NUMERIC(10,2),                 -- phụ phí cải thiện (từ 2015)
  total_amount                NUMERIC(10,2),                 -- tổng thu khách (không gồm cash tips)
  congestion_surcharge        NUMERIC(10,2),                 -- phụ phí tắc đường NYC
  "Airport_fee"                 NUMERIC(10,2),               -- phụ phí sân bay (LGA/JFK)
  cbd_congestion_fee          NUMERIC(10,2),                 -- phụ phí CBD (từ 2025-01-05)

  -- Ràng buộc “nhẹ” để tránh giá trị vô lý (cho phép âm khi điều chỉnh/refund)
  CHECK (fare_amount IS NULL OR fare_amount > -10000),
  CHECK (total_amount IS NULL OR total_amount > -10000)
);

---

-- Gợi ý index phục vụ truy vấn/ETL
CREATE INDEX IF NOT EXISTS idx_yellow_pickup_ts ON public.yellow_trips (tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_yellow_dropoff_ts ON public.yellow_trips (tpep_dropoff_datetime);
CREATE INDEX IF NOT EXISTS idx_yellow_pu ON public.yellow_trips ("PULocationID");
CREATE INDEX IF NOT EXISTS idx_yellow_do ON public.yellow_trips ("DOLocationID");
CREATE INDEX IF NOT EXISTS idx_yellow_payment ON public.yellow_trips (payment_type);