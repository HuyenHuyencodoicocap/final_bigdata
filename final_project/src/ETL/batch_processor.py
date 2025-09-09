from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.functions import from_json, col, date_format, to_timestamp, when

# Khởi tạo Spark Session
spark = SparkSession.builder.appName("NYC_Taxi_Batch_Processor").getOrCreate()

# 1. Định nghĩa Schema
schema = StructType([
    StructField("trip_id", LongType(), True),
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True), 
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True)
])

# 2. Extract (Trích xuất)
# Đọc dữ liệu từ Kafka
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nycdb.public.yellow_trips") \
    .option("startingOffsets", "earliest") \
    .load()

# Giải mã dữ liệu JSON và chọn các cột cần thiết
data_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

print("Bước extract dữ liệu từ Kafka thành công.")
# 3. Transform (Chuyển đổi)
# Chuyển đổi các cột datetime từ String sang Timestamp
transformed_df = data_df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
                        .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))

# Lọc bỏ các bản ghi không hợp lệ
transformed_df = transformed_df.filter(
    (col("passenger_count") >= 0) & 
    (col("trip_distance") >= 0)
)

# Xử lý giá trị null và âm của các trường tiền tệ
transformed_df = transformed_df.withColumn(
    "tip_amount", 
    when(col("tip_amount").isNull() | (col("tip_amount") < 0), 0)
    .otherwise(col("tip_amount"))
)

transformed_df = transformed_df.withColumn(
    "tolls_amount", 
    when(col("tolls_amount").isNull() | (col("tolls_amount") < 0), 0)
    .otherwise(col("tolls_amount"))
)

transformed_df = transformed_df.withColumn(
    "fare_amount", 
    when(col("fare_amount").isNull() | (col("fare_amount") < 0), 0)
    .otherwise(col("fare_amount"))
)

# Thêm các cột phân vùng để Iceberg tổ chức dữ liệu
transformed_df = transformed_df.withColumn("trip_date", col("tpep_pickup_datetime").cast("date")) \
                               .withColumn("trip_month", date_format(col("tpep_pickup_datetime"), "yyyy-MM")) \
                               .withColumn("trip_hour", date_format(col("tpep_pickup_datetime"), "HH"))

print("Bước tranform dữ liệu thành công.")
# 4. Load (Tải)
# Tạo database nếu chưa có (chỉ chạy lần đầu)
spark.sql("CREATE DATABASE IF NOT EXISTS hive_catalog.nyc_taxi")

# Ghi dữ liệu đã chuyển đổi vào bảng Iceberg
transformed_df.write \
    .format("iceberg") \
    .mode("append") \
    .partitionBy("trip_month", "trip_date") \
    .save("hive_catalog.nyc_taxi.yellow_trips")

print("Ghi dữ liệu vào Iceberg thành công.")

spark.stop()
