# Lệnh khởi động dự án
docker compose up -d
# kiểm tra Postgres đã sẵn sàng
docker compose exec -T postgres pg_isready -U postgres -d nyc_taxi
# tạo cấu trúc bảng dữ liệu và thêm dữ liệu
docker compose exec -T postgres psql -U postgres -d nyc_taxi -f /sql/schema_trips.sql
<!-- docker compose exec -T postgres psql -U postgres -d nyc_taxi -f /sql/seed_trips.sql -->
python ".\src\Postgres\load_parquet_to_postgres.py"
# test xem dữ liệu đã tồn tại chưa
docker compose exec -T postgres psql -U postgres -d nyc_taxi -c "SELECT COUNT(*) FROM public.yellow_trips;"
# tạo connector postgres vs debezium
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  --data-binary "@src/Connector/pg-nyc-source.json"
# kiểm tra trạng thái của connector
curl.exe http://localhost:8083/connectors/pg-nyc-connector/status
# Lệnh kiểm tra các csdl trên clickhouse
docker compose exec -T clickhouse clickhouse-client -q "SHOW DATABASES;"
# tạo connector với clickhouse
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  --data-binary "@src/Connector/clickhouse-sink.json"
# kiểm tra trạng thái connector với clickhouse
curl.exe http://localhost:8083/connectors/clickhouse-sink/status
# Chạy file data_generator.py để tự động insert, update và delete dữ liệu
# lệnh chạy các file .py dùng spark, lệnh kèm các lệnh chạy các gói cài đặt iceberg
# lệnh chạy quá trình ETL của batch processing ( file batch_processor.py)
docker-compose exec spark spark-submit --master local[*] --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.hive_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.hive_catalog.type=hive --conf spark.sql.catalog.hive_catalog.uri=thrift://hive-metastore:9083 /app/ETL/batch_processor.py

  


