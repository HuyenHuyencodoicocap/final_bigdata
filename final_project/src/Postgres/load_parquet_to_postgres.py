import pandas as pd
df = pd.read_parquet('src\Data_Source\yellow_tripdata_2025-01.parquet')
import psycopg2
from sqlalchemy import create_engine
import time
start_time = time.time()
# Thông tin kết nối
db_user = 'postgres'
db_password = 'postgres'
db_host = 'localhost' # hoặc địa chỉ IP của máy chủ
db_port = '5432'
db_name = 'nyc_taxi'

# Tạo chuỗi kết nối
engine_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(engine_string)

# Tên bảng bạn muốn tạo hoặc ghi đè
table_name = 'yellow_trips'

# Sử dụng 'if_exists' để xử lý trường hợp bảng đã tồn tại:
# 'fail': báo lỗi nếu bảng tồn tại
# 'replace': xóa và tạo lại bảng
# 'append': thêm dữ liệu vào cuối bảng
with engine.connect() as conn:
    df.to_sql(table_name, conn, if_exists='append', index=False)
print(f'Dữ liệu từ file Parquet đã được nạp vào bảng "{table_name}" thành công!')
end_time = time.time()
print(f'Thời gian thực hiện: {end_time - start_time} giây')
