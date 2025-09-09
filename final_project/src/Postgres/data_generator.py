import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import time
import os
from sqlalchemy import text # Make sure to add this import

# Database connection information
db_user = 'postgres'
db_password = 'postgres'
db_host = 'localhost'
db_port = '5432'
db_name = 'nyc_taxi'

# Create the engine string
engine_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(engine_string)

def insert_random_rows_periodically(parquet_file_path, table_name, interval_seconds=20, num_rows=10):
    
    if not os.path.exists(parquet_file_path):
        print(f"Error: Parquet file not found at {parquet_file_path}")
        return

    try:
        # Read the entire Parquet file into a DataFrame
        print(f"Reading data from {parquet_file_path}...")
        df = pd.read_parquet(parquet_file_path)
        total_rows = len(df)
        print(f"Successfully read {total_rows} rows.")

        with engine.connect() as conn:
            while True:
                try:
                    # Sample a random subset of rows
                    sampled_df = df.sample(n=num_rows)

                    # Insert the sampled data into the database
                    start_time = time.time()
                    sampled_df.to_sql(table_name, conn, if_exists='append', index=False)
                    end_time = time.time()

                    print(f'Successfully inserted {num_rows} random rows into "{table_name}".')
                    print(f'Time taken for this insert: {end_time - start_time:.4f} seconds.')

                    # Wait for the specified interval before the next insertion
                    print(f'Waiting for {interval_seconds} seconds...')
                    time.sleep(interval_seconds)
                except Exception as e:
                    print(f"An error occurred during insertion: {e}")
                    print(f"Waiting for {interval_seconds} seconds before retrying...")
                    time.sleep(interval_seconds)
    except Exception as e:
        print(f"An error occurred: {e}")


def update_random_rows_periodically(table_name, interval_seconds=60, num_rows=5):
    while True:
        try:
            with engine.connect() as conn:
                with conn.begin() as transaction:
                    try:
                        # Your SQL generation code remains the same
                        query_trip_ids = f'SELECT "trip_id" FROM {table_name} ORDER BY RANDOM() LIMIT {num_rows};'
                        trip_ids_to_update = pd.read_sql_query(query_trip_ids, conn)['trip_id'].tolist()
                        
                        if not trip_ids_to_update:
                            print("Không tìm thấy trip_id nào để cập nhật. Hãy đảm bảo bảng đã có dữ liệu.")
                            continue

                        trip_ids_str = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in trip_ids_to_update])
                        update_query = f"""
                            UPDATE {table_name}
                            SET "store_and_fwd_flag" = 'N'
                            WHERE trip_id IN ({trip_ids_str});
                        """
                        
                        # Fix: Use text() to wrap your raw SQL query
                        result = conn.execute(text(update_query)) 
                        
                        print(f'Đã cập nhật thành công {result.rowcount} hàng ngẫu nhiên trong bảng "{table_name}".')
                        print(f'Các trip_id đã được cập nhật: {trip_ids_to_update}')

                    except Exception as e:
                        transaction.rollback()
                        print(f"Đã xảy ra lỗi trong quá trình cập nhật: {e}")
                        print(f"Đang chờ {interval_seconds} giây trước khi thử lại...")
                        time.sleep(interval_seconds)
                        
        except Exception as e:
            print(f"Đã xảy ra lỗi kết nối cơ sở dữ liệu: {e}")
        
        print(f'Đang chờ {interval_seconds} giây...')
        time.sleep(interval_seconds)
        
def delete_random_rows_periodically(table_name, interval_seconds=60, num_rows=5):
   
    while True:
        try:
            with engine.connect() as conn:
                with conn.begin() as transaction:
                    try:
                        # 1. Query a random subset of trip IDs to delete
                        # Use double quotes if the column name is case-sensitive
                        query_trip_ids = f'SELECT "trip_id" FROM {table_name} ORDER BY RANDOM() LIMIT {num_rows};'
                        trip_ids_to_delete = pd.read_sql_query(query_trip_ids, conn)['trip_id'].tolist()
                        
                        if not trip_ids_to_delete:
                            print("Không tìm thấy trip_id nào để xóa. Hãy đảm bảo bảng đã có dữ liệu.")
                            time.sleep(interval_seconds)
                            continue

                        # 2. Create the SQL DELETE statement
                        trip_ids_str = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in trip_ids_to_delete])
                        delete_query = f"""
                            DELETE FROM {table_name}
                            WHERE trip_id IN ({trip_ids_str});
                        """
                        
                        # 3. Execute the DELETE statement using text()
                        result = conn.execute(text(delete_query))
                        
                        print(f'Đã xóa thành công {result.rowcount} hàng ngẫu nhiên trong bảng "{table_name}".')
                        print(f'Các trip_id đã được xóa: {trip_ids_to_delete}')

                    except Exception as e:
                        # Rollback the transaction if any error occurs
                        transaction.rollback()
                        print(f"Đã xảy ra lỗi trong quá trình xóa: {e}")
                        print(f"Đang chờ {interval_seconds} giây trước khi thử lại...")
                        time.sleep(interval_seconds)
                        
        except Exception as e:
            print(f"Đã xảy ra lỗi kết nối cơ sở dữ liệu: {e}")
        
        # Wait for the next deletion interval
        print(f'Đang chờ {interval_seconds} giây...')
        time.sleep(interval_seconds)

        
# Example usage of the function
if __name__ == '__main__':
    parquet_file = r'src\Data_Source\yellow_tripdata_2025-01.parquet'
    db_table = 'yellow_trips'
    # insert_random_rows_periodically(parquet_file, db_table, interval_seconds=20, num_rows=10)
    #update_random_rows_periodically(db_table, interval_seconds=60, num_rows=5)
    delete_random_rows_periodically(db_table, interval_seconds=60, num_rows=5)