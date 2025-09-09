from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(hours=24), 
    catchup=False,
    tags=["nyc_taxi", "batch", "spark"]
) as dag:
    
    run_spark_job = SparkSubmitOperator(
        task_id="transform_and_load_data",
        application="/src/ETL/batch_processor.py",
        conn_id="spark_default",  # Cần được cấu hình trong Airflow
        # Các tham số khác như packages và conf sẽ được thêm vào
    )