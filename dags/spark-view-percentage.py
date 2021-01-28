from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"

video_views = "/usr/local/spark/resources/data/video_views.csv"
videos = "/usr/local/spark/resources/data/videos.json"
postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        "spark-postgres-percentage", 
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job_load_data = SparkSubmitOperator(
    task_id="job-load-data",
    application="/usr/local/spark/app/load-data.py",
    name="load-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[video_views,videos,postgres_db,postgres_user,postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

spark_job_calculation = SparkSubmitOperator(
    task_id="job-calculate-data",
    application="/usr/local/spark/app/calculate-data.py",
    name="read-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[postgres_db,postgres_user,postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job_load_data >> spark_job_calculation >> end