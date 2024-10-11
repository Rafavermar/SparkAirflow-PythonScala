import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "Rafael Vera-Maranon",
    "start_date": airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id="sparking_flow",
    default_args=default_args,
    schedule_interval="@daily",
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag,
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag,
)

scala_job = SparkSubmitOperator(
   task_id="scala_job",
   conn_id="spark-conn",
   application="jobs/scala/target/scala-2.12/word-count_2.12-0.1.jar",
   dag=dag
)


start >> [python_job, scala_job] >> end
