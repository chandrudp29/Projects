from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'chandru',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_wordcount',
    default_args=default_args,
    description='Run Spark WordCount daily',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
)

spark_submit_cmd = """
spark-submit \
--master local[*] \
/path/to/your/project/spark_jobs/wordcount.py \
/path/to/input/data/input.txt \
/path/to/output/data/output
"""

run_spark_wordcount = BashOperator(
    task_id='run_spark_wordcount',
    bash_command=spark_submit_cmd,
    dag=dag,
)

run_spark_wordcount
