from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_review_submission_dag',
    default_args=default_args,
    description='Submit review to Kafka via Flask app',
    schedule_interval=None,  # Only run manually
)

submit_review = BashOperator(
    task_id='submit_review_to_kafka',
    bash_command='python3 /usr/local/airflow/scripts/submit_review.py',
    dag=dag,
)

submit_review
