from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# DAG 기본 설정
default_args = {
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


dag = DAG(
    dag_id="20m_feature_update",
    start_date=datetime(2025, 8, 1),
    schedule="10 0 * * *",  # 매일 00:10
    catchup=False,
    default_args=default_args,
)

task0 = BashOperator(
    task_id="start_flink_job",
    bash_command="echo 'start flink job'",
    dag=dag,
)

task1 = BashOperator(
    task_id="flink_remote_submit",
    bash_command=(
        "flink run -m jobmanager:8081 "
        "-py /opt/airflow/flink-jobs/feature_batch_update.py "
        "--processingDate '{{ ds }}'"
    ),
    dag=dag,
)

task0 >> task1
