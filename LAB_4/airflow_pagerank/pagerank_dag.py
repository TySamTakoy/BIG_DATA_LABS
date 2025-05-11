from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pagerank_pipeline',
    default_args=default_args,
    description='Периодический запуск PageRank',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['pagerank', 'spark'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Запуск DAG PageRank..."',
    )

    compute = BashOperator(
        task_id='compute_pagerank',
        bash_command='spark-submit /opt/airflow/scripts/run_pagerank.py',
    )

    convert_to_html = BashOperator(
        task_id='convert_csv_to_html',
        bash_command='python3 /opt/airflow/scripts/csv_to_html.py',
    )

    finish = BashOperator(
        task_id='end',
        bash_command='echo "Завершено."',
    )

    start >> compute >> convert_to_html >> finish
