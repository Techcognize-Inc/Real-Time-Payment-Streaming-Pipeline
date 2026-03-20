from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "team4", "start_date": datetime(2024, 1, 1), "retries": 1}

with DAG(
    dag_id="payment_streaming_pipeline",
    default_args=default_args,
    schedule_interval=None,  ## 0 0 */14 * * -  If we want to run every 14 days.
    catchup=False,
) as dag:

    start_kafka_producer = BashOperator(
        task_id="start_kafka_producer",
        bash_command="python /opt/airflow/dags/producer/payment_producer.py",
    )

    submit_flink_job = BashOperator(
        task_id="submit_flink_job",
        bash_command="""
        docker exec flink-jobmanager bash -c "flink run -py /opt/flink/usrlib/payment_stream_processor.py"
        """,
    )

    monitor_flink_checkpoint = BashOperator(
        task_id="monitor_flink_checkpoint",
        bash_command="curl http://flink-jobmanager:8081/jobs",
    )

    validate_aggregates = BashOperator(
        task_id="validate_aggregates",
        bash_command="""
        docker exec postgres psql -U postgres -d payments \
        -c "SELECT * FROM payment_aggregates LIMIT 5;"
        """,
    )

    refresh_grafana = BashOperator(
        task_id="refresh_grafana", bash_command="curl http://grafana:3000", dag=dag
    )

    (
        start_kafka_producer
        >> submit_flink_job
        >> monitor_flink_checkpoint
        >> validate_aggregates
        >> refresh_grafana
    )
