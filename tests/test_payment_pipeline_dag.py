import pytest

try:
    from airflow.models import DagBag
except ImportError:
    pytest.skip("Airflow not installed, skipping DAG tests", allow_module_level=True)


def test_dag_is_loaded_successfully():
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)  
    # Load DAG files from airflow/dags folder

    dag = dag_bag.get_dag("payment_streaming_pipeline")  
    # Fetch your DAG by dag_id

    assert dag is not None  
    # DAG should load successfully


def test_dag_has_expected_id():
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)  
    # Load DAGs

    dag = dag_bag.get_dag("payment_streaming_pipeline")  
    # Fetch DAG

    assert dag.dag_id == "payment_streaming_pipeline"  
    # DAG id should match expected value


def test_dag_has_expected_tasks():
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)  
    # Load DAGs

    dag = dag_bag.get_dag("payment_streaming_pipeline")  
    # Fetch DAG

    expected_tasks = {
        "start_kafka_producer",
        "submit_flink_job",
        "monitor_flink_checkpoint",
        "validate_aggregates",
        "refresh_grafana",
    }  
    # These are the tasks expected in your DAG

    actual_tasks = {task.task_id for task in dag.tasks}  
    # Collect actual task_ids from DAG

    assert expected_tasks == actual_tasks  
    # All expected tasks should exist


def test_dag_task_order_is_correct():
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)  
    # Load DAGs

    dag = dag_bag.get_dag("payment_streaming_pipeline")  
    # Fetch DAG

    assert dag.get_task("start_kafka_producer").downstream_task_ids == {"submit_flink_job"}  
    # Producer task should flow to Flink job submission

    assert dag.get_task("submit_flink_job").downstream_task_ids == {"monitor_flink_checkpoint"}  
    # Flink job submission should flow to checkpoint monitoring

    assert dag.get_task("monitor_flink_checkpoint").downstream_task_ids == {"validate_aggregates"}  
    # Checkpoint monitoring should flow to aggregate validation

    assert dag.get_task("validate_aggregates").downstream_task_ids == {"refresh_grafana"}  
    # Validation should flow to Grafana refresh


def test_dag_default_args_present():
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)  
    # Load DAGs

    dag = dag_bag.get_dag("payment_streaming_pipeline")  
    # Fetch DAG

    assert "owner" in dag.default_args  
    # default_args should contain owner

    assert "start_date" in dag.default_args  
    # default_args should contain start_date

    assert "retries" in dag.default_args  
    # default_args should contain retries


def test_dag_schedule_and_catchup_settings():
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)  
    # Load DAGs

    dag = dag_bag.get_dag("payment_streaming_pipeline")  
    # Fetch DAG

    assert dag.catchup is False  
    # catchup should be False so old runs are not backfilled