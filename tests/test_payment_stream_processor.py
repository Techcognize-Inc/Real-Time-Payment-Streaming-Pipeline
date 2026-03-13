from pathlib import Path

# Path to your Flink stream processor file
STREAM_PROCESSOR_FILE = Path("flink_job/payment_stream_processor.py")


def test_stream_processor_file_exists():
    assert STREAM_PROCESSOR_FILE.exists()  
    # File should exist in project root


def test_stream_processor_uses_kafka_connector():
    content = STREAM_PROCESSOR_FILE.read_text(encoding="utf-8")  
    # Read full file content as text

    assert "'connector' = 'kafka'" in content or '"connector" = "kafka"' in content  
    # File should define Kafka connector in Flink source table


def test_stream_processor_uses_correct_kafka_topic():
    content = STREAM_PROCESSOR_FILE.read_text(encoding="utf-8")  
    # Read file content

    assert "payments_raw" in content  
    # Flink source topic should be payments_raw


def test_stream_processor_uses_correct_bootstrap_server():
    content = STREAM_PROCESSOR_FILE.read_text(encoding="utf-8")  
    # Read file content

    assert "kafka:9092" in content  
    # Flink should use kafka service name and port 9092 inside Docker network


def test_stream_processor_has_consumer_group():
    content = STREAM_PROCESSOR_FILE.read_text(encoding="utf-8")  
    # Read file content

    assert "group.id" in content or "flink-consumer-group" in content  
    # Kafka source should define consumer group for Flink job


def test_stream_processor_enables_checkpointing():
    content = STREAM_PROCESSOR_FILE.read_text(encoding="utf-8")  
    # Read file content

    assert "execution.checkpointing.interval" in content  
    # Stream processor should configure checkpointing interval


def test_stream_processor_uses_jdbc_sink():
    content = STREAM_PROCESSOR_FILE.read_text(encoding="utf-8")  
    # Read file content

    assert "'connector' = 'jdbc'" in content or '"connector" = "jdbc"' in content  
    # Sink table should use JDBC connector for PostgreSQL


def test_stream_processor_uses_postgres_url():
    content = STREAM_PROCESSOR_FILE.read_text(encoding="utf-8")  
    # Read file content

    assert "jdbc:postgresql://postgres:5432/payments" in content  
    # JDBC URL should point to postgres container and payments database


def test_stream_processor_creates_payments_sink_table():
    content = STREAM_PROCESSOR_FILE.read_text(encoding="utf-8")  
    # Read file content

    assert "CREATE TABLE payments_sink" in content  
    # Stream processor should define payments_sink table


def test_stream_processor_creates_aggregate_or_alert_tables():
    content = STREAM_PROCESSOR_FILE.read_text(encoding="utf-8")  
    # Read file content

    assert (
        "payment_aggregates" in content
        or "payments_alerts" in content
        or "payments_fraud_alerts" in content
    )  
    # File should contain aggregate table or alert table logic