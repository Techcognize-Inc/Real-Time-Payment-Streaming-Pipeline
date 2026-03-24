import json
import os
from datetime import datetime
import psycopg2


# ---------------------------------------------------
# 0. Ensure PostgreSQL table exists before job starts
# ---------------------------------------------------
def create_table_if_not_exists():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "payments"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_aggregates (
                    bank_code VARCHAR,
                    total_transactions INT,
                    failed_transactions INT,
                    success_rate FLOAT,
                    total_amount FLOAT,
                    window_start VARCHAR
                );
            """
            )
    conn.close()


create_table_if_not_exists()
# 👉 Creates the output table if it doesn't exist yet — safe to run on every start


# Flink core APIs
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.common import WatermarkStrategy
from pyflink.datastream import OutputTag
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, ProcessFunction

# Kafka + JDBC connectors
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)


# ---------------------------------------------------
# 1. Create Flink Environment + Checkpointing
# ---------------------------------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
# 👉 Creates Flink runtime environment
# 👉 parallelism=1 means single thread (easy for demo/debug)

# Enable checkpointing every 60 seconds (exactly-once semantics)
env.enable_checkpointing(60000, CheckpointingMode.EXACTLY_ONCE)
checkpoint_config = env.get_checkpoint_config()
checkpoint_config.set_checkpoint_timeout(30000)  # 👉 Fail if checkpoint takes > 30s
checkpoint_config.set_min_pause_between_checkpoints(
    10000
)  # 👉 At least 10s between checkpoints
checkpoint_config.set_max_concurrent_checkpoints(1)  # 👉 Only one checkpoint at a time
# 👉 Checkpointing saves job state so it can recover from failures
# 👉 EXACTLY_ONCE ensures no duplicate or lost records


# ---------------------------------------------------
# 2. Kafka Source (input stream)
# ---------------------------------------------------
source = (
    KafkaSource.builder()
    .set_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    .set_topics("payments_raw")
    .set_group_id("flink-group")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)
# 👉 Connects to Kafka broker (host read from env var, defaults to kafka:9092)
# 👉 Reads messages from topic: payments_raw
# 👉 Messages are read as plain strings (JSON)


stream = env.from_source(
    source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="Kafka Source",
)
# 👉 Converts Kafka data into Flink stream
# 👉 Uses processing-time watermark strategy


# ---------------------------------------------------
# 3. Parse JSON data — valid records on main stream, bad records to DLQ
# ---------------------------------------------------
dlq_tag = OutputTag("dlq", Types.STRING())
# 👉 Side-output tag for dead letter records (raw string preserved for inspection)


class ParseEvent(ProcessFunction):
    def process_element(self, value, ctx):
        try:
            data = json.loads(value)
            yield (
                data["bank_code"],  # grouping key
                data["status"],  # SUCCESS / FAILED
                float(data.get("amount", 0.0)),  # transaction amount
            )
        except (json.JSONDecodeError, KeyError) as e:
            # 👉 Route malformed / missing-field records to the DLQ topic
            dlq_payload = json.dumps({"raw": value, "error": str(e)})
            ctx.output(dlq_tag, dlq_payload)


parsed_stream = stream.process(
    ParseEvent(),
    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]),
)
# 👉 Valid records continue on parsed_stream; bad records go to the DLQ side output

dlq_stream = parsed_stream.get_side_output(dlq_tag)
# 👉 Separate stream containing only the failed / malformed records


# ---------------------------------------------------
# 4. Key by bank_code
# ---------------------------------------------------
keyed_stream = parsed_stream.key_by(lambda x: x[0])
# 👉 Groups data by bank_code
# 👉 All transactions of the same bank go to the same parallel slot


# ---------------------------------------------------
# 5. Window + Aggregation logic
# ---------------------------------------------------
class PaymentAggregator(ProcessWindowFunction):

    def process(self, key, context, elements):
        total = 0
        failed = 0
        total_amount = 0.0

        for e in elements:
            total += 1
            total_amount += e[2]
            if e[1] == "FAILED":
                failed += 1
        # 👉 Loop through all records in window
        # 👉 Count total / failed transactions and sum amount

        success_rate = (total - failed) / total if total > 0 else 0.0
        # 👉 Calculate success rate

        window_start = datetime.fromtimestamp(context.window().start / 1000)
        # 👉 Get window start time

        yield Row(
            key,  # bank_code
            total,  # total transactions
            failed,  # failed transactions
            success_rate,  # success rate
            round(total_amount, 2),  # total transaction value
            window_start.strftime("%Y-%m-%d %H:%M:%S"),  # window start timestamp
        )
        # 👉 Output aggregated result per bank per window as Row (required by JdbcSink)


aggregated_stream = keyed_stream.window(
    TumblingProcessingTimeWindows.of(Time.minutes(1))
).process(
    PaymentAggregator(),
    output_type=Types.ROW(
        [
            Types.STRING(),  # bank_code
            Types.INT(),  # total
            Types.INT(),  # failed
            Types.FLOAT(),  # success_rate
            Types.FLOAT(),  # total_amount
            Types.STRING(),  # window_start
        ]
    ),
)
# 👉 Creates 1-minute tumbling window
# 👉 Runs aggregation for each bank per window


# ---------------------------------------------------
# 6. PostgreSQL Sink (output)
# ---------------------------------------------------
db_host = os.getenv("POSTGRES_HOST", "postgres")
db_port = os.getenv("POSTGRES_PORT", "5432")
db_name = os.getenv("POSTGRES_DB", "payments")
db_user = os.getenv("POSTGRES_USER", "postgres")
db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")
# 👉 DB credentials loaded from environment variables

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

sink = JdbcSink.sink(
    """
    INSERT INTO payment_aggregates
    (bank_code, total_transactions, failed_transactions, success_rate, total_amount, window_start)
    VALUES (?, ?, ?, ?, ?, ?)
    """,
    type_info=Types.ROW(
        [
            Types.STRING(),
            Types.INT(),
            Types.INT(),
            Types.FLOAT(),
            Types.FLOAT(),
            Types.STRING(),
        ]
    ),
    jdbc_connection_options=(
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(jdbc_url)
        .with_driver_name("org.postgresql.Driver")
        .with_user_name(db_user)
        .with_password(db_pass)
        .build()
    ),
    jdbc_execution_options=(
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(200)  # 👉 Flush every 200ms
        .with_batch_size(100)  # 👉 Or when 100 rows are buffered
        .with_max_retries(3)  # 👉 Retry up to 3 times on failure
        .build()
    ),
)
# 👉 Writes aggregated results into PostgreSQL using the modern JdbcConnectionOptions API
# 👉 Batching reduces DB round-trips; retries handle transient failures


aggregated_stream.add_sink(sink)
# 👉 Connects stream output to database


# ---------------------------------------------------
# 7. Failure Alert Sink — banks with failure rate > 5%
# ---------------------------------------------------
alert_stream = aggregated_stream.filter(lambda x: x.f3 < 0.95)
# 👉 f3 is success_rate — filter keeps only banks below 95% success (>5% failure)

alert_message_stream = alert_stream.map(
    lambda x: json.dumps(
        {
            "bank_code": x.f0,
            "total_transactions": x.f1,
            "failed_transactions": x.f2,
            "failure_rate_pct": round((1 - x.f3) * 100, 2),
            "window_start": x.f5,
            "alert": f"ALERT: {x.f0} failure rate {round((1 - x.f3) * 100, 2)}% exceeds 5% threshold",
        }
    ),
    output_type=Types.STRING(),
)
# 👉 Formats alert as JSON with bank, failure rate, and human-readable alert message

alerts_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("payments.alerts")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .build()
)
# 👉 Sends alert messages to payments_alerts Kafka topic

alert_message_stream.sink_to(alerts_sink)
# 👉 Any bank exceeding 5% failure rate in a window triggers an alert


# ---------------------------------------------------
# 8. Dead Letter Queue — Kafka sink for failed records
# ---------------------------------------------------
dlq_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("payments_dlq")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .build()
)
# 👉 Writes malformed / unprocessable records to Kafka topic: payments_dlq
# 👉 Each message is JSON: {"raw": "<original message>", "error": "<reason>"}
# 👉 Allows ops team to inspect, fix, and replay failed records without data loss

dlq_stream.sink_to(dlq_sink)
# 👉 Connects the DLQ side output to the Kafka DLQ sink


# ---------------------------------------------------
# 9. Execute Flink Job
# ---------------------------------------------------
env.execute("Payment Stream Processor - DataStream API")
# 👉 Starts the streaming job
# 👉 Flink runs continuously and processes real-time data
