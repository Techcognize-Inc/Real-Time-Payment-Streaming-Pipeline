Project Overview 
The Real-Time Payment Streaming Pipeline continuously ingests payment events, performs streaming transformations and 1-minute windowed aggregations, detects abnormal failure patterns, and publishes analytics outputs and alerts for monitoring and operational response.

Primary objectives
Near-real-time monitoring of payment transaction health by bank
Anomaly detection for elevated failure rates (per bank, per window)
Operational visibility via metrics, dashboards, and alert topics
Reliability & fault tolerance via checkpointing and Dead Letter Queue (DLQ) handling

Core outcomes
Aggregated per-bank metrics persisted in PostgreSQL for downstream analytics
Alerts published to Kafka when failure-rate breaches a defined threshold
End-to-end automation via Airflow orchestration and Jenkins CI validation

2. Architecture Overview
The platform follows a streaming-first architecture: events are produced continuously, processed in Flink, aggregated by bank in time windows, persisted for analytics, and monitored for anomalies.

2.1 Key Design Decisions (Operationally Relevant)

Partitioning / message key: Events are keyed by bank_code to keep bank-level aggregations consistent and scalable.
Windowing strategy: 1-minute tumbling windows enable fast detection of failure spikes with predictable aggregation cadence.
Fault tolerance: Flink checkpointing ensures stateful processing resilience.
Data quality controls: Malformed/unprocessable events are routed to a DLQ (payments_dlq) instead of crashing the job.
