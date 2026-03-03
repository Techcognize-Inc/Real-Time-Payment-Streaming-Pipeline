# DE-4-Real-Time-Payment-Streaming-Pipeline
Designing and implementing an event-driven real-time payment processing pipeline that handles high-volume transaction streams, ensures data quality and consistency, and enables low-latency data delivery for analytics, monitoring, and reporting systems.


                      ┌─────────────────────┐
                      │ Python Faker        │
                      │ Payment Producer    │
                      └──────────┬──────────┘
                                 │
                                 ▼
                      ┌─────────────────────┐
                      │   Kafka             │
                      │ Topic: payments.raw │
                      │ (3 partitions)      │
                      └──────────┬──────────┘
                                 │
                                 ▼
               ┌────────────────────────────────┐
               │      Apache Flink              │
               │  PaymentStreamProcessor        │
               │--------------------------------│
               │ keyBy(bank_code)               │
               │ 1-min Tumbling Window          │
               │ Compute Aggregates             │
               │ Detect Failure > 5%            │
               │ Checkpointing (Exactly Once)   │
               └──────────────┬─────────────────┘
                     ┌────────┴─────────┐
                     ▼                  ▼
        ┌────────────────────┐   ┌────────────────────┐
        │ PostgreSQL         │   │ Kafka              │
        │ payment_aggregates │   │ payments.alerts    │
        └────────────┬───────┘   └────────────────────┘
                     ▼
            ┌──────────────────┐
            │ Prometheus       │
            │ → Grafana        │
            └──────────────────┘