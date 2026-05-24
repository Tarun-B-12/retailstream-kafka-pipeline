# RetailStream Architecture

## Overview
A real-time retail event streaming pipeline built with Confluent Cloud Kafka,
Python, DuckDB, and Streamlit.

## Pipeline Flow

```text
[ Python Producer ]
  Generates synthetic retail transaction events
  Publishes to Confluent Cloud Kafka topic: retail-transactions
  Rate: 1 event per second (configurable)
        |
        v
[ Confluent Cloud Kafka ]
  Managed Kafka broker
  Topic: retail-transactions
  Partitions: 6
        |
        v
[ Python Consumer ]
  Subscribes to retail-transactions
  Transformer: adds revenue, is_high_value, hour_of_day fields
  Anomaly Detector: flags missing fields, negative amounts, outliers
  Writes all events to DuckDB
        |
        v
[ DuckDB ]
  Tables: raw_events, anomalies
  Lightweight embedded analytics database
        |
        v
[ Streamlit Dashboard ]
  Auto-refreshes every 5 seconds
  Shows: Total Revenue, Events, High Value Transactions, Anomalies
  Charts: Revenue by Category, Top Stores, Events by Hour
  Table: Recent Transactions
```

## Key Design Decisions
- Synthetic data generator gives full control over event schema and anomaly injection
- DuckDB chosen for zero-config embedded analytics storage
- Streamlit chosen for rapid dashboard development without a separate backend
- Consumer group ID versioned to allow offset reset during development

## Limitations
- Single consumer instance (no horizontal scaling in this demo)
- DuckDB is not suited for concurrent high-volume writes in production
- No schema registry used (JSON serialization only)
- Dashboard reads directly from DuckDB file (not a live API)

## Production Improvements
- Add Avro schema registry for schema enforcement
- Replace DuckDB with PostgreSQL or Redshift for production storage
- Add Kafka consumer lag monitoring
- Add alerting for anomaly spikes
- Deploy consumer as a containerized service
- Add dbt models on top of DuckDB for a proper transformation layer