# Real-Time CDC Data Pipeline

Portfolio project demonstrating Change Data Capture with PostgreSQL, Debezium, Kafka and PySpark Structured Streaming.

## Architecture

```text
PostgreSQL OLTP
   INSERT / UPDATE / DELETE
          |
          v
       Debezium
          |
          v
        Kafka
          |
          v
PySpark Structured Streaming
     |          |
     v          v
  Bronze      Bad Records
     |
     v
  Silver CDC Events
     |
     +--> Current Customer State
     `--> Customer Change History
```

## Skills

- PostgreSQL logical replication
- Debezium CDC
- Kafka
- PySpark Structured Streaming
- CDC envelope parsing
- INSERT / UPDATE / DELETE handling
- event-time and Kafka offsets
- deduplication
- current-state upserts using foreachBatch
- historical change tracking
- checkpoints
- data quality
- Docker Compose
- tests and GitHub Actions

## Requirements

- Docker Desktop
- Python 3.11
- Java 17

## 1. Python setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2. Start PostgreSQL, Kafka and Debezium

```powershell
docker compose up -d
```

Wait 20-30 seconds.

## 3. Register the Debezium connector

PowerShell:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType "application/json" `
  -InFile "debezium/customer-connector.json"
```

Check:

```powershell
Invoke-RestMethod http://localhost:8083/connectors
```

Debezium creates a Kafka topic similar to:

```text
cdc.public.customers
```

## 4. Start PySpark CDC consumer

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 src/streaming/process_customer_cdc.py
```

## 5. Generate database changes

In another terminal:

```powershell
python -m src.producers.generate_database_changes
```

This continuously inserts, updates and deletes customers in PostgreSQL.

## Output

```text
data/
  bronze/customer_cdc/
  silver/customer_changes/
  gold/customer_current/
  gold/customer_history/
  bad_records/
  checkpoints/
```

## Inspect output

After events have been processed:

```powershell
python -m src.quality.inspect_outputs
```

## CDC operation codes

Debezium uses:
- `c` = create
- `u` = update
- `d` = delete
- `r` = snapshot/read

The PySpark pipeline converts these to readable operation names.

## Important portfolio note

The local implementation uses Parquet for portability. In a production warehouse/lakehouse,
the current-state `foreachBatch` logic would typically MERGE into Delta/Iceberg/Hudi or a
warehouse such as Snowflake.

Never commit database/cloud credentials to GitHub.
