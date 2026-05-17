# 12 - Phase 2: Real-time Streaming (Kafka) + Big Data Transform (Spark)

> Extension của hệ thống ETL Kimball sang kiến trúc Lambda/Kappa.
> Đọc file này trước khi implement bất kỳ thứ gì liên quan đến Kafka hoặc Spark.

## 12.1 Mục tiêu phase này

| Mục tiêu | Công nghệ | Thay thế / Bổ sung |
|---|---|---|
| Real-time order event ingestion | Apache Kafka | Bổ sung song song với batch extract |
| Scalable Transform layer | Apache Spark (PySpark) | Thay Pandas ở clean + deliver phase |
| Unified orchestration | Airflow (giữ nguyên) | Thêm KafkaConsumerOperator, SparkSubmitOperator |

## 12.2 Kiến trúc tổng thể sau phase 2

```
[Source Systems]
    ├── Northwind CSV  ──────────────────────▶ [Batch Extract] ──▶ data/raw/
    └── Simulated Order Events (Python) ──▶ [Kafka Producer]
                                                    │
                                               [Kafka Topic]
                                           orders_raw / customers_raw
                                                    │
                                            [Kafka Consumer]
                                                    │
                                                    ▼
                                            data/raw/streaming/
                                                    │
                              ┌─────────────────────┴──────────────────────┐
                              ▼                                             ▼
                     [Spark Clean Job]                          [Spark Conform Job]
                     (thay Pandas clean)                        (thay Pandas conform)
                              │                                             │
                              └─────────────────────┬──────────────────────┘
                                                    ▼
                                         [Spark Deliver Job]
                                         (SCD + SK Pipeline)
                                                    │
                                                    ▼
                                            PostgreSQL Warehouse
```

## 12.3 Docker Compose — Services cần thêm

Thêm vào `docker-compose.yml`:

```yaml
  # ── Zookeeper (required by Kafka) ──────────────────────────
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    container_name: etl_zookeeper
    restart: unless-stopped
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks: [etl_net]

  # ── Kafka Broker ────────────────────────────────────────────
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: etl_kafka
    restart: unless-stopped
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    networks: [etl_net]
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:29092", "--list"]
      interval: 10s
      timeout: 10s
      retries: 5

  # ── Kafka UI (lightweight monitor) ──────────────────────────
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: etl_kafka_ui
    restart: unless-stopped
    ports:
      - "8081:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: etl-cluster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on: [kafka]
    networks: [etl_net]

  # ── Spark Master ─────────────────────────────────────────────
  spark-master:
    image: bitnami/spark:3.5
    container_name: etl_spark_master
    restart: unless-stopped
    environment:
      SPARK_MODE: master
      SPARK_RPC_AUTHENTICATION_ENABLED: "no"
      SPARK_RPC_ENCRYPTION_ENABLED: "no"
    ports:
      - "8082:8080"   # Spark UI
      - "7077:7077"   # Spark master port
    volumes:
      - ./src:/opt/etl/src
      - ./data:/opt/etl/data
      - ./config:/opt/etl/config
    networks: [etl_net]

  # ── Spark Worker ─────────────────────────────────────────────
  spark-worker:
    image: bitnami/spark:3.5
    container_name: etl_spark_worker
    restart: unless-stopped
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 2
    volumes:
      - ./src:/opt/etl/src
      - ./data:/opt/etl/data
    depends_on: [spark-master]
    networks: [etl_net]
```

**Ports sau phase 2:**

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Kafka UI | http://localhost:8081 |
| Spark UI | http://localhost:8082 |
| Kafka broker (external) | localhost:9092 |

## 12.4 Kafka Topics

| Topic | Producer | Consumer | Format |
|---|---|---|---|
| `orders_raw` | `src/streaming/order_producer.py` | `src/streaming/order_consumer.py` | JSON |
| `customers_raw` | `src/streaming/customer_producer.py` | `src/streaming/customer_consumer.py` | JSON |
| `etl_errors` | Mọi module khi có FATAL error | Monitoring | JSON |

## 12.5 Module structure mới (`src/streaming/`)

```
src/streaming/
├── __init__.py
├── producers/
│   ├── base_producer.py          # BaseKafkaProducer abstract
│   ├── order_producer.py         # Simulate order events từ Northwind data
│   └── customer_producer.py     # Simulate customer change events
├── consumers/
│   ├── base_consumer.py          # BaseKafkaConsumer abstract
│   ├── order_consumer.py         # Consume → data/raw/streaming/orders/
│   └── customer_consumer.py     # Consume → data/raw/streaming/customers/
└── schemas/
    ├── order_event.py            # Pydantic schema cho order event
    └── customer_event.py        # Pydantic schema cho customer event
```

## 12.6 Kafka Producer spec

```python
class OrderProducer(BaseKafkaProducer):
    """
    Đọc orders từ data/seed/northwind/orders.csv,
    publish từng row lên Kafka topic 'orders_raw' dưới dạng JSON.
    Simulate delay ngẫu nhiên 0.1-0.5s giữa các message (real-time feel).
    """
    topic = "orders_raw"
    
    def produce_from_seed(self, delay_range=(0.1, 0.5)) -> int:
        """
        Returns: số message đã publish
        """
```

**Message schema (JSON):**
```json
{
  "event_id": "uuid-v4",
  "event_timestamp": "2024-06-25T10:30:15.123Z",
  "event_type": "ORDER_CREATED",
  "payload": {
    "OrderID": 10248,
    "CustomerID": "VINET",
    "EmployeeID": 5,
    "OrderDate": "1996-07-04",
    "Freight": 32.38
  },
  "source": "northwind_simulator"
}
```

## 12.7 Kafka Consumer spec

```python
class OrderConsumer(BaseKafkaConsumer):
    """
    Consume từ 'orders_raw', buffer 100 messages hoặc 30 giây,
    flush xuống data/raw/streaming/orders/YYYY-MM-DD-HHMMSS.parquet
    """
    topic = "orders_raw"
    batch_size = 100
    flush_interval_sec = 30
    
    def consume_to_staging(self, output_dir: Path) -> int:
        """
        Returns: số records flushed
        """
```

## 12.8 Spark Jobs — thay thế Pandas

### Spark Clean Job

```
src/spark_jobs/
├── clean_job.py          # Thay src/clean/pipeline.py
├── conform_job.py        # Thay src/conform/pipeline.py
├── deliver_job.py        # Thay src/deliver/pipeline.py
└── aggregate_job.py      # Thay src/deliver/aggregate_builder.py
```

**clean_job.py spec:**

```python
"""
Spark job thay thế Pandas clean pipeline.
Submit: spark-submit --master spark://spark-master:7077 src/spark_jobs/clean_job.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_clean_job(batch_id: str, raw_dir: str, staging_dir: str):
    spark = SparkSession.builder \
        .appName(f"ETL-Clean-{batch_id}") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()
    
    # Đọc từ raw (batch hoặc streaming parquet)
    customers = spark.read.csv(f"{raw_dir}/customers.csv", header=True)
    
    # Apply screens tương tự Pandas version nhưng dùng Spark SQL / DataFrame API
    # Column Nullity
    customers = customers.filter(F.col("CustomerID").isNotNull())
    
    # Ghi error events vào PostgreSQL staging.error_events
    # Ghi cleaned data ra staging_dir
    customers.write.parquet(f"{staging_dir}/cleaned_customers", mode="overwrite")
    
    spark.stop()
```

### Surrogate Key Pipeline trên Spark

Thách thức: SK generation không thể dùng auto-increment khi Spark chạy distributed.

**Giải pháp**: dùng `zipWithIndex()` + offset từ PostgreSQL:

```python
def generate_surrogate_keys(df, dim_name: str, pg_conn) -> DataFrame:
    """
    1. Query max(SK) hiện tại từ PostgreSQL
    2. df.rdd.zipWithIndex() để gán SK = max + index + 1
    3. Convert lại thành DataFrame
    """
```

## 12.9 Airflow DAGs mới

```
dags/
├── dag_batch_etl.py           # DAG hiện tại (giữ nguyên)
├── dag_kafka_producer.py      # Trigger producer, chạy theo schedule
├── dag_streaming_consumer.py  # Monitor consumer, flush khi đủ batch
└── dag_spark_transform.py     # SparkSubmitOperator chạy clean/conform/deliver
```

**dag_spark_transform.py:**

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_clean = SparkSubmitOperator(
    task_id="spark_clean",
    application="/opt/etl/src/spark_jobs/clean_job.py",
    conn_id="spark_default",           # Config trong Airflow Connections
    application_args=["--batch-id", "{{ run_id }}"],
    dag=dag,
)
```

## 12.10 Dependencies bổ sung (`requirements.txt`)

```
# Kafka
confluent-kafka==2.4.0
kafka-python==2.0.2

# Spark (client only — Spark server chạy trong Docker)
pyspark==3.5.1

# Airflow providers
apache-airflow-providers-apache-spark==4.7.1
apache-airflow-providers-apache-kafka==1.3.1
```

## 12.11 Thứ tự implement (cho Claude Code)

### Sprint 1: Kafka infrastructure
1. Update `docker-compose.yml` thêm Kafka + Zookeeper + Kafka UI
2. Implement `BaseKafkaProducer` + `OrderProducer`
3. Implement `BaseKafkaConsumer` + `OrderConsumer`
4. Test: producer → topic → consumer → parquet file
5. DAG `dag_kafka_producer.py`

### Sprint 2: Spark Transform
1. Update `docker-compose.yml` thêm Spark Master + Worker
2. Implement `clean_job.py` (Spark version của clean phase)
3. Implement `conform_job.py`
4. Implement `deliver_job.py` với SK Pipeline trên Spark
5. DAG `dag_spark_transform.py`

### Sprint 3: Integration
1. E2E test: Kafka events → Consumer → Spark Clean → Spark Deliver → PostgreSQL
2. So sánh kết quả với batch pipeline (phải giống nhau)
3. Update `CLAUDE.md` với tech stack mới

## 12.12 Prompt templates cho Claude Code

### Prompt 1: Thêm Kafka vào Docker stack

```
Đọc docs/12-phase2-kafka-spark.md mục 12.3 (Docker services).
Thêm Zookeeper, Kafka, Kafka UI vào docker-compose.yml hiện tại.
Sau đó chạy: docker compose up -d zookeeper kafka kafka-ui
Verify: curl http://localhost:8081 trả về Kafka UI.
KHÔNG động vào các service postgres/airflow.
```

### Prompt 2: Implement Kafka Producer

```
Đọc docs/12-phase2-kafka-spark.md mục 12.5 và 12.6.
Implement:
  - src/streaming/producers/base_producer.py
  - src/streaming/producers/order_producer.py
  - src/streaming/schemas/order_event.py (Pydantic)
Test: chạy producer với 10 messages, verify trong Kafka UI topic 'orders_raw'.
Kèm pytest với mock Kafka.
```

### Prompt 3: Implement Spark Clean Job

```
Đọc docs/12-phase2-kafka-spark.md mục 12.8.
Đọc docs/06-clean-phase.md (logic hiện tại bằng Pandas).
Implement src/spark_jobs/clean_job.py — port toàn bộ logic 
từ src/clean/pipeline.py sang PySpark DataFrame API.
Test locally: spark-submit --master local[2] src/spark_jobs/clean_job.py
Output phải giống hệt Pandas version khi chạy với cùng input.
```

## 12.13 Checklist hoàn thành phase 2

- [ ] Kafka UI accessible tại http://localhost:8081
- [ ] Spark UI accessible tại http://localhost:8082
- [ ] Producer publish được 830 order records lên topic `orders_raw`
- [ ] Consumer flush được parquet file vào `data/raw/streaming/`
- [ ] Spark clean job chạy không lỗi, output row count khớp Pandas version
- [ ] Spark deliver job load được fact_sales vào PostgreSQL
- [ ] DAG `dag_spark_transform` chạy được trong Airflow
- [ ] CV bullet points có thể giải thích cụ thể khi interview