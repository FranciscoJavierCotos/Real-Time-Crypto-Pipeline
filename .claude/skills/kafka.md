---
name: kafka
description: "Use this skill for any real-time data pipeline work involving Apache Kafka. Triggers: designing producers/consumers, topic configuration, partition strategy, offset management, schema design, error handling in streaming pipelines, consumer group setup, Kafka Streams, or any mention of 'event streaming', 'message queue', 'Kafka', or 'real-time pipeline'. Do NOT use for batch ETL jobs, non-Kafka message brokers (RabbitMQ, SQS), or static data processing."
---

# Kafka — Real-Time Data Pipeline Skill

## Non-Negotiables (Always Apply These)

### 1. Never Auto-Create Topics in Production

Always provision topics explicitly with controlled config. Auto-creation hides misconfigured replication and retention.

```bash
kafka-topics.sh --create \
  --topic events.user-actions \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \   # 7 days
  --config min.insync.replicas=2 \
  --bootstrap-server broker:9092
```

### 2. Use Explicit Serialization — Never Raw Strings in Production

Always pair with a Schema Registry. Use Avro or Protobuf.

```python
# BAD — schema drift will silently corrupt consumers
producer.send("events", value=json.dumps(event).encode())

# GOOD — schema enforced at write time
from confluent_kafka.avro import AvroProducer
producer = AvroProducer(conf, default_value_schema=schema)
producer.produce(topic="events.user-actions", value=event)
```

### 3. Always Set `acks=all` for Critical Pipelines

```python
producer_config = {
    "bootstrap.servers": "broker:9092",
    "acks": "all",                  # wait for all ISR replicas
    "enable.idempotence": True,     # exactly-once delivery
    "retries": 5,
    "retry.backoff.ms": 300,
    "compression.type": "lz4",      # always compress
}
```

### 4. Consumer — Always Commit After Processing, Not Before

```python
# BAD — message lost if processing fails after commit
consumer.commit()
process(msg)

# GOOD — at-least-once guarantee
process(msg)
consumer.commit(asynchronous=False)  # sync commit for critical paths
```

### 5. Dead Letter Queue (DLQ) — Mandatory for Every Consumer

Never silently drop or infinite-retry a poison pill message.

```python
def safe_consume(consumer, dlq_producer, topic):
    msg = consumer.poll(1.0)
    if msg is None:
        return
    try:
        process(msg)
        consumer.commit()
    except Exception as e:
        dlq_producer.produce(
            topic=f"{topic}.dlq",
            value=msg.value(),
            headers={"error": str(e), "origin-topic": topic}
        )
        dlq_producer.flush()
        consumer.commit()  # move past the bad message
```

---

## Topic Naming Convention

```
{domain}.{entity}.{event-type}

Examples:
  payments.transactions.initiated
  payments.transactions.completed
  auth.sessions.created
  inventory.products.updated
```

Never use generic names like `events` or `data` — they become unmanageable.

---

## Partition Strategy

| Traffic Pattern           | Partition Key        | Rationale                            |
| ------------------------- | -------------------- | ------------------------------------ |
| Per-user ordering         | `user_id`            | All user events go to same partition |
| Per-entity                | `entity_id`          | Preserves order per resource         |
| High-throughput, no order | `null` (round-robin) | Max parallelism                      |
| Geo-distributed           | `region_code`        | Locality-aware routing               |

**Rule:** Number of partitions = expected peak throughput (MB/s) ÷ 10MB/s per partition. Round up to nearest power of 2. You can increase partitions later but it breaks key-based ordering — plan ahead.

---

## Consumer Group Design

```python
consumer_config = {
    "bootstrap.servers": "broker:9092",
    "group.id": "payments-service-v2",     # version your group IDs
    "auto.offset.reset": "earliest",       # never use 'latest' in prod
    "enable.auto.commit": False,           # always manual commit
    "max.poll.interval.ms": 300000,        # must exceed your processing time
    "session.timeout.ms": 45000,
}
```

**One consumer group per logical workload.** Don't share consumer groups between services — it makes rebalancing unpredictable and breaks independent scaling.

---

## Offset Management

```python
# On startup — always log where you're resuming from
partitions = consumer.assignment()
for p in partitions:
    committed = consumer.committed([p])
    print(f"Resuming {p.topic}[{p.partition}] from offset {committed[0].offset}")
```

For exactly-once semantics (EOS), use Kafka transactions:

```python
producer.init_transactions()
try:
    producer.begin_transaction()
    producer.produce(output_topic, value=result)
    producer.send_offsets_to_transaction(offsets, consumer_group_metadata)
    producer.commit_transaction()
except Exception:
    producer.abort_transaction()
```

---

## Monitoring — Metrics You Must Track

| Metric                        | Alert Threshold | Meaning                    |
| ----------------------------- | --------------- | -------------------------- |
| `consumer_lag`                | > 10k messages  | Consumer falling behind    |
| `under_replicated_partitions` | > 0             | Data durability at risk    |
| `request_latency_avg`         | > 500ms         | Broker overloaded          |
| `produce_error_rate`          | > 0.1%          | Data loss happening        |
| `rebalance_rate`              | Frequent        | Consumer group instability |

Export via JMX → Prometheus → Grafana. Never run blind.

---

## Common Pitfalls

| Mistake                                      | Fix                                                          |
| -------------------------------------------- | ------------------------------------------------------------ |
| `auto.offset.reset=latest` in production     | Use `earliest`; you'll miss messages on new consumers        |
| Reusing consumer group IDs across envs       | Namespace by env: `prod-payments`, `staging-payments`        |
| Unbounded retry loops                        | Exponential backoff + DLQ after N attempts                   |
| One partition for ordering + high throughput | Use a compacted topic or rethink the ordering requirement    |
| Not setting `max.poll.records`               | Default 500 can overwhelm slow processors — tune to workload |

---

## Quick Setup Checklist

- [ ] Topics created explicitly (not auto-created)
- [ ] Replication factor ≥ 3, `min.insync.replicas=2`
- [ ] Schema Registry configured, Avro/Protobuf schemas registered
- [ ] `acks=all` + `enable.idempotence=true` on producer
- [ ] `enable.auto.commit=false` on consumer
- [ ] DLQ topic exists for every consumer
- [ ] Consumer lag alerting configured
- [ ] `under_replicated_partitions` alert set to 0 threshold
- [ ] Group IDs versioned and namespaced per environment
