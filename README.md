# Samsa

A simple publish/subscribe (pub/sub) message broker for learning
architectural patterns like unidirectional data flow.

## Running the example

```shell
cargo run --example basic_pubsub
bacon ex -- basic_pubsub
```

## Architecture: pull vs. push

Samsa uses a **pull model**: consumers call `poll()` / `poll_batch()`
(see `src/consumer.rs`) to fetch available events from the broker, rather
than the broker actively pushing events to them.

| Factor | Pull (this crate) | Push |
|---|---|---|
| Throughput / many consumers | Preferred (Kafka-style) | — |
| Low latency (real-time alerts) | — | Preferred |
| Backpressure for slow consumers | Natural (consumer polls less) | Hard (broker must buffer/drop) |
| Broker complexity | Low | High (tracks acks, retries) |
| Varying consumer rates | Preferred | — |
| Small number of consumers | — | Preferred |
| Client simplification | — | Preferred |
| Idle/active load | Polling overhead even when idle | No polling when idle |

**Why pull:** it is how Apache Kafka itself works — the industry standard
for high-throughput brokers. Pull gives consumer-driven backpressure (a
slow consumer simply polls less), keeps the broker simple, and lets each
consumer manage its own offset. Its main downside is latency: a consumer
must actively poll to learn about new events.

**Hybrid option:** many systems push a lightweight *notification*
("new data available") and then let the consumer pull the *payload*. This
gets low latency without losing backpressure.
