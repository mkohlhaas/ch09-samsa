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

## Architecture at a glance

```
+------------+      +------------------+      +------------+
|  Producer  |      |       Broker     |      |  Consumer  |
|            |      |                  |      |            |
|  send()    |----->|  Topic offsets   |----->|  poll()    |
|  send_keyed|      |  Storage:        |      |            |
|            |      |  +------------+  |      |            |
+------------+      |  | HashMap    |  |      +------------+
                    |  | topic ->   |  |
                    |  | Vec<Event> |  |
                    |  +------------+  |
                    +------------------+

 Message = { topic, key: Option<String>, value: Vec<u8>, timestamp }
   |
   v
 Event   = { message, offset: u64 }   (offset assigned by broker)
   |
   v
 Consumer reads sequentially via poll()
```

## Partitioning

In Apache Kafka, partitioning is the mechanism used to split a single topic
into multiple smaller, independent logs (called partitions) to enable
horizontal scaling, fault tolerance, and parallel processing.

While a Topic is a logical category or feed name where records are published, a
Partition is the physical underlying commit log where the data is actually
stored across the servers (brokers) in a cluster.

## ⚙️ How Kafka Partitions Work

* **Ordered & Immutable Logs**: Each partition is an append-only, ordered sequence of records. Once a message is written to a partition, it cannot be changed or reordered.
* **Offsets**: Every record within a partition is assigned a unique, sequential ID number called an offset. Offsets are unique only within that specific partition (e.g., Partition 0 and Partition 1 both have their own offset 0).
* **Distributed Storage**: A single topic's partitions are scattered across different machines (brokers) in the Kafka cluster. This prevents any single server from running out of disk space or becoming a hardware bottleneck.

## 🚀 Why Partitioning is Essential

Partitioning provides three core architectural benefits:

## 1. Scalability and High Throughput

If a topic lived on only one server, it would be limited by that server's
network and disk capacity. By splitting the topic into multiple partitions
across multiple servers, Kafka can handle millions of messages per second by
utilizing the aggregate power of the entire cluster.

## 2. Parallel Processing (The Consumer Model)

Partitions are the fundamental unit of parallelism in Kafka. Inside a Kafka
consumer group (a team of apps reading data together), each partition can be
assigned to only one consumer at a time.

* If you have a topic with 4 partitions, you can have up to 4 consumers reading data simultaneously in parallel.
* If you add a 5th consumer to that group, it will sit idle because there are no extra partitions to assign to it.

## 3. Fault Tolerance & Replication

Kafka replicates partitions across multiple brokers. For each partition, one
broker acts as the Leader (handling all reads and writes), while other brokers
act as Followers (passive copiers). If the leader broker crashes, a follower
instantly takes over so your application experiences zero downtime.

## 🗺️ How Data is Routed to Partitions

When an application (Producer) sends a message to Kafka, it decides which
partition to send it to using one of three strategies:

| Strategy | How it Works | Best Used For |
|---|---|---|
| Key-Based Routing | Kafka hashes a specific key provided with the message (e.g., user_id or order_id) and maps it to a partition using hash(key) % total_partitions. | Guaranteed ordering. All messages with the same key will always land in the exact same partition and be read in the exact order they were sent. |
| Round-Robin | If no key is provided, Kafka distributes the messages evenly across all available partitions one by one. | Maximum load balancing. This ensures data is spread completely flat across the cluster. |
| Explicit Partition | The producer manually specifies a target partition number (e.g., "Send this directly to Partition 3"). | Custom routing logic required by highly specialized architectures. |

⚠️ Crucial Rule: Kafka only guarantees strict message ordering within a single
partition, not across the entire topic. If global topic ordering is a strict
requirement for your system, you have to use a single partition (which limits
your processing scale).
