use samsa::{Broker, Event, Message, Producer};
use std::sync::Arc;

fn make_broker() -> Broker {
    Broker::new()
}

#[test]
fn test_full_pubsub_lifecycle() {
    let broker = make_broker();
    let producer = Producer::new(&broker);

    let offset1 = producer.send_text("greetings", "Hello").unwrap();
    let offset2 = producer.send_text("greetings", "World").unwrap();
    let offset3 = producer.send_text("greetings", "Rust").unwrap();
    assert_eq!(offset1, 0);
    assert_eq!(offset2, 1);
    assert_eq!(offset3, 2);

    let mut consumer = broker.consumer_from_beginning("greetings");

    let mut received = Vec::new();
    while let Some(event) = consumer.poll().unwrap() {
        received.push(event.message.as_text().unwrap().to_string());
    }

    assert_eq!(received, vec!["Hello", "World", "Rust"]);
}

#[test]
fn test_multiple_producers_share_topic_offsets() {
    let broker = make_broker();
    let producer_a = Producer::new(&broker);
    let producer_b = Producer::new(&broker);

    let offsets_a = (0..3)
        .map(|i| producer_a.send_text("shared", &format!("A{i}")))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let offsets_b = (0..3)
        .map(|i| producer_b.send_text("shared", &format!("B{i}")))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(offsets_a, vec![0, 1, 2]);
    assert_eq!(offsets_b, vec![3, 4, 5]);
    assert_eq!(broker.latest_offset("shared"), 6);
}

#[test]
fn test_multiple_consumers_track_independent_positions() {
    let broker = make_broker();
    let producer = Producer::new(&broker);
    producer.send_text("topic", "one").unwrap();
    producer.send_text("topic", "two").unwrap();
    producer.send_text("topic", "three").unwrap();

    let mut consumer_a = broker.consumer_from_beginning("topic");
    let mut consumer_b = broker.consumer_from_beginning("topic");

    let first_a = consumer_a.poll().unwrap().unwrap();
    assert_eq!(first_a.message.as_text(), Some("one"));

    let first_b = consumer_b.poll().unwrap().unwrap();
    assert_eq!(first_b.message.as_text(), Some("one"));

    assert_eq!(consumer_a.current_offset(), 1);
    assert_eq!(consumer_b.current_offset(), 1);
}

#[test]
fn test_consumer_seek_and_reconsume() {
    let broker = make_broker();
    let producer = Producer::new(&broker);
    producer.send_text("seekable", "first").unwrap();
    producer.send_text("seekable", "second").unwrap();
    producer.send_text("seekable", "third").unwrap();

    let mut consumer = broker.consumer_from_beginning("seekable");

    let first = consumer.poll().unwrap().unwrap();
    assert_eq!(first.message.as_text(), Some("first"));
    assert_eq!(consumer.current_offset(), 1);

    consumer.seek(0);
    assert_eq!(consumer.current_offset(), 0);

    let replayed = consumer.poll_batch(10).unwrap();
    let texts: Vec<Option<&str>> = replayed.iter().map(|e| e.message.as_text()).collect();
    assert_eq!(texts, vec![Some("first"), Some("second"), Some("third")]);
}

#[test]
fn test_consumer_starts_at_latest_by_default() {
    let broker = make_broker();
    let producer = Producer::new(&broker);
    producer.send_text("news", "old").unwrap();

    let mut consumer = broker.consumer("news");
    assert_eq!(consumer.current_offset(), 1);
    assert!(consumer.poll().unwrap().is_none());

    producer.send_text("news", "new").unwrap();
    let event = consumer.poll().unwrap().unwrap();
    assert_eq!(event.message.as_text(), Some("new"));
    assert_eq!(event.offset, 1);
}

#[test]
fn test_producer_send_variants_roundtrip() {
    let broker = make_broker();
    let producer = Producer::new(&broker);

    producer.send(Message::text("misc", "raw")).unwrap();
    producer
        .send_keyed("misc", "key-1", b"keyed bytes")
        .unwrap();

    let mut consumer = broker.consumer_from_beginning("misc");

    let raw = consumer.poll().unwrap().unwrap();
    assert_eq!(raw.message.as_text(), Some("raw"));
    assert!(raw.message.key.is_none());

    let keyed = consumer.poll().unwrap().unwrap();
    assert_eq!(keyed.message.key.as_deref(), Some("key-1"));
    assert_eq!(keyed.message.value, b"keyed bytes");

    assert!(consumer.poll().unwrap().is_none());
}

#[test]
fn test_message_payloads_can_be_binary() {
    let broker = make_broker();
    let producer = Producer::new(&broker);

    let binary: Vec<u8> = (0u8..=255).collect();
    producer
        .send(Message::new("binary", None, binary.clone()))
        .unwrap();

    let mut consumer = broker.consumer_from_beginning("binary");
    let event = consumer.poll().unwrap().unwrap();
    assert_eq!(event.message.value, binary);
    assert_eq!(event.message.as_text(), None);
}

#[test]
fn test_fetch_returns_shared_arcs() {
    let broker = make_broker();
    let producer = Producer::new(&broker);
    producer.send_text("arc", "data").unwrap();

    let events_a: Vec<Arc<Event>> = broker.fetch("arc", 0, 10).unwrap();
    let events_b: Vec<Arc<Event>> = broker.fetch("arc", 0, 10).unwrap();
    assert_eq!(events_a.len(), 1);
    assert_eq!(events_b.len(), 1);

    // Both fetches share the same underlying Arc<Event>, no deep clone of payloads.
    assert!(Arc::ptr_eq(&events_a[0], &events_b[0]));
    assert_eq!(events_a[0].message.as_text(), Some("data"));
}

#[test]
fn test_fetch_nonexistent_topic_is_empty() {
    let broker = make_broker();
    let events = broker.fetch("ghost", 0, 10).unwrap();
    assert!(events.is_empty());
}
