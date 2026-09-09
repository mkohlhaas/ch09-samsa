use crate::error::Result;
use crate::storage::{InMemoryStorage, StorageBackend};
use crate::{Consumer, Event, Message, Producer};
use std::sync::{Arc, Mutex, PoisonError};

/// The central message broker
///
/// Coordinates message flow from producers to consumers while maintaining
/// clear directional boundaries.
///
/// Storage is the single source of truth for offsets: each backend derives the
/// next offset from the events it already holds, so the broker keeps no
/// separate offset bookkeeping.
///
/// Brokers are cheaply cloneable: they wrap their storage in `Arc`, so cloning
/// shares the same underlying storage rather than duplicating it.
#[derive(Clone)]
pub struct Broker {
    storage: Arc<Mutex<Box<dyn StorageBackend>>>,
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

impl Broker {
    /// Create a broker backed by the in-memory storage
    pub fn new() -> Self {
        Self::with_storage(InMemoryStorage::default())
    }

    /// Create a broker backed by a custom storage implementation
    pub fn with_storage<S>(storage: S) -> Self
    where
        S: StorageBackend + 'static,
    {
        Self {
            storage: Arc::new(Mutex::new(Box::new(storage))),
        }
    }

    /// Create a producer bound to this broker
    pub fn producer(&self) -> Producer {
        Producer::new(self)
    }

    /// Create a consumer bound to this broker, starting at the latest offset
    pub fn consumer(&self, topic: impl Into<String>) -> Consumer {
        Consumer::new(self, topic)
    }

    /// Create a consumer bound to this broker, starting at offset 0
    pub fn consumer_from_beginning(&self, topic: impl Into<String>) -> Consumer {
        Consumer::from_beginning(self, topic)
    }

    /// Publish a message (called by producers)
    ///
    /// This represents data flowing DOWN from producer to broker.
    /// The offset is derived from the storage backend while holding the lock,
    /// so offset assignment and append are atomic. Returns the offset assigned
    /// to the message (where it was stored), or an error if storage failed.
    pub fn publish(&self, message: Message) -> Result<u64> {
        // Recover a poisoned lock instead of panicking: a panic in another thread
        // only poisoned the mutex after releasing it, so the guard is still valid.
        let mut storage = self.storage.lock().unwrap_or_else(PoisonError::into_inner);

        let topic = message.topic.clone();
        let current_offset = storage.latest_offset(&topic);
        storage.append(topic, Event::new(message, current_offset))?;

        Ok(current_offset)
    }

    /// Fetch events for a consumer (called by consumers)
    pub fn fetch(
        &self,
        topic: &str,
        from_offset: u64,
        max_events: usize,
    ) -> Result<Vec<Arc<Event>>> {
        let storage = self.storage.lock().unwrap_or_else(PoisonError::into_inner);
        storage.fetch(topic, from_offset, max_events)
    }

    /// Get the latest offset for a topic (called by consumers)
    pub fn latest_offset(&self, topic: &str) -> u64 {
        self.storage
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .latest_offset(topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_creation() {
        let broker = Broker::new();
        assert_eq!(broker.latest_offset("test.topic"), 0);
    }

    #[test]
    fn test_publish_single_message() {
        let broker = Broker::new();
        let message = Message::text("test.topic", "Hello");

        let offset = broker.publish(message).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(broker.latest_offset("test.topic"), 1);
    }

    #[test]
    fn test_publish_multiple_messages() {
        let broker = Broker::new();

        let offset1 = broker
            .publish(Message::text("test.topic", "First"))
            .unwrap();
        let offset2 = broker
            .publish(Message::text("test.topic", "Second"))
            .unwrap();
        let offset3 = broker
            .publish(Message::text("test.topic", "Third"))
            .unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 2);
        assert_eq!(broker.latest_offset("test.topic"), 3);
    }

    #[test]
    fn test_publish_to_different_topics() {
        let broker = Broker::new();

        let offset_a1 = broker
            .publish(Message::text("topic.a", "Message A1"))
            .unwrap();
        let offset_b1 = broker
            .publish(Message::text("topic.b", "Message B1"))
            .unwrap();
        let offset_a2 = broker
            .publish(Message::text("topic.a", "Message A2"))
            .unwrap();

        assert_eq!(offset_a1, 0);
        assert_eq!(offset_b1, 0);
        assert_eq!(offset_a2, 1);

        assert_eq!(broker.latest_offset("topic.a"), 2);
        assert_eq!(broker.latest_offset("topic.b"), 1);
    }

    #[test]
    fn test_fetch_events() {
        let broker = Broker::new();

        broker
            .publish(Message::text("test.topic", "First"))
            .unwrap();
        broker
            .publish(Message::text("test.topic", "Second"))
            .unwrap();
        broker
            .publish(Message::text("test.topic", "Third"))
            .unwrap();

        let events = broker.fetch("test.topic", 0, 10).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].offset, 0);
        assert_eq!(events[1].offset, 1);
        assert_eq!(events[2].offset, 2);
    }

    #[test]
    fn test_fetch_with_offset() {
        let broker = Broker::new();

        broker
            .publish(Message::text("test.topic", "First"))
            .unwrap();
        broker
            .publish(Message::text("test.topic", "Second"))
            .unwrap();
        broker
            .publish(Message::text("test.topic", "Third"))
            .unwrap();

        // Fetch from offset 1
        let events = broker.fetch("test.topic", 1, 10).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].offset, 1);
        assert_eq!(events[1].offset, 2);
    }

    #[test]
    fn test_fetch_with_max_events() {
        let broker = Broker::new();

        broker
            .publish(Message::text("test.topic", "First"))
            .unwrap();
        broker
            .publish(Message::text("test.topic", "Second"))
            .unwrap();
        broker
            .publish(Message::text("test.topic", "Third"))
            .unwrap();

        // Fetch max 2 events
        let events = broker.fetch("test.topic", 0, 2).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_fetch_nonexistent_topic() {
        let broker = Broker::new();
        let events = broker.fetch("nonexistent", 0, 10).unwrap();
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_consumer_from_beginning() {
        let broker = Broker::new();
        broker
            .publish(Message::text("test.topic", "First"))
            .unwrap();
        broker
            .publish(Message::text("test.topic", "Second"))
            .unwrap();

        let mut consumer = broker.consumer_from_beginning("test.topic");
        assert_eq!(consumer.current_offset(), 0);

        let events = consumer.poll_batch(10).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].offset, 0);
        assert_eq!(events[1].offset, 1);
    }

    #[test]
    fn test_default_trait() {
        let broker: Broker = Default::default();
        assert_eq!(broker.latest_offset("any.topic"), 0);
    }
}
