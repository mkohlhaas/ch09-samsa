use crate::Event;
use crate::error::Result;
use std::collections::HashMap;
use std::sync::Arc;

/// Contract for message storage backends.
///
/// The broker depends on this abstraction so the actual storage system
/// (in-memory, disk, database, ...) can be swapped out without touching the
/// broker. Storage sits at the bottom of the data flow - data only flows IN.
/// The `Send + Sync` bounds are required because a backend lives behind an
/// `Arc<Mutex<dyn StorageBackend>>` shared by producers and consumers.
pub trait StorageBackend: Send + Sync {
    /// Append an event to a topic
    fn append(&mut self, topic: String, event: Event) -> Result<()>;

    /// Fetch events from a topic
    fn fetch(&self, topic: &str, from_offset: u64, max_events: usize) -> Result<Vec<Arc<Event>>>;

    /// Get the latest offset for a topic (= the next offset a new event would get)
    fn latest_offset(&self, topic: &str) -> u64;
}

/// Default in-memory storage backend
///
/// The broker treats this as the source of truth. Each topic maps to a
/// Vec<Arc<Event>>, and the event's position in that vec is its offset.
#[derive(Debug, Default)]
pub struct InMemoryStorage {
    topics: HashMap<String, Vec<Arc<Event>>>, // topic -> Events
}

impl StorageBackend for InMemoryStorage {
    /// Append an event to a topic
    fn append(&mut self, topic: String, event: Event) -> Result<()> {
        self.topics.entry(topic).or_default().push(Arc::new(event));
        Ok(())
    }

    /// Fetch events from a topic
    ///
    /// Events are stored in offset order (offset *i* lives at index *i*), so a
    /// direct slice gives O(1) access instead of scanning the whole log. Only
    /// called by the broker (in broker's fetch method).
    fn fetch(&self, topic: &str, from_offset: u64, max_events: usize) -> Result<Vec<Arc<Event>>> {
        let events = self
            .topics
            .get(topic)
            .and_then(|events| events.get(from_offset as usize..))
            .map(|events| events.iter().take(max_events).cloned().collect())
            .unwrap_or_default();

        Ok(events)
    }

    /// Get the latest offset for a topic (= the next index where a new message would be stored)
    fn latest_offset(&self, topic: &str) -> u64 {
        self.topics
            .get(topic)
            .and_then(|events| events.last())
            .map(|event| event.offset + 1)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Message;

    fn event(topic: &str, text: &str, offset: u64) -> Event {
        Event::new(Message::text(topic, text), offset)
    }

    #[test]
    fn test_storage_default_is_empty() {
        let storage = InMemoryStorage::default();
        assert_eq!(storage.latest_offset("any.topic"), 0);
        let events = storage.fetch("any.topic", 0, 10).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_storage_append() {
        let mut storage = InMemoryStorage::default();
        storage
            .append("test.topic".into(), event("test.topic", "First", 0))
            .unwrap();

        let events = storage.fetch("test.topic", 0, 10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].offset, 0);
        assert_eq!(events[0].message.as_text(), Some("First"));
    }

    #[test]
    fn test_storage_append_multiple() {
        let mut storage = InMemoryStorage::default();
        for i in 0..5 {
            storage
                .append(
                    "test.topic".into(),
                    event("test.topic", &format!("Msg {i}"), i),
                )
                .unwrap();
        }

        let events = storage.fetch("test.topic", 0, 10).unwrap();
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].offset, 0);
        assert_eq!(events[4].offset, 4);
    }

    #[test]
    fn test_storage_append_separate_topics() {
        let mut storage = InMemoryStorage::default();
        storage
            .append("topic.a".into(), event("topic.a", "A", 0))
            .unwrap();
        storage
            .append("topic.b".into(), event("topic.b", "B", 0))
            .unwrap();
        storage
            .append("topic.a".into(), event("topic.a", "A2", 1))
            .unwrap();

        let a_events = storage.fetch("topic.a", 0, 10).unwrap();
        let b_events = storage.fetch("topic.b", 0, 10).unwrap();
        assert_eq!(a_events.len(), 2);
        assert_eq!(b_events.len(), 1);
        assert_eq!(storage.latest_offset("topic.a"), 2);
        assert_eq!(storage.latest_offset("topic.b"), 1);
    }

    #[test]
    fn test_storage_fetch_from_offset() {
        let mut storage = InMemoryStorage::default();
        for i in 0..5 {
            storage
                .append(
                    "test.topic".into(),
                    event("test.topic", &format!("Msg {i}"), i),
                )
                .unwrap();
        }

        let events = storage.fetch("test.topic", 3, 10).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].offset, 3);
        assert_eq!(events[1].offset, 4);
    }

    #[test]
    fn test_storage_fetch_from_beyond_last() {
        let mut storage = InMemoryStorage::default();
        storage
            .append("test.topic".into(), event("test.topic", "Only", 0))
            .unwrap();

        let events = storage.fetch("test.topic", 5, 10).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_storage_fetch_max_events() {
        let mut storage = InMemoryStorage::default();
        for i in 0..5 {
            storage
                .append(
                    "test.topic".into(),
                    event("test.topic", &format!("Msg {i}"), i),
                )
                .unwrap();
        }

        let events = storage.fetch("test.topic", 0, 2).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].offset, 0);
        assert_eq!(events[1].offset, 1);
    }

    #[test]
    fn test_storage_fetch_nonexistent_topic() {
        let mut storage = InMemoryStorage::default();
        storage
            .append("test.topic".into(), event("test.topic", "A", 0))
            .unwrap();

        let events = storage.fetch("missing.topic", 0, 10).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_storage_latest_offset_empty_topic() {
        let storage = InMemoryStorage::default();
        assert_eq!(storage.latest_offset("empty.topic"), 0);
    }
}
