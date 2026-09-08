use crate::Event;
use crate::error::Result;
use std::collections::HashMap;
use std::sync::Arc;

/// In-memory storage for messages
///
/// Represents the bottom of our data flow - data only flows IN
#[derive(Debug)]
pub struct Storage {
    topics: HashMap<String, Vec<Arc<Event>>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    /// Append an event to a topic (data flowing DOWN)
    pub fn append(&mut self, topic: String, event: Event) -> Result<()> {
        self.topics.entry(topic).or_default().push(Arc::new(event));
        Ok(())
    }

    /// Fetch events from a topic (data flowing UP to caller)
    ///
    /// Note: This is the one place where data flows upward, but it's a query
    /// operation, not state mutation. The storage itself doesn't change.
    /// Returns Arc<Event> to avoid cloning message payloads.
    pub fn fetch(
        &self,
        topic: &str,
        from_offset: u64,
        max_events: usize,
    ) -> Result<Vec<Arc<Event>>> {
        let events = self
            .topics
            .get(topic)
            .map(|events| {
                events
                    .iter()
                    .filter(|event| event.offset >= from_offset)
                    .take(max_events)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Ok(events)
    }

    /// Get the latest offset for a topic
    ///
    /// Note: This method is part of the Storage API contract shown in the chapter text
    /// (see StorageBackend trait). While not currently called in the implementation,
    /// it demonstrates the interface that storage backends should provide.
    #[allow(dead_code)]
    pub fn latest_offset(&self, topic: &str) -> u64 {
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
    fn test_storage_new_is_empty() {
        let storage = Storage::new();
        assert_eq!(storage.latest_offset("any.topic"), 0);
        let events = storage.fetch("any.topic", 0, 10).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_storage_append() {
        let mut storage = Storage::new();
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
        let mut storage = Storage::new();
        for i in 0..5 {
            storage
                .append("test.topic".into(), event("test.topic", &format!("Msg {i}"), i))
                .unwrap();
        }

        let events = storage.fetch("test.topic", 0, 10).unwrap();
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].offset, 0);
        assert_eq!(events[4].offset, 4);
    }

    #[test]
    fn test_storage_append_separate_topics() {
        let mut storage = Storage::new();
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
        let mut storage = Storage::new();
        for i in 0..5 {
            storage
                .append("test.topic".into(), event("test.topic", &format!("Msg {i}"), i))
                .unwrap();
        }

        let events = storage.fetch("test.topic", 3, 10).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].offset, 3);
        assert_eq!(events[1].offset, 4);
    }

    #[test]
    fn test_storage_fetch_from_beyond_last() {
        let mut storage = Storage::new();
        storage
            .append("test.topic".into(), event("test.topic", "Only", 0))
            .unwrap();

        let events = storage.fetch("test.topic", 5, 10).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_storage_fetch_max_events() {
        let mut storage = Storage::new();
        for i in 0..5 {
            storage
                .append("test.topic".into(), event("test.topic", &format!("Msg {i}"), i))
                .unwrap();
        }

        let events = storage.fetch("test.topic", 0, 2).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].offset, 0);
        assert_eq!(events[1].offset, 1);
    }

    #[test]
    fn test_storage_fetch_nonexistent_topic() {
        let mut storage = Storage::new();
        storage
            .append("test.topic".into(), event("test.topic", "A", 0))
            .unwrap();

        let events = storage.fetch("missing.topic", 0, 10).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_storage_latest_offset_empty_topic() {
        let storage = Storage::new();
        assert_eq!(storage.latest_offset("empty.topic"), 0);
    }
}
