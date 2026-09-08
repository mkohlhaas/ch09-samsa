/// A message flowing through the Samsa system
///
/// Messages are immutable once created and flow downward:
/// Producer -> Broker -> Consumer
#[derive(Debug, Clone)]
pub struct Message {
    pub topic: String,
    pub key: Option<String>,
    pub value: Vec<u8>,
    pub timestamp: u64,
}

impl Message {
    pub fn new(topic: impl Into<String>, key: Option<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            key,
            value: value.into(),
            timestamp: current_timestamp(),
        }
    }

    /// Create a simple text message
    pub fn text(topic: impl Into<String>, text: impl Into<String>) -> Self {
        Self::new(topic, None, text.into().into_bytes())
    }

    /// Create a message with a key for partitioning
    pub fn keyed(
        topic: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        Self::new(topic, Some(key.into()), value)
    }

    /// Get the message value as a string (if valid UTF-8)
    pub fn as_text(&self) -> Option<&str> {
        std::str::from_utf8(&self.value).ok()
    }
}

/// An event represents a message with metadata added by the broker
///
/// Events flow from broker to consumers and include offset information
#[derive(Debug, Clone)]
pub struct Event {
    pub message: Message,
    pub offset: u64,
}

impl Event {
    pub fn new(message: Message, offset: u64) -> Self {
        Self { message, offset }
    }
}

pub fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_new() {
        let message = Message::new("test.topic", Some("key1".into()), b"value".to_vec());
        assert_eq!(message.topic, "test.topic");
        assert_eq!(message.key.as_deref(), Some("key1"));
        assert_eq!(message.value, b"value");
        assert!(message.timestamp > 0);
    }

    #[test]
    fn test_message_new_without_key() {
        let message: Message = Message::new("test.topic", None, vec![1, 2, 3]);
        assert_eq!(message.topic, "test.topic");
        assert!(message.key.is_none());
        assert_eq!(message.value, vec![1, 2, 3]);
    }

    #[test]
    fn test_message_text() {
        let message = Message::text("test.topic", "hello world");
        assert_eq!(message.topic, "test.topic");
        assert!(message.key.is_none());
        assert_eq!(message.as_text(), Some("hello world"));
    }

    #[test]
    fn test_message_keyed() {
        let message = Message::keyed("test.topic", "user-42", b"payload".to_vec());
        assert_eq!(message.topic, "test.topic");
        assert_eq!(message.key.as_deref(), Some("user-42"));
        assert_eq!(message.value, b"payload");
    }

    #[test]
    fn test_message_as_text_valid_utf8() {
        let message = Message::text("test.topic", "some text");
        assert_eq!(message.as_text(), Some("some text"));
    }

    #[test]
    fn test_message_as_text_invalid_utf8() {
        let message = Message::new("test.topic", None, vec![0xff, 0xfe, 0x80]);
        assert_eq!(message.as_text(), None);
    }

    #[test]
    fn test_message_as_text_empty() {
        let message: Message = Message::new("test.topic", None, b"");
        assert_eq!(message.as_text(), Some(""));
    }

    #[test]
    fn test_message_clone() {
        let message = Message::keyed("test.topic", "key", b"value".to_vec());
        let clone = message.clone();
        assert_eq!(clone.topic, message.topic);
        assert_eq!(clone.key, message.key);
        assert_eq!(clone.value, message.value);
        assert_eq!(clone.timestamp, message.timestamp);
    }

    #[test]
    fn test_message_accepts_owned_and_borrowed() {
        let from_str = Message::text("t", "hello");
        assert_eq!(from_str.topic, "t");
        let from_string = Message::text(String::from("t2"), String::from("hi"));
        assert_eq!(from_string.topic, "t2");
    }

    #[test]
    fn test_event_new() {
        let message = Message::text("test.topic", "event message");
        let event = Event::new(message.clone(), 42);
        assert_eq!(event.offset, 42);
        assert_eq!(event.message.topic, "test.topic");
        assert_eq!(event.message.as_text(), Some("event message"));
    }

    #[test]
    fn test_event_clone() {
        let message = Message::text("test.topic", "event");
        let event = Event::new(message, 7);
        let clone = event.clone();
        assert_eq!(clone.offset, 7);
        assert_eq!(clone.message.as_text(), Some("event"));
    }

    #[test]
    fn test_current_timestamp_nonzero() {
        assert!(current_timestamp() > 0);
    }
}
