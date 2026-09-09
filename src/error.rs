//! Unified error handling for Samsa
//!
//! This module consolidates all error types into a single SamsaError enum,
//! providing better ergonomics for library users and demonstrating the
//! thiserror crate for idiomatic Rust error handling.

use thiserror::Error;

/// The main error type for all Samsa operations
///
/// This replaces multiple separate error enums with a single unified type,
/// making it easy for users to handle any Samsa error.
#[derive(Error, Debug, Clone)]
pub enum SamsaError {
    /// Configuration-related errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Topic validation errors
    #[error("Topic validation failed: {0}")]
    Topic(String),

    /// Consumer-related errors
    #[error("Consumer error: {0}")]
    Consumer(String),

    /// Consumer group errors
    #[error("Consumer group error: {0}")]
    Group(String),

    /// Resource management errors (pools, guards, etc.)
    #[error("Resource error: {0}")]
    Resource(String),

    /// Service lifecycle errors
    #[error("Service error: {0}")]
    Service(String),

    /// Schema and message validation errors
    #[error("Schema validation failed: {0}")]
    Schema(String),

    /// Message routing and delivery errors
    #[error("Message routing failed: {0}")]
    Routing(String),

    /// Generic broker errors
    #[error("Broker error: {0}")]
    Broker(String),

    /// Connection-related errors
    #[error("Connection failed: {0}")]
    Connection(String),
}

/// Convenience type alias for Results using SamsaError
pub type Result<T> = std::result::Result<T, SamsaError>;

// Convenience constructors for common error patterns
impl SamsaError {
    pub fn config(msg: impl Into<String>) -> Self {
        SamsaError::Config(msg.into())
    }

    pub fn topic(msg: impl Into<String>) -> Self {
        SamsaError::Topic(msg.into())
    }

    pub fn consumer(msg: impl Into<String>) -> Self {
        SamsaError::Consumer(msg.into())
    }

    pub fn resource(msg: impl Into<String>) -> Self {
        SamsaError::Resource(msg.into())
    }

    pub fn service(msg: impl Into<String>) -> Self {
        SamsaError::Service(msg.into())
    }

    pub fn schema(msg: impl Into<String>) -> Self {
        SamsaError::Schema(msg.into())
    }

    pub fn routing(msg: impl Into<String>) -> Self {
        SamsaError::Routing(msg.into())
    }

    pub fn broker(msg: impl Into<String>) -> Self {
        SamsaError::Broker(msg.into())
    }

    pub fn connection(msg: impl Into<String>) -> Self {
        SamsaError::Connection(msg.into())
    }

    pub fn group(msg: impl Into<String>) -> Self {
        SamsaError::Group(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_display() {
        let err = SamsaError::Config("bad setting".into());
        assert_eq!(err.to_string(), "Configuration error: bad setting");
    }

    #[test]
    fn test_topic_display() {
        let err = SamsaError::Topic("invalid name".into());
        assert_eq!(err.to_string(), "Topic validation failed: invalid name");
    }

    #[test]
    fn test_consumer_display() {
        let err = SamsaError::Consumer("offset too large".into());
        assert_eq!(err.to_string(), "Consumer error: offset too large");
    }

    #[test]
    fn test_group_display() {
        let err = SamsaError::Group("not a member".into());
        assert_eq!(err.to_string(), "Consumer group error: not a member");
    }

    #[test]
    fn test_resource_display() {
        let err = SamsaError::Resource("pool exhausted".into());
        assert_eq!(err.to_string(), "Resource error: pool exhausted");
    }

    #[test]
    fn test_service_display() {
        let err = SamsaError::Service("failed to start".into());
        assert_eq!(err.to_string(), "Service error: failed to start");
    }

    #[test]
    fn test_schema_display() {
        let err = SamsaError::Schema("missing field".into());
        assert_eq!(err.to_string(), "Schema validation failed: missing field");
    }

    #[test]
    fn test_routing_display() {
        let err = SamsaError::Routing("no route found".into());
        assert_eq!(err.to_string(), "Message routing failed: no route found");
    }

    #[test]
    fn test_broker_display() {
        let err = SamsaError::Broker("storage unavailable".into());
        assert_eq!(err.to_string(), "Broker error: storage unavailable");
    }

    #[test]
    fn test_connection_display() {
        let err = SamsaError::Connection("refused".into());
        assert_eq!(err.to_string(), "Connection failed: refused");
    }

    #[test]
    fn test_convenience_constructors() {
        assert!(matches!(SamsaError::config("c"), SamsaError::Config(_)));
        assert!(matches!(SamsaError::topic("t"), SamsaError::Topic(_)));
        assert!(matches!(SamsaError::consumer("c"), SamsaError::Consumer(_)));
        assert!(matches!(SamsaError::resource("r"), SamsaError::Resource(_)));
        assert!(matches!(SamsaError::service("s"), SamsaError::Service(_)));
        assert!(matches!(SamsaError::schema("s"), SamsaError::Schema(_)));
        assert!(matches!(
            SamsaError::connection("c"),
            SamsaError::Connection(_)
        ));
    }

    #[test]
    fn test_convenience_constructor_preserves_message() {
        let err = SamsaError::config("preserved");
        match err {
            SamsaError::Config(msg) => assert_eq!(msg, "preserved"),
            _ => panic!("expected Config variant"),
        }
    }

    #[test]
    fn test_error_is_clone() {
        let err = SamsaError::Broker("clonable".into());
        let clone = err.clone();
        assert_eq!(err.to_string(), clone.to_string());
    }

    #[test]
    fn test_error_is_debug() {
        let err = SamsaError::Topic("debuggable".into());
        let debug = format!("{err:?}");
        assert!(debug.contains("Topic"));
    }

    #[test]
    fn test_result_alias() {
        let ok: Result<u64> = Ok(42);
        assert!(ok.is_ok());
        assert_eq!(ok.as_ref().ok().copied(), Some(42));

        let err: Result<u64> = Err(SamsaError::Broker("boom".into()));
        assert!(err.is_err());
    }

    #[test]
    fn test_variants_are_public() {
        // This test ensures all documented variants are constructible from outside
        let variants = vec![
            SamsaError::Config("c".into()),
            SamsaError::Topic("t".into()),
            SamsaError::Consumer("c".into()),
            SamsaError::Group("g".into()),
            SamsaError::Resource("r".into()),
            SamsaError::Service("s".into()),
            SamsaError::Schema("s".into()),
            SamsaError::Routing("r".into()),
            SamsaError::Broker("b".into()),
            SamsaError::Connection("c".into()),
        ];
        assert_eq!(variants.len(), 10);
        for v in variants {
            let _ = v.to_string();
        }
    }
}
