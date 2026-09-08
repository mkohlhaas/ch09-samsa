//! Samsa - A simple publish/subscribe microservice
//!
//! This crate demonstrates architectural patterns through building
//! a working message broker system.

// Public API - what users can access
pub use broker::Broker;
pub use consumer::Consumer;
pub use error::{Result, SamsaError};
pub use message::{Event, Message};
pub use producer::Producer;

// Private modules - implementation hidden
mod broker;
mod consumer;
mod error;
mod message;
mod producer;
mod storage;
