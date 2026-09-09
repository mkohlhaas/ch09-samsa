use samsa::Broker;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting Samsa pub/sub example...");

    // Create the central broker
    let broker = Broker::new();

    // Create a producer that sends data DOWN to the broker
    let producer = broker.producer();

    // Create a consumer that pulls data DOWN from the broker
    let mut consumer = broker.consumer_from_beginning("greetings");

    // Producer sends messages (data flows DOWN)
    println!("Sending messages...");
    producer.send_text("greetings", "Hello, Samsa!")?;
    producer.send_text("greetings", "How are you today?")?;
    producer.send_text("greetings", "Goodbye!")?;

    // Consumer receives messages (data flows DOWN)
    println!("Receiving messages...");
    while let Some(event) = consumer.poll()? {
        if let Some(text) = event.message.as_text() {
            println!("Received: {} (offset: {})", text, event.offset);

            // Break after receiving the goodbye message
            if text == "Goodbye!" {
                break;
            }
        }
    }

    println!("Example completed successfully!");
    Ok(())
}
