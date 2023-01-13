use rdkafka::producer::Producer;

use {
    std::time::Duration,
    serde::{Serialize, Deserialize},
    prost::Message,
    rand::Rng,
    rdkafka::{
        config::ClientConfig,
        client::ClientContext,
        producer::{FutureProducer, FutureRecord},
        consumer::{ConsumerContext, StreamConsumer, Consumer},
    },
    crate::config::QueueConfig,
};

pub const TOPIC_LICHESS_DATA_FILES_SYNCED: &str = "chess-lichess-data-files-synced";
pub const TOPIC_LICHESS_RAW_GAMES: &str = "chess-lichess-raw-games";
pub const TOPIC_CHESS_GAMES: &str = "chess-games";
pub const TOPIC_CHESS_GAME_PARSER_ERRORS: &str = "chess-game-parser-errors";
pub const TOPIC_CHESS_LOGS: &str = "chess-logs";

pub struct Queue {
    kafka_endpoint: String,
    producer: FutureProducer,
}

pub struct StreamingContext;

impl ClientContext for StreamingContext {}

impl ConsumerContext for StreamingContext {}

#[derive(Serialize, Deserialize)]
pub struct SyncedFileMessage {
    path: String,
    total_chunks: u64,
}

impl Queue {
    pub fn new(config: &QueueConfig) -> Self {
        Self {
            kafka_endpoint: config.endpoint.clone(),
            producer: producer(&config.endpoint),
        }
    }

    pub fn kafka_endpoint(&self) -> &str {
        &self.kafka_endpoint
    }

    pub fn producer(&self) -> FutureProducer {
        producer(&self.kafka_endpoint)
    }

    pub fn consumer(&self, group_id: &str) -> StreamConsumer<StreamingContext> {
        ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", self.kafka_endpoint())
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "beginning")
            .create_with_context(StreamingContext)
            .unwrap()
    }

    pub fn transactional_producer(&self, transactional_id: &str) -> FutureProducer {
        transactional_producer(&self.kafka_endpoint, transactional_id)
    }

    pub fn consumer_for_topic(&self, group_id: &str, topic: &str) -> StreamConsumer<StreamingContext> {
        let consumer = self.consumer(group_id);
        consumer.subscribe(&vec![topic]).unwrap();
        consumer
    }

    pub async fn send_message(&self, message: FutureRecord<'_, Vec<u8>, Vec<u8>>) {
        self.producer.send(message, Duration::from_secs(32)).await.unwrap();
    }

    pub async fn send_game_parser_error(&self, error: String) {
        self.send_message(
            FutureRecord::to(TOPIC_CHESS_GAME_PARSER_ERRORS)
                .payload(&error.encode_to_vec())
                .key(&random_key())
            ).await;
    }

    pub async fn send_log_message(&self, message: Vec<u8>) {
        self.send_message(
            FutureRecord::to(TOPIC_CHESS_LOGS)
                .payload(&message)
                .key(&random_key())
            ).await;
    }
}

impl SyncedFileMessage {
    pub fn new(path: String, total_chunks: u64) -> Self {
        Self {
            path,
            total_chunks,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn total_chunks(&self) -> u64 {
        self.total_chunks
    }
}

fn producer(endpoint: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", endpoint)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap()
}

fn transactional_producer(endpoint: &str, transactional_id: &str) -> FutureProducer {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", endpoint)
        .set("message.timeout.ms", "5000")
        .set("transactional.id", transactional_id)
        .create()
        .unwrap();

    producer.init_transactions(Duration::from_secs(10)).unwrap();

    producer
}

fn random_key() -> Vec<u8> {
    let mut id = [0u8; 12];
    rand::thread_rng().fill(&mut id);
    id.to_vec()
}