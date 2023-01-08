use {
    std::{
        sync::Arc, 
        io::Read, 
        time::Duration, 
        collections::hash_map::DefaultHasher, 
        hash::Hasher,
        time::Instant,
    },
    tracing::info,
    prost::Message,
    rdkafka::{
        Message as KafkaMessage,
        config::ClientConfig,
        producer::FutureRecord,
        consumer::{StreamConsumer, CommitMode, Consumer},
    },
    bigdata_chess_core::{
        storage::Storage,
        queue::{Queue, StreamingContext, TOPIC_LICHESS_DATA_FILES_SYNCED, SyncedFileMessage, TOPIC_LICHESS_RAW_GAMES},
        data::RawChessGame,
    },
};

pub async fn chunk_splitter_step(storage: Arc<Storage>, queue: Arc<Queue>) -> std::io::Result<()> {
    info!("hello from chunk splitter!");

    let consumer: StreamConsumer<StreamingContext> = ClientConfig::new()
        .set("group.id", "bigdata-chess-chunk-splitter")
        .set("bootstrap.servers", queue.kafka_endpoint())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "beginning")
        .create_with_context(StreamingContext)
        .unwrap();
    consumer.subscribe(&vec![TOPIC_LICHESS_DATA_FILES_SYNCED]).unwrap();

    let producer = queue.producer();

    loop {
        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();
        let payload: SyncedFileMessage = serde_json::from_slice(payload).unwrap();

        info!("processing file {}", payload.path());
        let reader = LichessDataFileChunkReader::new(storage.clone(), payload.path().to_owned(), payload.total_chunks());

        let mut decoder = zstd::Decoder::new(reader).unwrap();
        
        let mut pgn = String::new();
        let mut buf = vec![0; 1024];

        let mut total_games_processed = 0;
        let started_at = Instant::now();
        let mut report_time = Instant::now();

        loop {
            let res = decoder.read(&mut buf).unwrap();
            pgn.push_str(&String::from_utf8_lossy(&buf));

            let mut found_something = true;

            while found_something {
                if let Some(metadata_end) = pgn.find("\n\n") {
                    let metadata = &pgn[0..metadata_end];
                    let after_metadata = &pgn[metadata_end+2..];
    
                    if let Some(moves_end) = after_metadata.find("\n\n") {
                        let moves = &after_metadata[0..moves_end];
    
                        let metadata = metadata.to_owned();
                        let moves = moves.to_owned();
                        pgn.drain(0..metadata_end+2+moves_end+2);
                        
                        let game = RawChessGame {
                            metadata,
                            moves,
                        };

                        let encoded_game = game.encode_to_vec();

                        let mut hasher = DefaultHasher::new();
                        hasher.write(&encoded_game);

                        producer.send(
                            FutureRecord::to(TOPIC_LICHESS_RAW_GAMES)
                                .payload(&encoded_game)
                                .key(&hasher.finish().to_string()),
                            Duration::from_secs(0)
                        ).await.unwrap();

                        total_games_processed += 1;

                        let now = Instant::now();
                        if (now - report_time).as_millis() > 1000 {
                            let seconds_since_start = (now - started_at).as_secs();
                            report_time = now;
                            info!("total games processed: {} (avg. {} games per sec)", total_games_processed, total_games_processed / seconds_since_start);
                        }
                    } else {
                        found_something = false;
                    }
                } else {
                    found_something = false;
                }
            }

            if res == 0 {
                break;
            }
        }

        info!("done processing file {}", payload.path());

        consumer.commit_message(&msg, CommitMode::Sync).unwrap();
    }
}

pub struct LichessDataFileChunkReader {
    storage: Arc<Storage>,
    path: String,
    total_chunks: u64,

    chunk_buffer: Vec<u8>,
    current_chunk: Option<u64>,
}

impl LichessDataFileChunkReader {
    pub fn new(storage: Arc<Storage>, path: String, total_chunks: u64) -> Self {
        Self {
            storage,
            path,
            total_chunks,
            chunk_buffer: Vec::new(),
            current_chunk: None,
        }
    }
}

impl Read for LichessDataFileChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while buf.len() > self.chunk_buffer.len() && self.current_chunk.unwrap_or(0) < self.total_chunks {
            let chunk_to_fetch = self.current_chunk.unwrap_or(0);
            let mut chunk_data = futures::executor::block_on(async {
                self.storage.get_lichess_data_file_chunk(&self.path, chunk_to_fetch).await
            });
            self.chunk_buffer.append(&mut chunk_data);
            self.current_chunk = Some(chunk_to_fetch + 1);
        }

        let bytes_to_return = buf.len().min(self.chunk_buffer.len());
        buf.clone_from_slice(&self.chunk_buffer.drain(0..bytes_to_return).collect::<Vec<u8>>());

        Ok(bytes_to_return)
    }
}