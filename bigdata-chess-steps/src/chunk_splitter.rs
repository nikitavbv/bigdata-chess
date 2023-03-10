// current performance: 1230 games/second

use {
    std::{
        sync::Arc, 
        io::Read, 
        collections::{hash_map::DefaultHasher, VecDeque},
        hash::Hasher,
        time::{Instant, Duration},
    },
    tracing::{info, error},
    prost::Message,
    rdkafka::{
        Message as KafkaMessage,
        config::ClientConfig,
        producer::{FutureRecord, Producer},
        consumer::{StreamConsumer, CommitMode, Consumer},
    },
    rand::{Rng, distributions::Alphanumeric},
    bigdata_chess_core::{
        storage::Storage,
        queue::{Queue, StreamingContext, TOPIC_LICHESS_DATA_FILES_SYNCED, SyncedFileMessage, TOPIC_LICHESS_RAW_GAMES},
        data::RawChessGame,
        config::ChunkSplitterStepConfig,
    },
    crate::progress::Progress,
};

pub async fn chunk_splitter_step(config: &ChunkSplitterStepConfig, storage: Arc<Storage>, queue: Arc<Queue>) -> std::io::Result<()> {
    info!("hello from chunk splitter!");

    let consumer: StreamConsumer<StreamingContext> = ClientConfig::new()
        .set("group.id", config.group_id())
        .set("bootstrap.servers", queue.kafka_endpoint())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "beginning")
        .set("max.poll.interval.ms", "3000000")
        .create_with_context(StreamingContext)
        .unwrap();
    consumer.subscribe(&vec![TOPIC_LICHESS_DATA_FILES_SYNCED]).unwrap();

    let producer = queue.transactional_producer(&format!("chunk-splitter-{}", random_transactional_id()));

    let to_topic = config.to_topic().clone();

    let mut progress = Progress::new("skipping".to_owned());

    let mut output_batch = Vec::new();

    loop {
        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();
        let payload: SyncedFileMessage = serde_json::from_slice(payload).unwrap();

        info!("processing file {}", payload.path());
        let reader = LichessDataFileChunkReader::new(storage.clone(), payload.path().to_owned(), payload.total_chunks());
        let data = reader.read().await;

        let mut decoder = zstd::Decoder::new(data).unwrap();
        
        let mut pgn = String::new();
        let mut buf = vec![0; 1024];

        let mut time_total: f64 = 0.0;
        let mut time_io: f64 = 0.0;
        let mut time_decompress: f64 = 0.0;

        let mut games_produced = 0;
        let games_to_skip = storage.get_lichess_data_file_chunk_splitting_state(payload.path().to_owned()).await;
        let mut state_sync_time = Instant::now();

        // let mut message_join_handles = VecDeque::new();

        info!("skipping {} games", games_to_skip);

        loop {
            let started_at = Instant::now();

            let uncompress_started_at = Instant::now();
            let res = decoder.read(&mut buf).unwrap();
            time_decompress += (Instant::now() - uncompress_started_at).as_secs_f64();
            pgn.push_str(&String::from_utf8_lossy(&buf));

            let mut found_something = true;

            if games_produced == games_to_skip {
                info!("skipped {} games", games_to_skip);
                progress = Progress::new("processing games".to_owned());
                time_total = 0.0;
            }

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

                        games_produced += 1;

                        if games_produced > games_to_skip {
                            output_batch.push((hasher.finish().encode_to_vec(), encoded_game));

                            if output_batch.len() >= 16 {
                                let io_started_at = Instant::now();
                                producer.begin_transaction().unwrap();
                                for (key, value) in &output_batch {
                                    producer.send(FutureRecord::to(&to_topic)
                                        .payload(&value)
                                        .key(&key), Duration::from_secs(10))
                                        .await
                                        .unwrap();
                                }
                                producer.commit_transaction(Duration::from_secs(10)).unwrap();
                                output_batch.clear();
                                time_io += (Instant::now() - io_started_at).as_secs_f64();
                            }

                            /*let io_started_at = Instant::now();

                            let queue = queue.clone();
                            let to_topic = to_topic.clone();
                            let message_future = async move {
                                queue.send_message(
                                    FutureRecord::to(&to_topic)
                                        .payload(&encoded_game)
                                        .key(&hasher.finish().encode_to_vec())).await
                            };
                            let task_join_handle = tokio::spawn(message_future);
                            message_join_handles.push_back(task_join_handle);
    
                            while message_join_handles.len() >= 16 {
                                message_join_handles.pop_front().unwrap().await.unwrap();
                            }
    
                            time_io += (Instant::now() - io_started_at).as_secs_f64();*/

                            let now = Instant::now();
                            if (now - state_sync_time).as_secs_f32() > 60.0 {
                                storage.put_lichess_data_file_chunk_splitting_state(payload.path().to_owned(), games_produced).await;
                                state_sync_time = now;
                            }

                            if progress.update() {
                                info!("time_total: {}", time_total.round());
                                info!("time_io: {}", time_io.round());
                            }
                        } else {
                            if progress.update() {
                                info!("time_total: {}", time_total.round());
                                info!("time_decompress: {}", time_decompress.round());
                            }
                        }
                    } else {
                        found_something = false;
                    }
                } else {
                    found_something = false;
                }
            }

            time_total += (Instant::now() - started_at).as_secs_f64();

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

    pub async fn read(self) -> VecDeque<u8> {
        let mut res = VecDeque::new();

        for chunk in 0..self.total_chunks {
            info!("fetching chunk {}/{}", chunk, self.total_chunks);
            let mut chunk = self.storage.get_lichess_data_file_chunk(&self.path, chunk).await.unwrap();
            res.append(&mut chunk.into());
        }

        res
    }
}

fn random_transactional_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect()
}