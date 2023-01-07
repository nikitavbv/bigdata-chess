use {
    std::{time::Duration, sync::Arc},
    tracing::info,
    rdkafka::{
        config::ClientConfig,
        consumer::{StreamConsumer, CommitMode, Consumer},
        producer::{FutureProducer, FutureRecord},
        Message,
    },
    url::Url,
    futures_util::StreamExt,
    bigdata_chess_core::{
        queue::{TOPIC_LICHESS_DATA_FILES_SYNCED, SyncedFileMessage, StreamingContext},
        storage::Storage,
    },
};

#[allow(dead_code)] // used from other crate
pub async fn file_downloader_step(storage: Arc<Storage>, bootstrap_servers: String) -> std::io::Result<()> {
    info!("running file downloader step");
    
    let consumer: StreamConsumer<StreamingContext> = ClientConfig::new()
        .set("group.id", "bigdata-chess-file-downloader")
        .set("bootstrap.servers", &bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "beginning")
        .set("max.poll.interval.ms", "3000000") // downloading file takes a lot of time
        .create_with_context(StreamingContext)
        .unwrap();
    
    consumer.subscribe(&vec!["chess-lichess-data-files"]).unwrap();
    
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();

    loop {
        let m = consumer.recv().await.unwrap();
        let payload = m.payload_view::<str>().unwrap().unwrap();

        let path = path_from_url(payload);
        let object_storage_path = path.replace(".pgn.zst", "");

        info!("file is: {} and path {}", payload, path);

        if storage.is_lichess_data_file_metadata_present(object_storage_path.clone()).await {
            info!("file already downloaded or being downloaded");
            continue;
        }

        info!("file has not been downloaded yet");

        let data = reqwest::get(payload)
            .await
            .unwrap();

        let chunk_target_size = 100 * 1024 * 1024;
        let expected_chunks = ((data.content_length().unwrap() as f64) / (chunk_target_size as f64)).ceil() as u64;
        storage.put_lichess_data_file_metadata(object_storage_path.clone(), expected_chunks).await;

        let mut stream = data.bytes_stream();

        let mut chunk = Vec::new();
        let mut chunk_index = 0;

        loop {
            let next = match stream.next().await {
                Some(v) => v.unwrap(),
                None => {
                    if !storage.is_lichess_data_file_chunk_present(&object_storage_path, chunk_index).await {
                        storage.upload_lichess_data_file_chunk(object_storage_path.clone(), chunk_index, &chunk).await;
                    }
                    chunk.clear();
                    chunk_index += 1;
                    break;
                }
            };
            chunk.append(&mut next.to_vec());

            if chunk.len() > chunk_target_size {
                if !storage.is_lichess_data_file_chunk_present(&object_storage_path, chunk_index).await {
                    storage.upload_lichess_data_file_chunk(object_storage_path.clone(), chunk_index, &chunk).await;
                }
                chunk.clear();
                chunk_index += 1;
                info!("writing chunks: {}/{}", chunk_index, expected_chunks);
            }
        }

        info!("finished downloading {}, total chunks: {}", path, chunk_index);

        producer.send(
            FutureRecord::to(TOPIC_LICHESS_DATA_FILES_SYNCED)
                .payload(&serde_json::to_vec(&SyncedFileMessage::new(object_storage_path.clone(), chunk_index)).unwrap())
                .key(&object_storage_path), 
            Duration::from_secs(0)
        ).await.unwrap();
        consumer.commit_message(&m, CommitMode::Sync).unwrap();
    }
}

fn path_from_url(url: &str) -> String {
    Url::parse(url).unwrap().path().to_string()
}
