use {
    std::{time::Duration, sync::Arc},
    tracing::info,
    rdkafka::{producer::{FutureProducer, FutureRecord}},
    bigdata_chess_core::{
        config::UpdateCheckerStepConfig,
        lichess::Lichess,
        queue::Queue,
    },
};

#[allow(dead_code)] // used from other crate
pub async fn update_checker_step(config: &UpdateCheckerStepConfig, kafka_endpoint: String, lichess: Lichess, queue: Arc<Queue>) -> std::io::Result<()> {
    if !config.enabled {
        return Ok(());
    }
    
    let producer = queue.producer();

    loop {
        info!("fetching files list from lichess");
        let lichess_data_files = enforce_lichess_download_limit(&lichess.files_from_lichess_database_list().await);
        info!("lichess data files: {:?}", lichess_data_files);

        for file in lichess_data_files {
            producer.send(FutureRecord::to("chess-lichess-data-files").payload(&file).key(&file), Duration::from_secs(0)).await.unwrap();
        }

        info!("sleeping before checking for updates again");
        tokio::time::sleep(Duration::from_secs(60 * 60)).await;
    }
}
fn enforce_lichess_download_limit(files: &Vec<String>) -> Vec<String> {
    files.iter()
        .take(1)
        .cloned()
        .collect()
}
