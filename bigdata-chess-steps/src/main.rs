mod file_downloader;
mod hdfs_import;
mod postgres_import;
mod progress;
mod storage_import;
mod utils;

use {
    std::sync::Arc,
    tracing::info,
    bigdata_chess_core::{
        config::Config,
        queue::Queue,
        storage::Storage,
    },
    crate::{
        hdfs_import::hdfs_import_step,
        utils::init_logging,
    },
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_logging();

    let config = Config::load();
    let storage = Arc::new(Storage::new(&config.infra().storage()));

    hdfs_import_step(storage).await;

    Ok(())
}