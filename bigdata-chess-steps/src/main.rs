mod file_downloader;
mod hdfs_import;
mod postgres_import;
mod progress;
mod storage_import;
mod update_checker;
mod utils;

use {
    std::sync::Arc,
    tracing::{info, error},
    bigdata_chess_core::{
        config::Config,
        storage::Storage,
    },
    crate::{
        hdfs_import::hdfs_import_step,
        utils::init_logging,
    },
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_logging(None);

    let config = Config::load();
    let storage = Arc::new(Storage::new(&config.infra().storage()));

    hdfs_import_step(config.steps.hdfs_import(), storage).await;

    Ok(())
}