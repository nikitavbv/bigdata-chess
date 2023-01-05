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
        database::Database,
        storage::Storage,
    },
    crate::{
        postgres_import::postgres_import_step,
        storage_import::storage_import_step,
        utils::init_logging,
    },
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_logging();

    let config = Config::load();
    let queue = Arc::new(Queue::new(&config.infra().queue()));
    let database = Arc::new(Database::new(&config.infra().database()).await);
    let storage = Arc::new(Storage::new(&config.infra().storage()));

    let res = storage.remote_list_game_data_files().await.unwrap();
    let res = storage.remote_game_data_file(&res[0]).await.unwrap();

    info!("res is: {:?}", String::from_utf8(res).unwrap());

    // postgres_import_step(queue, database).await;
    // storage_import_step(queue, storage).await;

    Ok(())
}