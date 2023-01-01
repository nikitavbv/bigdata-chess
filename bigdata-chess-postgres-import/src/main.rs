mod step;
mod utils;

use {
    std::sync::Arc,
    tracing::info,
    bigdata_chess_core::{
        config::Config,
        queue::Queue,
        database::Database,
    },
    crate::{
        step::postgres_import_step,
        utils::init_logging,
    },
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_logging();

    let config = Config::load();
    let queue = Arc::new(Queue::new(&config.infra().queue()));
    let database = Arc::new(Database::new(&config.infra().database()).await);

    postgres_import_step(queue, database).await;

    Ok(())
}