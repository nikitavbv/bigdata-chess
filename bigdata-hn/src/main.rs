use {
    std::{time::Duration, sync::Arc, collections::VecDeque},
    bigdata_chess_core::{
        config::Config,
        queue::Queue,
    },
    tracing::info,
    crate::{
        data_loading::load_data_files,
        tokenization::run_tokenization_step,
        lemmatization::run_lemmatization_step,
        hasher::run_hashing_step,
        sentiment::run_sentiment_step,
    }
};

mod data_loading;
mod hasher;
mod lemmatization;
mod models;
mod progress;
mod sentiment;
mod tokenization;
mod utils;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    utils::init_logging();

    info!("bigdata nlp computer workshop");

    let config = Config::load();
    let queue = Arc::new(Queue::new(&config.infra().queue())); 

    // load_data_files(queue).await;
    // run_tokenization_step(queue).await;
    // run_lemmatization_step(queue).await;
    // run_hashing_step(queue).await;
    run_sentiment_step(queue).await;

    Ok(())
}
