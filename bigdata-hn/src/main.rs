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
        tfidf_stats_collector::run_stats_collector_step,
        tfidf_metric_calculator::run_metric_calculator_step,
        sentiment::run_sentiment_step,
        categories::run_categories_step,
        embeddings_saver::run_embeddings_saver_step,
        bag_of_words::run_bag_of_words_step,
        clustering::run_clustering_step,
    }
};

mod bag_of_words;
mod categories;
mod clustering;
mod data_loading;
mod embeddings_saver;
mod hasher;
mod lemmatization;
mod models;
mod progress;
mod sentiment;
mod tfidf_metric_calculator;
mod tfidf_stats_collector;
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
    // run_sentiment_step(queue).await;
    // run_categories_step(queue).await;
    // run_stats_collector_step(queue).await;
    // run_metric_calculator_step(queue).await;
    // run_embeddings_saver_step(queue).await;
    // run_bag_of_words_step(queue).await;
    run_clustering_step().await;

    Ok(())
}
