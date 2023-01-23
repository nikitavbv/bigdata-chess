use {
    std::{sync::Arc, collections::VecDeque},
    rdkafka::{consumer::Consumer, Message},
    tracing::info,
    bigdata_chess_core::queue::Queue,
    rdkafka::producer::{FutureRecord, Producer},
    rust_bert::pipelines::sentiment::SentimentModel,
    crate::{
        models::CommentTokenized,
        progress::Progress,
    }
};

pub async fn run_sentiment_step(queue: Arc<Queue>) {
    info!("this works?");

    let sentiment_model = SentimentModel::new(Default::default()).unwrap();
    let output = sentiment_model.predict(&["I love cookies so much!"]);

    info!("output: {:?}", output);
}