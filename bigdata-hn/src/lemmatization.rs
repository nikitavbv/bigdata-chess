use rdkafka::{consumer::Consumer, Message};

use {
    std::sync::Arc,
    bigdata_chess_core::queue::Queue,
    tracing::info,
    crate::{
        progress::Progress,
        models::CommentTokenized,
    },
};

pub async fn run_lemmatization_step(queue: Arc<Queue>) {
    let consumer = queue.consumer("bigdata-hn-lemmatizer");
    consumer.subscribe(&["hn-comments-tokenized"]).unwrap();

    let mut progress = Progress::new("lemmatizing comments".to_owned());

    loop {
        let msg = consumer.recv().await.unwrap();

        let payload: CommentTokenized = serde_json::from_slice(&msg.payload().unwrap()).unwrap();

        info!("payload: {:?}", payload);
        break;
    }
}