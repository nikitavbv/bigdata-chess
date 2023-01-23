use {
    std::{sync::Arc, collections::VecDeque},
    rdkafka::{consumer::Consumer, Message, producer::FutureRecord},
    tracing::info,
    rust_bert::pipelines::sentence_embeddings::{SentenceEmbeddingsBuilder, SentenceEmbeddingsModelType},
    bigdata_chess_core::queue::Queue,
    crate::{
        models::Comment,
        progress::Progress,
    }
};

pub async fn run_categories_step(queue: Arc<Queue>) {
    let model = tokio::task::spawn_blocking(|| SentenceEmbeddingsBuilder::remote(SentenceEmbeddingsModelType::AllMiniLmL12V2).create_model().unwrap()).await.unwrap();

    let consumer = queue.consumer("bigdata-hn-categories");
    consumer.subscribe(&["hn-comments"]).unwrap();

    let mut progress = Progress::new("calculating categories for comments".to_owned());

    // let mut message_join_handles = VecDeque::new();

    loop {
        let msg = consumer.recv().await.unwrap();

        let payload: Comment = serde_json::from_slice(&msg.payload().unwrap()).unwrap();
        
        let embeddings = model.encode(&[payload.text.as_str()]).unwrap();
    
        info!("embeddings: {:?}", embeddings);
        break;
    }
}