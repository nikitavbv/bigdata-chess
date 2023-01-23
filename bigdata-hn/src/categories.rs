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

    let mut message_join_handles = VecDeque::new();

    loop {
        let msg = consumer.recv().await.unwrap();

        let payload: Comment = serde_json::from_slice(&msg.payload().unwrap()).unwrap();
        
        let embeddings = model.encode(&[payload.text.as_str()]).unwrap()[0].clone();
        let embeddings = payload.embeddings(embeddings);

        let payload = serde_json::to_vec(&embeddings).unwrap();
        let id = embeddings.id.as_bytes().to_vec();

        let queue = queue.clone();
        let message_future = async move {
            queue.send_message(FutureRecord::to("hn-comments-embeddings").payload(&payload).key(&id)).await;
        };

        let task_future = tokio::spawn(message_future);
        message_join_handles.push_back(task_future);

        while message_join_handles.len() >= 1024 {
            message_join_handles.pop_front().unwrap().await.unwrap();
        }

        progress.update();
    }
}