use {
    std::{sync::Arc, collections::VecDeque},
    rdkafka::{consumer::Consumer, Message},
    tokenizers::Tokenizer,
    bigdata_chess_core::queue::Queue,
    rdkafka::producer::{FutureRecord, Producer},
    crate::{
        models::Comment,
        progress::Progress,
    },
};

pub async fn run_tokenization_step(queue: Arc<Queue>) {
    let tokenizer = tokio::task::spawn_blocking(|| Tokenizer::from_pretrained("bert-base-cased", None).unwrap()).await.unwrap();

    let consumer = queue.consumer("bigdata-hn-tokenizer");
    consumer.subscribe(&["hn-comments"]).unwrap();

    let mut progress = Progress::new("tokenizing comments".to_owned());

    let mut message_join_handles = VecDeque::new();

    loop {
        let msg = consumer.recv().await.unwrap();

        let payload: Comment = serde_json::from_slice(&msg.payload().unwrap()).unwrap();

        let encoding = tokenizer.encode(payload.text.as_str(), false).unwrap();
        
        let tokenized = payload.tokenized(encoding.get_tokens().to_vec());

        let payload = serde_json::to_vec(&tokenized).unwrap();
        let id = tokenized.id.as_bytes().to_vec();

        let queue = queue.clone();
        let message_future = async move {
            queue.send_message(FutureRecord::to("hn-comments-tokenized").payload(&payload).key(&id)).await;
        };

        let task_future = tokio::spawn(message_future);
        message_join_handles.push_back(task_future);

        while message_join_handles.len() >= 1024 {
            message_join_handles.pop_front().unwrap().await.unwrap();
        }

        progress.update();
    }
}