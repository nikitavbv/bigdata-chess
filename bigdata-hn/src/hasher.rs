use {
    std::{sync::Arc, collections::VecDeque, io::Cursor},
    rdkafka::{consumer::Consumer, Message, producer::FutureRecord},
    bigdata_chess_core::queue::Queue,
    crate::{
        models::{HASHING_BUCKETS, CommentTokenized},
        progress::Progress,
    }
};

pub async fn run_hashing_step(queue: Arc<Queue>) {
    let consumer = queue.consumer("bigdata-hn-hasher");
    consumer.subscribe(&["hn-comments-tokenized"]).unwrap();

    let mut progress = Progress::new("hashing comments".to_owned());

    let mut message_join_handles = VecDeque::new();

    loop {
        let msg = consumer.recv().await.unwrap();

        let payload: CommentTokenized = serde_json::from_slice(&msg.payload().unwrap()).unwrap();

        let hashes: Vec<u32> = payload.tokens.iter()
            .map(|token| murmur3::murmur3_32(&mut Cursor::new(token), 0).unwrap() % HASHING_BUCKETS)
            .collect();

        let hashed = payload.hashed(hashes);

        let payload = serde_json::to_vec(&hashed).unwrap();
        let id = hashed.id.as_bytes().to_vec();

        let queue = queue.clone();

        let message_future = async move {
            queue.send_message(FutureRecord::to("hn-comments-hashed").payload(&payload).key(&id)).await;
        };

        let task_future = tokio::spawn(message_future);
        message_join_handles.push_back(task_future);

        while message_join_handles.len() >= 1024 {
            message_join_handles.pop_front().unwrap().await.unwrap();
        }

        progress.update();
    }
}