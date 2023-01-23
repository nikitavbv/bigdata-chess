use {
    std::{sync::Arc, collections::HashMap, fs},
    rdkafka::{consumer::Consumer, Message},
    bigdata_chess_core::queue::Queue,
    crate::{
        models::CommentTokenized,
        progress::Progress,
    }
};

pub async fn run_bag_of_words_step(queue: Arc<Queue>) {
    let consumer = queue.consumer("bigdata-hn-bag-of-words");
    consumer.subscribe(&["hn-comments-tokenized"]).unwrap();

    let mut progress = Progress::new("calculating bag of words".to_owned());

    let mut bag_of_words = HashMap::new();

    loop {
        let msg = consumer.recv().await.unwrap();
        
        let payload: CommentTokenized = serde_json::from_slice(&msg.payload().unwrap()).unwrap();
        for token in &payload.tokens {
            let key = token.to_lowercase();
            bag_of_words.insert(key.clone(), bag_of_words.get(&key).unwrap_or(&0) + 1);
        }

        if progress.update() {
            fs::write("bag_of_words.json", &serde_json::to_vec(&bag_of_words).unwrap()).unwrap();
        }
    }
}