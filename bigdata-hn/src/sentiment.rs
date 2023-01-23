use {
    std::{sync::Arc, collections::VecDeque},
    rdkafka::{consumer::Consumer, Message},
    tracing::info,
    bigdata_chess_core::queue::Queue,
    rdkafka::producer::{FutureRecord, Producer},
    rust_bert::pipelines::sentiment::{SentimentModel, SentimentPolarity},
    crate::{
        models::Comment,
        progress::Progress,
    }
};

pub async fn run_sentiment_step(queue: Arc<Queue>) {
    let consumer = queue.consumer("bigdata-hn-sentiment");
    consumer.subscribe(&["hn-comments"]).unwrap();

    let sentiment_model = tokio::task::spawn_blocking(|| SentimentModel::new(Default::default()).unwrap()).await.unwrap();
    
    let mut progress = Progress::new("analyzing sentiment".to_owned());

    let mut message_join_handles = VecDeque::new();
    loop {
        let msg = consumer.recv().await.unwrap();

        let payload: Comment = serde_json::from_slice(&msg.payload().unwrap()).unwrap();
        
        let output = sentiment_model.predict(&[payload.text.as_str()]);
    
        let sentiment = payload.sentiment(output.get(0).map(|v| crate::models::Sentiment {
            polarity: match v.polarity {
                SentimentPolarity::Positive => "positive".to_owned(),
                SentimentPolarity::Negative => "negative".to_owned(),
            },
            score: v.score,
        }).unwrap());

        let payload = serde_json::to_vec(&sentiment).unwrap();
        let id = sentiment.id.as_bytes().to_vec();

        let queue = queue.clone();

        let message_future = async move {
            queue.send_message(FutureRecord::to("hn-comments-sentiment").payload(&payload).key(&id)).await;
        };

        let task_future = tokio::spawn(message_future);
        message_join_handles.push_back(task_future);

        while message_join_handles.len() >= 1024 {
            message_join_handles.pop_front().unwrap().await.unwrap();
        }

        progress.update();
    }    
}