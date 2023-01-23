use {
    std::{sync::Arc, collections::VecDeque},
    bigdata_chess_core::queue::Queue,
    tracing::info,
    rdkafka::{consumer::Consumer, Message, producer::FutureRecord},
    pyo3::{prelude::*, types::IntoPyDict},
    crate::{
        progress::Progress,
        models::CommentTokenized,
    },
};

pub async fn run_lemmatization_step(queue: Arc<Queue>) {
    let consumer = queue.consumer("bigdata-hn-lemmatizer");
    consumer.subscribe(&["hn-comments-tokenized"]).unwrap();

    Python::with_gil(|py| {
        py.run(r#"
from nltk.stem import WordNetLemmatizer  
lemmatizer = WordNetLemmatizer()
"#, None, None).unwrap();
    });

    let mut progress = Progress::new("lemmatizing comments".to_owned());

    let mut message_join_handles = VecDeque::new();

    loop {
        let msg = consumer.recv().await.unwrap();

        let payload: CommentTokenized = serde_json::from_slice(&msg.payload().unwrap()).unwrap();
        let lemmas: Vec<String> = payload.tokens.iter().map(|token| lemmatize(token.to_string())).collect();

        let lemmatized = payload.lemmatized(lemmas);

        let payload = serde_json::to_vec(&lemmatized).unwrap();
        let id = lemmatized.id.as_bytes().to_vec();

        let queue = queue.clone();

        let message_future = async move {
            queue.send_message(FutureRecord::to("hn-comments-lemmatized").payload(&payload).key(&id)).await;
        };

        let task_future = tokio::spawn(message_future);
        message_join_handles.push_back(task_future);

        while message_join_handles.len() >= 1024 {
            message_join_handles.pop_front().unwrap().await.unwrap();
        }

        progress.update();
    }
}

fn lemmatize(word: String) -> String {
    Python::with_gil(|py| 
        py.eval(
            "lemmatizer.lemmatize(word)", 
            None, 
            Some([("word", word)].into_py_dict(py))
        ).unwrap().extract().unwrap()
    )
}