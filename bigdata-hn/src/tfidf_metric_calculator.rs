use {
    std::{sync::Arc, collections::{VecDeque, HashMap}, path::Path},
    tracing::info,
    rdkafka::{consumer::Consumer, Message, producer::FutureRecord},
    leveldb::{
        database::Database,
        kv::KV,
        options::{WriteOptions, ReadOptions, Options},
    },
    byteorder::ByteOrder,
    bigdata_chess_core::queue::Queue,
    crate::{
        models::CommentHashed,
        progress::Progress,
    }
};

pub async fn run_metric_calculator_step(queue: Arc<Queue>) {
    let consumer = queue.consumer("bigdata-hn-tfidf-metric-calculator");
    consumer.subscribe(&["hn-comments-hashed"]).unwrap();

    let mut open_opts = Options::new();
    open_opts.create_if_missing = true;
    let database: Database<i32> = Database::open(Path::new("./tfidf_stats"), open_opts).unwrap();

    let mut total_documents = database.get(ReadOptions::new(), -1)
        .unwrap()
        .map(|v| byteorder::BigEndian::read_u32(&v))
        .unwrap_or(0);

    let mut progress = Progress::new("calculating ifidf metric".to_owned());

    let mut message_join_handles = VecDeque::new();

    loop {
        let msg = consumer.recv().await.unwrap();

        let payload: CommentHashed = serde_json::from_slice(&msg.payload().unwrap()).unwrap();
        let mut hashes_count = HashMap::new();
        let mut metrics = Vec::new();

        for hash in &payload.hashes {
            hashes_count.insert(*hash, hashes_count.get(hash).unwrap_or(&0) + 1);
        }

        for (hash, count) in hashes_count {
            let tf = (count as f32) / (payload.hashes.len() as f32);
            let number_of_documents_with_term = database.get(ReadOptions::new(), hash as i32)
                .unwrap()
                .map(|v| byteorder::BigEndian::read_u32(&v))
                .unwrap_or(0) as f32;

            let idf = ((total_documents as f32) / number_of_documents_with_term).log(std::f32::consts::E);
        
            let tfidf = tf * idf;
            metrics.push(tfidf);
        }

        let tfidf = payload.tfidf(metrics);

        let payload = serde_json::to_vec(&tfidf).unwrap();
        let id = tfidf.id.as_bytes().to_vec();

        let queue = queue.clone();
        let message_future = async move {
            queue.send_message(FutureRecord::to("hn-comments-tfidf").payload(&payload).key(&id)).await;
        };

        let task_future = tokio::spawn(message_future);
        message_join_handles.push_back(task_future);

        while message_join_handles.len() >= 1024 {
            message_join_handles.pop_front().unwrap().await.unwrap();
        }

        progress.update();
    }
}