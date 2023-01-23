use byteorder::ByteOrder;

use {
    std::{sync::Arc, collections::HashSet, path::Path},
    rdkafka::{consumer::Consumer, Message},
    tracing::info,
    leveldb::{
        database::Database,
        kv::KV,
        options::{WriteOptions, ReadOptions, Options},
    },
    bigdata_chess_core::queue::Queue,
    crate::{
        models::CommentHashed,
        progress::Progress,
    }
};

pub async fn run_stats_collector_step(queue: Arc<Queue>) {
    let consumer = queue.consumer("bigdata-hn-tfidf-stats-collector");
    consumer.subscribe(&["hn-comments-hashed"]).unwrap();

    let mut open_opts = Options::new();
    open_opts.create_if_missing = true;
    let database: Database<i32> = Database::open(Path::new("./tfidf_stats"), open_opts).unwrap();

    let mut progress = Progress::new("collecting stats for tfidf".to_owned());

    let mut total_documents = database.get(ReadOptions::new(), -1)
        .unwrap()
        .map(|v| byteorder::BigEndian::read_u32(&v))
        .unwrap_or(0);

    loop {
        let msg = consumer.recv().await.unwrap();

        let payload: CommentHashed = serde_json::from_slice(&msg.payload().unwrap()).unwrap();
    
        let hashes: HashSet<u32> = payload.hashes.into_iter().collect();
        for hash in hashes {
            let key = hash as i32; // safe because of bucketing

            let prev = database.get(ReadOptions::new(), key)
                .unwrap()
                .map(|v| byteorder::BigEndian::read_u32(&v))
                .unwrap_or(0);

            let mut value = vec![0; 4];
            byteorder::BigEndian::write_u32(&mut value, prev + 1);

            database.put(WriteOptions::new(), key, &value).unwrap();
        }
        
        total_documents += 1;
        let mut value = vec![0; 4];
        byteorder::BigEndian::write_u32(&mut value, total_documents);
        database.put(WriteOptions::new(), -1, &value).unwrap();

        progress.update();
    }
}