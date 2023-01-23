use {
    std::{sync::Arc, fs::OpenOptions, io::prelude::*},
    rdkafka::{consumer::Consumer, Message},
    bigdata_chess_core::queue::Queue,
    crate::{
        models::CommentEmbeddings,
        progress::Progress,
    }
};

pub async fn run_embeddings_saver_step(queue: Arc<Queue>) {
    let consumer = queue.consumer("bigdata-hn-embeddings-saver");
    consumer.subscribe(&["hn-comments-embeddings"]).unwrap();

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("embeddings.json")
        .unwrap();
        
    let mut progress = Progress::new("saving embeddings".to_owned());

    loop {
        let msg = consumer.recv().await.unwrap();

        let payload = String::from_utf8(msg.payload().unwrap().to_vec()).unwrap();

        writeln!(file, "{}", payload).unwrap();

        progress.update();
    }
}