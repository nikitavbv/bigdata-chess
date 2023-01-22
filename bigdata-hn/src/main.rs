use {
    std::{fs::{File, read_dir}, time::Duration, sync::Arc, collections::VecDeque},
    serde::{Serialize, Deserialize},
    rdkafka::producer::{FutureRecord, Producer},
    indicatif::ProgressBar,
    bigdata_chess_core::{
        config::Config,
        queue::Queue,
    },
};

#[derive(Deserialize, Debug)]
pub struct RawDataEntry {
    id: String,
    text: String,
}

#[derive(Serialize, Deserialize)]
pub struct Comment {
    id: String,
    text: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    println!("Hello, world!");

    let config = Config::load();
    let queue = Arc::new(Queue::new(&config.infra().queue())); 
 
    for path in read_dir("./data").unwrap() {
        load_data_file(queue.clone(), &path.unwrap().file_name().to_str().unwrap().to_string()).await;
    }

    Ok(())
}

async fn load_data_file(queue: Arc<Queue>, file_name: &str) {
    println!("loading file: {}", file_name);

    let mut reader = csv::Reader::from_reader(File::open(format!("data/{}", file_name)).unwrap());
    let headers = reader.headers().unwrap().clone();
    let records: Vec<_> = reader.records().collect();

    let pb = ProgressBar::new(records.len() as u64);

    let mut message_join_handles = VecDeque::new();

    for result in records {
        let result: RawDataEntry = result.unwrap().deserialize(Some(&headers)).unwrap();
        let comment = Comment {
            id: result.id,
            text: result.text,
        };

        let payload = serde_json::to_vec(&comment).unwrap();
        let id = comment.id.as_bytes().to_vec();

        let queue = queue.clone();
        let message_future = async move {
            queue.send_message(FutureRecord::to("hn-comments").payload(&payload).key(&id)).await;
        };

        let task_future = tokio::spawn(message_future);
        message_join_handles.push_back(task_future);

        while message_join_handles.len() >= 1024 {
            message_join_handles.pop_front().unwrap().await.unwrap();
        }

        pb.inc(1);
    }

    pb.finish();
}