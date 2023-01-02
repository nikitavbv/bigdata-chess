use {
    std::sync::Arc,
    tracing::info,
    rdkafka::{consumer::{Consumer, CommitMode}, Message},
    prost::Message as ProstMessage,
    rand::{Rng, distributions::Alphanumeric},
    bigdata_chess_core::{
        queue::{Queue, TOPIC_CHESS_GAMES},
        storage::Storage,
        entity::into_chess_game_entity,
        data::ChessGame,
    },
    crate::progress::Progress,
};

pub async fn storage_import_step(queue: Arc<Queue>, storage: Arc<Storage>) {
    info!("running storage import step");

    let consumer = queue.consumer_for_topic(
        "bigdata-chess-storage-import",
        TOPIC_CHESS_GAMES,
    );

    let mut progress = Progress::new("processing games".to_owned());
    let mut games = Vec::new();

    loop {
        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();

        let game = ChessGame::decode(payload).unwrap();
        let id = base64::encode(msg.key().unwrap());
        games.push(into_chess_game_entity(id, game));

        consumer.commit_message(&msg, CommitMode::Sync).unwrap();
        progress.update();
        
        if games.len() > 100_000 {
            let output_data = {
                let mut output_data = Vec::new();

                {
                    let mut csv_writer = csv::Writer::from_writer(&mut output_data);
                    for game in &games {
                        csv_writer.serialize(&game).unwrap();
                    }
                    games.clear();
                }   

                output_data
            };
            
            let key = generate_game_data_file_key();
            storage.put_game_data_file(&key, output_data).await;
            info!("uploaded game data file with key: {}", key);
        }
    }
}

fn generate_game_data_file_key() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect()
}