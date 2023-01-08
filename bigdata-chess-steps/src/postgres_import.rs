// current performance: 27/second
use {
    std::{sync::Arc, time::Instant},
    tracing::info,
    prost::Message as ProstMessage,
    rdkafka::{message::Message, consumer::{Consumer, CommitMode}},
    histogram::Histogram,
    futures_util::stream::FuturesUnordered,
    futures::{StreamExt, FutureExt, future::join_all},
    tokio::sync::Mutex,
    bigdata_chess_core::{
        queue::{Queue, TOPIC_CHESS_GAMES},
        database::Database,
        data::ChessGame,
        entity::{ChessGameEntity, into_chess_game_entity, into_chess_game_move_entity},
    },
    crate::progress::Progress,
};

#[allow(dead_code)] // used from other crate
pub async fn postgres_import_step(queue: Arc<Queue>, database: Arc<Database>) {
    info!("running postgres import step");

    let progress = Arc::new(Mutex::new(Progress::new("processing games".to_owned())));  
    
    let mut consumers = Vec::new();
    for _ in 0..4 {
        consumers.push(run_consumer(queue.clone(), database.with_same_config().await, progress.clone()));
    }

    join_all(consumers).await;
}

async fn run_consumer(queue: Arc<Queue>, database: Database, progress: Arc<Mutex<Progress>>) {
    let consumer = queue.consumer_for_topic(
        "bigdata-chess-postgres-import",
        TOPIC_CHESS_GAMES,
    );

    loop {
        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();

        let game = ChessGame::decode(payload).unwrap();
        let game_id = base64::encode(msg.key().unwrap());

        let mut entry_index = 0;

        let mut futures = Vec::new();

        for entry in &game.game_entries {
            entry_index += 1;
            if let Some(san) = &entry.san {
                if let Some(normal) = &san.normal {
                    let key = format!("{}:{}", game_id, entry_index);
                    let game_move_entity = into_chess_game_move_entity(key, &game_id, normal);
                    futures.push(database.save_game_move(game_move_entity).boxed());
                }
            }
        }
        
        let game_entity = into_chess_game_entity(game_id, game);
        futures.push(database.save_game(game_entity).boxed());

        join_all(futures).await;

        consumer.commit_message(&msg, CommitMode::Sync).unwrap();
        {
            progress.lock().await.update();
        }
    }
}