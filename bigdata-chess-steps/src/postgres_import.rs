// current performance: 0.7/second

use {
    std::{sync::Arc, time::Instant},
    tracing::info,
    prost::Message as ProstMessage,
    rdkafka::{message::Message, consumer::{Consumer, CommitMode}},
    histogram::Histogram,
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

    let consumer = queue.consumer_for_topic(
        "bigdata-chess-postgres-import",
        TOPIC_CHESS_GAMES,
    );

    let mut progress = Progress::new("processing games".to_owned());

    let mut time_all = Histogram::new();
    let mut time_database_ops = Histogram::new();
    let mut time_database_moves = Histogram::new();
    let mut time_database_games = Histogram::new();

    let mut processed_games = 0;

    loop {
        let started_at = Instant::now();
        let mut database_ops_millis = 0;

        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();

        let game = ChessGame::decode(payload).unwrap();
        let game_id = base64::encode(msg.key().unwrap());

        let mut entry_index = 0;
        let mut database_moves_millis = 0;
        for entry in &game.game_entries {
            entry_index += 1;
            if let Some(san) = &entry.san {
                if let Some(normal) = &san.normal {
                    let key = format!("{}:{}", game_id, entry_index);
                    let game_move_entity = into_chess_game_move_entity(key, &game_id, normal);

                    let started_at = Instant::now();
                    database.save_game_move(&game_move_entity).await;
                    let time_spent = (Instant::now() - started_at).as_millis();
                    database_ops_millis += time_spent;
                    database_moves_millis += time_spent;
                }
            }
        }
        time_database_moves.increment(database_moves_millis as u64).unwrap();

        let game_entity = into_chess_game_entity(game_id, game);
        
        {
            let started_at = Instant::now();
            database.save_game(&game_entity).await;
            let time_spent = (Instant::now() - started_at).as_millis();
            database_ops_millis += time_spent;
            time_database_games.increment(time_spent as u64).unwrap();
        }
        
        consumer.commit_message(&msg, CommitMode::Sync).unwrap();
        
        progress.update();
        processed_games += 1;

        time_all.increment((Instant::now() - started_at).as_millis() as u64).unwrap();
        time_database_ops.increment(database_ops_millis as u64).unwrap();

        if processed_games % 100 == 0 {
            info!("time_all: {}", time_all.percentile(90.0).unwrap());
            info!("time_database_ops: {}", time_database_ops.percentile(90.0).unwrap());
            info!("time_database_games: {}", time_database_games.percentile(90.0).unwrap());
            info!("time_database_moves: {}", time_database_moves.percentile(90.0).unwrap());
        }
    }
}
