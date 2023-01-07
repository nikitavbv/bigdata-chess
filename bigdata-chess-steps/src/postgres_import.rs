// current performance: 0.7/second

use {
    std::sync::Arc,
    tracing::info,
    prost::Message as ProstMessage,
    rdkafka::{message::Message, consumer::{Consumer, CommitMode}},
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

    loop {
        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();

        let game = ChessGame::decode(payload).unwrap();
        let game_id = base64::encode(msg.key().unwrap());

        let mut entry_index = 0;
        for entry in &game.game_entries {
            entry_index += 1;
            if let Some(san) = &entry.san {
                if let Some(normal) = &san.normal {
                    let key = format!("{}:{}", game_id, entry_index);
                    let game_move_entity = into_chess_game_move_entity(key, &game_id, normal);
                    database.save_game_move(&game_move_entity).await;
                }
            }
        }
        let game_entity = into_chess_game_entity(game_id, game);
        
        database.save_game(&game_entity).await;

        consumer.commit_message(&msg, CommitMode::Sync).unwrap();
        
        progress.update();
    }
}
