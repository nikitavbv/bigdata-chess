use {
    std::sync::Arc,
    tracing::info,
    prost::Message as ProstMessage,
    rdkafka::{message::Message, consumer::{Consumer, CommitMode}},
    bigdata_chess_core::{
        queue::{Queue, TOPIC_CHESS_GAMES},
        database::Database,
        data::ChessGame,
        entity::{ChessGameEntity, into_chess_game_entity},
    },
    crate::progress::Progress,
};

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
        let id = base64::encode(msg.key().unwrap());
        let game_entity = into_chess_game_entity(id, game);
        
        database.save_game(&game_entity).await;

        consumer.commit_message(&msg, CommitMode::Sync).unwrap();
        
        progress.update();
    }
}
