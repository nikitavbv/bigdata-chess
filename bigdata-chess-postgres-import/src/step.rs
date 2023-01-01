use {
    std::sync::Arc,
    tracing::info,
    prost::Message as ProstMessage,
    rdkafka::{message::Message, consumer::{Consumer, CommitMode}},
    indicatif::{ProgressBar, ProgressStyle},
    bigdata_chess_core::{
        queue::{Queue, TOPIC_CHESS_GAMES},
        database::{Database, ChessGameEntity},
        data::ChessGame,
    },
};

pub async fn postgres_import_step(queue: Arc<Queue>, database: Arc<Database>) {
    info!("running postgres import step");

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("importing games into postgres: {pos} games processed [{per_sec}]").unwrap());
    pb.set_message("importing games into progres");

    let consumer = queue.consumer_for_topic(
        "bigdata-chess-postgres-import",
        TOPIC_CHESS_GAMES,
    );

    loop {
        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();
        let game = ChessGame::decode(payload).unwrap();
        let id = base64::encode(msg.key().unwrap());
        let game_entity = into_chess_game_entity(id, game);
        
        database.save_game(&game_entity).await;

        consumer.commit_message(&msg, CommitMode::Sync).unwrap();
    }
}

fn into_chess_game_entity(id: String, game: ChessGame) -> ChessGameEntity {
    ChessGameEntity::builder()
        .id(id)
        .opening(game.opening)
        .white_player_elo(game.white_player.unwrap().elo)
        .build()
}