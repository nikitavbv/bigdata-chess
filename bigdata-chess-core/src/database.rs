use {
    tracing::info,
    tokio_postgres::{NoTls, Statement},
    crate::{
        config::DatabaseConfig,
        entity::{ChessGameEntity, ChessGameMoveEntity},
    },
};

pub struct Database {
    connection_string: String,
    client: tokio_postgres::Client,

    statement_insert_game_move: Statement,
}

impl Database {
    pub async fn new(config: &DatabaseConfig) -> Self {
        info!("connecting to database...");

        // for some reason, adding a connection pool had negative impact on performance
        let (client, connection) = tokio_postgres::connect(config.connection_string().unwrap(), NoTls).await.unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        info!("preparing statements");
        let statement_insert_game_move = client.prepare("insert into chess_game_moves (id, game_id, from_file, from_rank, to_file, to_rank) values ($1, $2, $3, $4, $5, $6) on conflict do nothing").await.unwrap();

        info!("connected to database");
        Self {
            connection_string: config.connection_string().unwrap().clone(),
            client,

            statement_insert_game_move, 
        }
    }

    pub async fn with_same_config(&self) -> Self {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await.unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let statement_insert_game_move = client.prepare("insert into chess_game_moves (id, game_id, from_file, from_rank, to_file, to_rank) values ($1, $2, $3, $4, $5, $6) on conflict do nothing").await.unwrap();

        Self {
            connection_string: self.connection_string.clone(),
            client,
            statement_insert_game_move,
        }
    }

    pub async fn save_game(&self, game: ChessGameEntity) {
        self.client.query("insert into chess_games (id, opening, white_player_elo) values ($1, $2, $3) on conflict do nothing", &[
            &game.id(),
            &game.opening(),
            &(game.white_player_elo() as i32),
        ]).await.unwrap();
    }

    pub async fn save_game_move(&self, game_move: ChessGameMoveEntity) {
        self.client.query(&self.statement_insert_game_move, &[
            &game_move.id(),
            &game_move.game_id(),
            &game_move.from_file().map(|v| v as i32),
            &game_move.from_rank().map(|v| v as i32),
            &game_move.to_file().map(|v| v as i32),
            &game_move.to_rank().map(|v| v as i32)
        ]).await.unwrap();
    }
}
