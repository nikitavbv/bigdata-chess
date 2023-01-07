use {
    std::pin::Pin,
    tracing::info,
    sqlx::postgres::PgPoolOptions,
    tokio_postgres::{NoTls, Statement},
    bb8_postgres::PostgresConnectionManager,
    crate::{
        config::DatabaseConfig,
        entity::{ChessGameEntity, ChessGameMoveEntity},
    },
};

pub struct Database {
    pool: sqlx::postgres::PgPool,
    bb8_pool: bb8::Pool<PostgresConnectionManager<NoTls>>,

    statement_insert_game_move: Statement,
}

impl Database {
    pub async fn new(config: &DatabaseConfig) -> Self {
        info!("connecting to database...");

        let pg_mgr = PostgresConnectionManager::new(config.connection_string().unwrap().parse().unwrap(), NoTls);
        let bb8_pool = bb8::Pool::builder()
            .max_size(4)
            .build(pg_mgr)
            .await
            .unwrap();

        info!("preparing statements");
        let statement_insert_game_move = bb8_pool.get().await.unwrap().prepare("insert into chess_game_moves (id, game_id, from_file, from_rank, to_file, to_rank) values ($1, $2, $3, $4, $5, $6) on conflict do nothing").await.unwrap();

        info!("connected to database");
        Self {
            pool: PgPoolOptions::new()
                .max_connections(5)
                .connect(config.connection_string().unwrap())
                .await
                .unwrap(),
            bb8_pool,

            statement_insert_game_move, 
        }
    }

    pub async fn save_game(&self, game: ChessGameEntity) {
        sqlx::query("insert into chess_games (id, opening, white_player_elo) values ($1, $2, $3) on conflict do nothing")
            .bind(game.id())
            .bind(game.opening())
            .bind(game.white_player_elo() as i32)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    pub async fn save_game_move(&self, game_move: ChessGameMoveEntity) {
        self.bb8_pool.get().await.unwrap().query(&self.statement_insert_game_move, &[
            &game_move.id(),
            &game_move.game_id(),
            &game_move.from_file().map(|v| v as i32),
            &game_move.from_rank().map(|v| v as i32),
            &game_move.to_file().map(|v| v as i32),
            &game_move.to_rank().map(|v| v as i32)
        ]).await.unwrap();
    }
}
