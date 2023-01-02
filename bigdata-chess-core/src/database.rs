use {
    sqlx::postgres::PgPoolOptions,
    crate::{
        config::DatabaseConfig,
        entity::{ChessGameEntity, ChessGameMoveEntity},
    },
};

pub struct Database {
    pool: sqlx::postgres::PgPool,
}

impl Database {
    pub async fn new(config: &DatabaseConfig) -> Self {
        Self {
            pool: PgPoolOptions::new()
                .max_connections(5)
                .connect(config.connection_string().unwrap())
                .await
                .unwrap(),
        }
    }

    pub async fn save_game(&self, game: &ChessGameEntity) {
        sqlx::query("insert into chess_games (id, opening, white_player_elo) values ($1, $2, $3) on conflict do nothing")
            .bind(game.id())
            .bind(game.opening())
            .bind(game.white_player_elo() as i32)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    pub async fn save_game_move(&self, game_move: &ChessGameMoveEntity) {
        sqlx::query("insert into chess_game_moves (id, game_id, from_file, from_rank, to_file, to_rank) values ($1, $2, $3, $4, $5, $6)")
            .bind(game_move.id())
            .bind(game_move.game_id())
            .bind(game_move.from_file().map(|v| v as i32))
            .bind(game_move.from_rank().map(|v| v as i32))
            .bind(game_move.to_file().map(|v| v as i32))
            .bind(game_move.to_rank().map(|v| v as i32))
            .execute(&self.pool)
            .await
            .unwrap();
    }
}
