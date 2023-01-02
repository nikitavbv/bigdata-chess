use {
    sqlx::postgres::PgPoolOptions,
    crate::{
        config::DatabaseConfig,
        entity::ChessGameEntity,
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
}
