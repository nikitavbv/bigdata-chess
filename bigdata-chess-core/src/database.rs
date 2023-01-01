use {
    sqlx::postgres::PgPoolOptions,
    typed_builder::TypedBuilder,
    crate::config::DatabaseConfig,
};

pub struct Database {
    pool: sqlx::postgres::PgPool,
}

#[derive(TypedBuilder)]
pub struct ChessGameEntity {
    id: String,
    opening: String,
    white_player_elo: u32,
}

#[derive(TypedBuilder)]
pub struct ChessGameMoveEntity {
    id: String,
    game_id: String,
    from_file: u8,
    from_rank: u8,
    to_file: u8,
    to_rank: u8,
}

impl Database {
    pub async fn new(config: &DatabaseConfig) -> Self {
        Self {
            pool: PgPoolOptions::new()
                .max_connections(5)
                .connect(config.connection_string())
                .await
                .unwrap(),
        }
    }

    pub async fn save_game(&self, game: &ChessGameEntity) {
        sqlx::query("insert into chess_games (id, opening, white_player_elo) values ($1, $2, $3)")
            .bind(&game.id)
            .bind(&game.opening)
            .bind(game.white_player_elo as i32)
            .execute(&self.pool)
            .await
            .unwrap();
    }
}
