use {
    typed_builder::TypedBuilder,
    serde::Serialize,
    crate::data::ChessGame,
};

#[derive(TypedBuilder, Serialize)]
pub struct ChessGameEntity {
    id: String,
    opening: String,
    white_player_elo: u32,
}

#[derive(TypedBuilder, Serialize)]
pub struct ChessGameMoveEntity {
    id: String,
    game_id: String,
    from_file: u8,
    from_rank: u8,
    to_file: u8,
    to_rank: u8,
}

impl ChessGameEntity {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn opening(&self) -> &str {
        &self.opening
    }

    pub fn white_player_elo(&self) -> u32 {
        self.white_player_elo
    }
}

pub fn into_chess_game_entity(id: String, game: ChessGame) -> ChessGameEntity {
    ChessGameEntity::builder()
        .id(id)
        .opening(game.opening)
        .white_player_elo(game.white_player.unwrap().elo)
        .build()
}