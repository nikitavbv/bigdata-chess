use {
    typed_builder::TypedBuilder,
    serde::Serialize,
    crate::data::{ChessGame, NormalSan},
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
    from_file: Option<u8>,
    from_rank: Option<u8>,
    to_file: Option<u8>,
    to_rank: Option<u8>,
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

impl ChessGameMoveEntity {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn game_id(&self) -> &str {
        &self.game_id
    }

    pub fn from_file(&self) -> Option<u8> {
        self.from_file
    }

    pub fn from_rank(&self) -> Option<u8> {
        self.from_rank
    }

    pub fn to_file(&self) -> Option<u8> {
        self.to_file
    }

    pub fn to_rank(&self) -> Option<u8> {
        self.to_rank
    }
}

pub fn into_chess_game_entity(id: String, game: ChessGame) -> ChessGameEntity {
    ChessGameEntity::builder()
        .id(id)
        .opening(game.opening)
        .white_player_elo(game.white_player.unwrap().elo)
        .build()
}

pub fn into_chess_game_move_entity(id: String, game_id: &str, san: &NormalSan) -> ChessGameMoveEntity {
    ChessGameMoveEntity::builder()
        .id(id)
        .game_id(game_id.to_owned())
        .from_file(san.file.map(|v| v as u8))
        .from_rank(san.rank.map(|v| v as u8))
        .to_file(san.to.as_ref().map(|v| v.file as u8))
        .to_rank(san.to.as_ref().map(|v| v.rank as u8))
        .build()
}