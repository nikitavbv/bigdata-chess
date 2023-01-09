use {
    typed_builder::TypedBuilder,
    serde::Serialize,
    chrono::{NaiveDateTime, NaiveDate},
    crate::data::{ChessGame, NormalSan},
};

#[derive(TypedBuilder, Serialize)]
pub struct ChessGameEntity {
    id: String,
    event_name: String,
    link: String,
    date: Option<i64>,
    black_player_name: String,
    black_player_elo: u32,
    black_player_title: Option<String>,
    white_player_name: String,
    white_player_elo: u32,
    white_player_title: Option<String>,
    result: u8,
    rating_outcome_for_white: Option<i32>,
    rating_outcome_for_black: Option<i32>,
    eco: String,
    opening: String,
    timecontrol_duration: Option<u32>,
    timecontrol_increment: Option<u32>,
    termination: u32,

    // partition key should be last field
    day: String, // same as date, but YYYY-MM-DD, to be used for partitioning
}

// in hive: cluster by game_id
#[derive(TypedBuilder, Serialize)]
pub struct ChessGameMoveEntity {
    game_id: String,
    move_id: u32,
    from_file: Option<u8>,
    from_rank: Option<u8>,
    to_file: Option<u8>,
    to_rank: Option<u8>,
    capture: bool,
    promotion: Option<u8>,
    is_check: bool,
    is_checkmate: bool,
}

// in hive: cluster by game_id
#[derive(TypedBuilder, Serialize)]
pub struct ChessGameCommentEval {
    game_id: String,
    move_id: u32,
    eval: f32,
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
    pub fn game_id(&self) -> &str {
        &self.game_id
    }

    pub fn move_id(&self) -> u32 {
        self.move_id
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
        .event_name(game.event_name)
        .link(game.link)
        .date(game.date.as_ref().map(|v| v.seconds))
        .day(game.date.map(|v| {
            let naive = NaiveDateTime::from_timestamp_opt(v.seconds, 0).unwrap();
            naive.format("%Y-%m-%d").to_string()
        }).unwrap_or("0000-00-00".to_owned()))
        .black_player_name(game.black_player.as_ref().unwrap().name.clone())
        .black_player_elo(game.black_player.as_ref().unwrap().elo)
        .black_player_title(game.black_player.unwrap().title.map(|v| title_name_from_id(v)))
        .white_player_name(game.white_player.as_ref().unwrap().name.clone())
        .white_player_elo(game.white_player.as_ref().unwrap().elo)
        .white_player_title(game.white_player.unwrap().title.map(title_name_from_id))
        .result(game.result as u8)
        .rating_outcome_for_white(game.rating_outcome_for_white)
        .rating_outcome_for_black(game.rating_outcome_for_black)
        .eco(game.eco)
        .opening(game.opening)
        .timecontrol_duration(game.timecontrol.as_ref().map(|v| v.duration as u32))
        .timecontrol_increment(game.timecontrol.map(|v| v.increment as u32))
        .termination(game.termination as u32)
        .build()
}

pub fn into_chess_game_move_entity(game_id: &str, move_id: u32, san: &NormalSan, is_check: bool, is_checkmate: bool) -> ChessGameMoveEntity {
    ChessGameMoveEntity::builder()
        .game_id(game_id.to_owned())
        .move_id(move_id)
        .from_file(san.file.map(|v| v as u8))
        .from_rank(san.rank.map(|v| v as u8))
        .to_file(san.to.as_ref().map(|v| v.file as u8))
        .to_rank(san.to.as_ref().map(|v| v.rank as u8))
        .capture(san.capture)
        .promotion(san.promotion.map(|v| v as u8))
        .is_check(is_check)
        .is_checkmate(is_checkmate)
        .build()
}

pub fn into_chess_game_comment_eval_entity(game_id: &str, move_id: u32, eval: f32) -> ChessGameCommentEval {
    ChessGameCommentEval::builder()
        .game_id(game_id.to_owned())
        .move_id(move_id)
        .eval(eval)
        .build()
}

fn title_name_from_id(id: i32) -> String {
    match id {
        0 => "FM",
        1 => "IM",
        2 => "NM",
        3 => "BOT",
        4 => "CM",
        5 => "GM",
        6 => "WIM",
        7 => "WFM",
        8 => "LM",
        9 => "WGM",
        10 => "WCM",
        other => panic!("Unexpected title: {}", other),
    }.to_owned()
}