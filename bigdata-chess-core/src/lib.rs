pub mod config;
pub mod database;
pub mod entity;
pub mod game_parser;
pub mod lichess;
pub mod pgn;
pub mod queue;
pub mod storage;

pub mod data {
    include!(concat!(env!("OUT_DIR"), "/chess.rs"));
}