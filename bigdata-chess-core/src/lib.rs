pub mod config;
pub mod database;
pub mod queue;

pub mod data {
    include!(concat!(env!("OUT_DIR"), "/chess.rs"));
}