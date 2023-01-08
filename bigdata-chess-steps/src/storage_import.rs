use {
    std::{sync::Arc, time::Instant},
    tracing::info,
    rdkafka::{consumer::{Consumer, CommitMode}, Message},
    prost::Message as ProstMessage,
    rand::{Rng, distributions::Alphanumeric},
    bigdata_chess_core::{
        queue::{Queue, TOPIC_CHESS_GAMES},
        storage::Storage,
        entity::{into_chess_game_entity, into_chess_game_move_entity, into_chess_game_comment_eval_entity},
        data::ChessGame,
    },
    crate::progress::Progress,
};

const GAMES_PER_FILE: u64 = 20_000;
const MOVES_PER_FILE: u64 = GAMES_PER_FILE * 50;

#[allow(dead_code)] // used from other crate
pub async fn storage_import_step(queue: Arc<Queue>, storage: Arc<Storage>) {
    info!("running storage import step");

    let consumer = queue.consumer_for_topic(
        "bigdata-chess-storage-import",
        TOPIC_CHESS_GAMES,
    );

    let mut progress = Progress::new("processing games".to_owned());
    let mut games = Vec::new();
    let mut moves = Vec::new();
    let mut comment_evals = Vec::new();

    let mut time_total: f64 = 0.0;
    let mut time_commit_offsets: f64 = 0.0;

    loop {
        let started_at = Instant::now();

        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();

        let game = ChessGame::decode(payload).unwrap();
        let game_id = base64::encode(msg.key().unwrap());

        let mut entry_index = 0;
        for entry in &game.game_entries {
            entry_index += 1;
            if let Some(san) = &entry.san {
                if let Some(normal) = &san.normal {
                    moves.push(into_chess_game_move_entity(&game_id,  entry_index,&normal, san.is_check.unwrap_or(false), san.is_checkmate.unwrap_or(false)));
                }
            } else if let Some(comment) = &entry.comment {
                if let Some(eval) = comment.eval {
                    comment_evals.push(into_chess_game_comment_eval_entity(&game_id, entry_index, eval));
                }
            }
        }
        games.push(into_chess_game_entity(game_id, game));

        let commit_message_started_at = Instant::now();
        consumer.commit_message(&msg, CommitMode::Sync).unwrap();
        time_commit_offsets += (Instant::now() - commit_message_started_at).as_secs_f64();

        if progress.update() {
            info!("time_total: {}", time_total.round());
            info!("time_commit_offsets: {}", time_commit_offsets.round());
        }
        
        while games.len() > GAMES_PER_FILE as usize {
            let output_data = {
                let mut output_data = Vec::new();

                {
                    let mut csv_writer = csv::Writer::from_writer(&mut output_data);
                    for game in &games {
                        csv_writer.serialize(&game).unwrap();
                    }
                    games.clear();
                }   

                output_data
            };
            
            let key = generate_game_data_file_key();
            storage.put_game_data_file(&key, output_data).await;
            info!("uploaded game data file with key: {}", key);
        }

        while moves.len() > MOVES_PER_FILE as usize {
            let output_data = {
                let mut output_data = Vec::new();

                {
                    let mut csv_writer = csv::Writer::from_writer(&mut output_data);
                    for game_move in &moves {
                        csv_writer.serialize(&game_move).unwrap();
                    }
                    moves.clear();
                }

                output_data
            };

            let key = generate_game_data_file_key();
            storage.put_game_moves_data_file(&key, output_data).await;
            info!("uploaded game moves data file with key: {}", key);

            // comments eval
            let output_data = {
                let mut output_data = Vec::new();

                {
                    let mut csv_writer = csv::Writer::from_writer(&mut output_data);
                    for comment_eval in &comment_evals {
                        csv_writer.serialize(&comment_eval).unwrap();
                    }
                    comment_evals.clear();
                }

                output_data
            };

            let key = generate_game_data_file_key();
            storage.put_game_comment_eval_data_file(&key, output_data).await;
            info!("uploaded game eval comments data file with key: {}", key);

            time_total += (Instant::now() - started_at).as_secs_f64();
        }
    }
}

fn generate_game_data_file_key() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect()
}