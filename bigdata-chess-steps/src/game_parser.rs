// performance: 167 games/sec

use {
    std::{sync::Arc, time::Instant, collections::VecDeque},
    tracing::{info, error},
    rdkafka::{consumer::{Consumer, CommitMode}, Message, producer::FutureRecord},
    prost::Message as ProstMessage,
    prost_types::Timestamp,
    pgn_reader::{BufferedReader, Visitor, SanPlus, RawComment},
    shakmaty::san::Suffix,
    chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike},
    rand::Rng,
    bigdata_chess_core::{
        queue::{Queue, TOPIC_LICHESS_RAW_GAMES, TOPIC_CHESS_GAMES},
        data::{
            RawChessGame, 
            ChessGame, 
            ChessGameBuilder, 
            Player,
            PlayerBuilder,
            PlayerTitle,
            GameResult, 
            Timecontrol, 
            Termination, 
            GameEntry,
            NormalSan,
            Role,
            File,
            Rank,
            Square,
            CastleSan,
            CastlingSide,
            San,
            PutSan,
            Nag,
            Comment,
        },
        config::GameParserStepConfig,
    },
    crate::progress::Progress,
};

pub async fn game_parser_step(config: &GameParserStepConfig, queue: Arc<Queue>) -> std::io::Result<()> {
    info!("running game parser step");

    let consumer = queue.consumer(&config.group_id());
    consumer.subscribe(&vec![config.from_topic().as_str()]).unwrap();
    let to_topic = config.to_topic();

    let mut progress = Progress::new("processed games".to_owned());

    let mut message_join_handles = VecDeque::new();

    let mut time_total: f64 = 0.0;
    let mut time_message_recv: f64 = 0.0;
    let mut time_encode_decode: f64 = 0.0;
    let mut time_io: f64 = 0.0;
    let mut time_commit_offsets: f64 = 0.0;

    loop {
        let started_at = Instant::now();

        let message_recv_started_at = Instant::now();
        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();
        time_message_recv += (Instant::now() - message_recv_started_at).as_secs_f64();

        let encode_decode_started_at = Instant::now();
        let raw_game = RawChessGame::decode(payload).unwrap();
        let pgn = format!("{}\n\n{}", raw_game.metadata, raw_game.moves);

        let mut reader = BufferedReader::new(pgn.as_bytes());
        let mut visitor = GameVisitor::new();

        let game = match reader.read_game(&mut visitor).unwrap().unwrap() {
            Ok(v) => v.encode_to_vec(),
            Err(err) => {
                error!("Failed to read game: {:?} for pgn: {}", err, pgn);
                for error in err {
                    queue.send_game_parser_error(error).await;
                }
                consumer.commit_message(&msg, CommitMode::Sync).unwrap();
                continue;
            },
        };
        time_encode_decode += (Instant::now() - encode_decode_started_at).as_secs_f64();

        let io_started_at = Instant::now();

        let queue = queue.clone();
        let to_topic = to_topic.clone();
        let message_future = async move {
            queue.send_message(FutureRecord::to(&to_topic).payload(&game).key(&random_game_key())).await;
        };

        let task_future = tokio::spawn(message_future);
        message_join_handles.push_back(task_future);

        while message_join_handles.len() >= 16 {
            message_join_handles.pop_front().unwrap().await.unwrap();
        }

        time_io += (Instant::now() - io_started_at).as_secs_f64();

        let commit_message_started_at = Instant::now();
        consumer.commit_message(&msg, CommitMode::Async).unwrap();
        time_commit_offsets += (Instant::now() - commit_message_started_at).as_secs_f64();

        if progress.update() {
            info!("time_total: {}", time_total.round());
            info!("time_message_recv: {}", time_message_recv.round());
            info!("time_encode_decode: {}", time_encode_decode.round());
            info!("time_io: {}", time_io.round());
            info!("time_commit_offsets: {}", time_commit_offsets.round());
        }

        time_total += (Instant::now() - started_at).as_secs_f64();
    }
}

struct GameVisitor {
    game: ChessGameBuilder,
    errors: Vec<String>,

    white_player: PlayerBuilder,
    black_player: PlayerBuilder,

    date: Option<NaiveDate>,
    used_utc_header_for_date: bool, // there are multiple date fields, we try to prefer utc one

    game_entries: Vec<GameEntry>,
}

impl GameVisitor {
    pub fn new() -> Self {
        Self {
            game: ChessGameBuilder::default(),
            errors: Vec::new(),

            white_player: PlayerBuilder::default(),
            black_player: PlayerBuilder::default(),

            date: None,
            used_utc_header_for_date: false,

            game_entries: Vec::new(),
        }
    }
}

impl Visitor for GameVisitor {
    type Result = Result<ChessGame, Vec<String>>;

    fn header(&mut self, key: &[u8], value: pgn_reader::RawHeader<'_>) {
        let key = String::from_utf8_lossy(key).to_string();
        let value = value.decode_utf8().unwrap();

        match key.as_str() {
            "Event" => {
                self.game.event_name(value.to_string());
            },
            "Site" => {
                self.game.link(value.to_string());
            },
            "Date" => {
                if !self.used_utc_header_for_date {
                    self.date = Some(NaiveDate::parse_from_str(&value, "%Y.%m.%d").unwrap());
                }
            },
            "Round" => {
                if value == "-" {
                    // ignore
                } else {
                    panic!("Unexpected value for round header: {}", value);
                }
            },
            "White" => {
                self.white_player.name(value.to_string());
            },
            "WhiteElo" => {
                self.white_player.elo(value.parse().unwrap());
            },
            "WhiteTitle" => {
                match PlayerTitle::try_from(value.as_ref()) {
                    Ok(title) => {
                        self.white_player.title(Some(title.into()));
                    },
                    Err(err) => self.errors.push(err),
                }
            },
            "Black" => {
                self.black_player.name(value.to_string());
            },
            "BlackElo" => {
                self.black_player.elo(value.parse().unwrap());
            },
            "BlackTitle" => {
                match PlayerTitle::try_from(value.as_ref()) {
                    Ok(title) => {
                        self.black_player.title(Some(title.into()));
                    },
                    Err(err) => self.errors.push(err),
                }
            }
            "Result" => {
                self.game.result(match value.as_ref() {
                    "1-0" => GameResult::WhiteWins,
                    "0-1" => GameResult::BlackWins,
                    "1/2-1/2" => GameResult::Draw,
                    "*" => GameResult::Star,
                    other => panic!("Unexpected result: {}", other),
                }.into());
            },
            "UTCDate" => {
                self.date = Some(NaiveDate::parse_from_str(&value, "%Y.%m.%d").unwrap());
                self.used_utc_header_for_date = true;
            },
            "UTCTime" => {
                if !self.used_utc_header_for_date {
                    panic!("Expected UTCDate header to be parsed before the UTCTime");
                }

                let datetime = NaiveDateTime::new(
                    self.date.as_ref().unwrap().clone(),
                    NaiveTime::parse_from_str(&value, "%H:%M:%S").unwrap()
                );

                self.game.date(Some(Timestamp {
                    seconds: datetime.timestamp(),
                    nanos: 0,
                }));
            },
            "WhiteRatingDiff" => {
                self.game.rating_outcome_for_white(Some(value.parse().unwrap()));
            },
            "BlackRatingDiff" => {
                self.game.rating_outcome_for_black(Some(value.parse().unwrap()));
            },
            "ECO" => {
                self.game.eco(value.into_owned());
            },
            "Opening" => {
                self.game.opening(value.into_owned());
            },
            "TimeControl" => {
                if value == "-" {
                    self.game.timecontrol(None);
                } else if let Some(plus_index) = value.find("+") {
                    let duration = value[0..plus_index].parse().unwrap();
                    let increment = value[plus_index+1..].parse().unwrap();
                    self.game.timecontrol(Some(Timecontrol {
                        duration,
                        increment,
                    }));
                } else {
                    panic!("Expected timecontrol to contain plus or be set to \"-\" (which we interprete as None): {}", value);
                }
            },
            "Termination" => {
                let termination = match value.as_ref() {
                    "Normal" => Some(Termination::Normal),
                    "Time forfeit" => Some(Termination::TimeForefeit),
                    "Abandoned" => Some(Termination::Abandonded),
                    "Unterminated" => Some(Termination::Unterminated),
                    "Rules infraction" => Some(Termination::RulesInfraction),
                    other => {
                        self.errors.push(format!("Unexpected termination: {}", other));
                        None
                    },
                };

                if let Some(termination) = termination {
                    self.game.termination(termination.into());
                }
            }
            other => {
                self.errors.push(format!("Unexpected header: {} = {}", other, value));
            },
        }
    }

    fn san(&mut self, san_plus: SanPlus) {
        let is_check = san_plus.suffix.map(|v| v == Suffix::Check);
        let is_checkmate = san_plus.suffix.map(|v| v == Suffix::Checkmate);

        let san = match san_plus.san {
            pgn_reader::San::Normal { 
                role, 
                file, 
                rank, 
                capture, 
                to, 
                promotion 
            } => Some(San {
                normal: Some(NormalSan {
                    role: Role::from(role).into(),
                    file: file.map(|v| File::from(v).into()),
                    rank: rank.map(|v| Rank::from(v).into()),
                    capture,
                    to: Some(Square::from(to)),
                    promotion: promotion.map(|v| Role::from(v).into()),
                }),
                castle: None,
                put: None,
                is_check,
                is_checkmate,
            }),
            pgn_reader::San::Castle(castling_side) => Some(San {
                normal: None,
                castle: Some(CastleSan {
                    side: CastlingSide::from(castling_side).into(),
                }),
                put: None,
                is_check,
                is_checkmate,
            }),
            pgn_reader::San::Put { role, to } => Some(San {
                normal: None,
                castle: None,
                put: Some(PutSan {
                    role: Role::from(role).into(),
                    to: Some(Square::from(to)),
                }),
                is_check,
                is_checkmate,
            }),
            pgn_reader::San::Null => None,
        };

        self.game_entries.push(GameEntry {
            san,
            nag: None,
            comment: None,
        });
    }

    fn nag(&mut self, nag: pgn_reader::Nag) {
        self.game_entries.push(GameEntry {
            san: None,
            nag: Some(Nag::from(nag).into()),
            comment: None,
        });
    }

    fn comment(&mut self, comment: RawComment<'_>) {
        let mut comment: String = String::from_utf8(comment.as_bytes().to_vec()).unwrap();
        while let Some(comment_begin) = comment.find("[") {
            let comment_end = comment.find("]").unwrap();
            let part = comment[comment_begin+1..comment_end].to_owned();
            comment = comment[comment_end+1..].to_owned();

            let s = part.find(" ").unwrap();
            let key = part[0..s].to_owned();
            let value = part[s+1..].to_owned();

            let mut clock = None;
            let mut eval = None;
            let mut getting_mated_in = None;

            match key.as_ref() {
                "%clk" => {
                    clock = Some(NaiveTime::parse_from_str(&value, "%H:%M:%S").unwrap().num_seconds_from_midnight());
                },
                "%eval" => {
                    if value.starts_with("#") {
                        getting_mated_in = Some(value[1..].parse().unwrap());
                    } else {
                        eval = match value.parse() {
                            Ok(v) => Some(v),
                            Err(err) => {
                                panic!("failed to parse eval value = \"{}\": {:?}, whole part is: \"{}\"", value, err, part);
                            },
                        }
                    }
                },
                other => panic!("Unexpected comment: \"{}\"=\"{}\"", other, value),
            };

            self.game_entries.push(GameEntry {
                san: None,
                nag: None,
                comment: Some(Comment {
                    clock,
                    eval,
                    getting_mated_in,
                }),
            });
        }
    }

    fn end_game(&mut self) -> Self::Result {
        if !self.errors.is_empty() {
            return Err(self.errors.clone());
        }

        self.game.white_player(Some(self.white_player.build().unwrap()));
        self.game.black_player(Some(self.black_player.build().unwrap()));
        self.game.game_entries(self.game_entries.clone());
        self.game.build().map_err(|v| vec![v.to_string()])
    }
}

fn random_game_key() -> Vec<u8> {
    let mut id = [0u8; 12];
    rand::thread_rng().fill(&mut id);
    id.to_vec()
}