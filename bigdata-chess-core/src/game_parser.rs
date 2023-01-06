use {
    std::{sync::Arc, time::Instant},
    tracing::{info, error},
    rdkafka::{consumer::{Consumer, CommitMode}, Message, producer::FutureRecord},
    prost::Message as ProstMessage,
    prost_types::Timestamp,
    pgn_reader::{BufferedReader, Visitor, SanPlus, RawComment},
    shakmaty::san::Suffix,
    chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike},
    rand::Rng,
    crate::{
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
    },
};

async fn game_parser_step(queue: Arc<Queue>) -> std::io::Result<()> {
    info!("running game parser step");

    let consumer = queue.consumer("bigdata-chess-game-parser");
    consumer.subscribe(&vec![TOPIC_LICHESS_RAW_GAMES]).unwrap();

    let mut total_games_processed = 0;
    let started_at = Instant::now();
    let mut report_time = Instant::now();

    loop {
        let msg = consumer.recv().await.unwrap();
        let payload = msg.payload().unwrap();

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

        queue.send_message(FutureRecord::to(TOPIC_CHESS_GAMES).payload(&game).key(&random_game_key())).await;
        consumer.commit_message(&msg, CommitMode::Sync).unwrap();

        total_games_processed += 1;
        let now = Instant::now();
        if (now - report_time).as_millis() > 1000 {
            let seconds_since_start = (now - started_at).as_secs();
            report_time = now;
            info!("total games processed: {} (avg. {} games per sec)", total_games_processed, total_games_processed / seconds_since_start);
        }
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