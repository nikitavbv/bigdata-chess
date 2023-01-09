CREATE TABLE chess_games(
    id string,
    event_name string,
    link string,
    game_date int,
    black_player_name string,
    black_player_elo int,
    black_player_title string,
    white_player_name string,
    white_player_elo int,
    white_player_title string,
    result tinyint,
    rating_outcome_for_white int,
    rating_outcome_for_black int,
    eco string,
    opening string,
    timecontrol_duration int,
    timecontrol_increment int,
    termination int
)
PARTITIONED BY(day string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/tables_data/chess_games';