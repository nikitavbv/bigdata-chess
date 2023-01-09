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

create table chess_game_moves(
    game_id string,
    move_id int,
    from_file tinyint,
    from_rank tinyint,
    to_file tinyint,
    to_rank tinyint,
    capture boolean,
    promotion tinyint,
    is_check boolean,
    is_checkmate boolean
)
clustered by (game_id) into 24 buckets
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/tables_data/chess_game_moves';

create table chess_game_comments_eval(
    game_id string,
    move_id int,
    eval float
)
clustered by (game_id) into 24 buckets
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/tables_data/chess_game_comments_eval';