# bigdata-chess

Playing with datasets from [lichess database](https://database.lichess.org/) as a part of KPI course

## how will import into hdfs work

- write to object storage
- agent running on the master node downloads file and moves them into hdfs

## queries we need to process

- `select count(*) from chess_games`
- `select id, opening, white_player_elo, avg(white_player_elo) over (partition by opening) from chess_games`
- `select * from chess_games_moves moves join chess_games games on games.id = moves.game_id`

## infrastructure notes

- it seems that redpanda requires >4GB of ram. It hanged when running on 4GB instance.