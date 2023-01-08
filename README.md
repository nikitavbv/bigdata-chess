# bigdata-chess

Playing with datasets from [lichess database](https://database.lichess.org/) as a part of KPI course

## how will import into hdfs work

- [x] write to object storage
- [ ] agent running on the master node downloads file and moves them into hdfs

## queries we need to process

hive workers: 2 -> 4 -> 8 -> 16

- `select count(*) from chess_games` (0.5s/39s on postgres, 51s->46s->49s->42s/147s->86s->47s->34s on hive)
- `select count(*) from (select id, opening, white_player_elo, avg(white_player_elo) over (partition by opening) from chess_games) as t` (192ms on postgres, 15s->15s->17s->17s on hive)
- `select count(*) from (select * from chess_game_moves moves join chess_games games on games.id = moves.game_id) as t;` (82s on postgres, 244s->132s->76s->51s on hive)

## infrastructure notes

- it seems that redpanda requires >4GB of ram. It hanged when running on 4GB instance.