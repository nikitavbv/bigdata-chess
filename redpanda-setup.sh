#!/bin/bash
rpk topic create chess-lichess-data-files -c cleanup.policy=compact -r 1 -p 24
rpk topic create chess-lichess-data-files-synced -r 1 -p 24
rpk topic create chess-lichess-raw-games -r 1 -p 24
rpk topic create chess-lichess-raw-games-blue -r 1 -p 24
rpk topic create chess-games -r 1 -p 24
rpk topic create chess-game-parser-errors -r 1 -p 1
rpk topic create chess-logs -r 1 -p 1