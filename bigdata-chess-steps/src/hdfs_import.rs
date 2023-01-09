use {
    std::{sync::Arc, time::Duration, collections::HashSet},
    tracing::info,
    bigdata_chess_core::storage::Storage,
    tokio::{time::sleep, fs, process::Command},
    bigdata_chess_core::config::HdfsImportStepConfig,
};

pub async fn hdfs_import_step(config: &HdfsImportStepConfig, storage: Arc<Storage>) {
    info!("running hdfs import step");

    loop {
        let keys_in_local_storage = load_sync_state().await;
        let mut synced_keys = HashSet::new();

        let games_in_remote_storage = storage.remote_list_game_data_files().await.unwrap();
        let mut total_games_synced = 0;
        for game_in_remote_storage in games_in_remote_storage {
            total_games_synced += 1;
            if let Some(limit) = config.synced_games_files_limit() {
                if total_games_synced > *limit {
                    info!("reached limit of total synced games");
                    break;
                }
            }
            
            if !keys_in_local_storage.contains(&game_in_remote_storage) {
                synced_keys.insert(game_in_remote_storage.clone());
                
                let file_id = file_name_from_path(&game_in_remote_storage);

                info!("syncing game {}", file_id);
                let game = storage.remote_game_data_file(&game_in_remote_storage).await.unwrap();
            
                import_data_into_table(&file_id, game, "chess_games").await;

                let mut all_keys = keys_in_local_storage.clone();
                all_keys.extend(synced_keys.clone().into_iter());
                save_sync_state(&all_keys).await;
            } else {
                info!("game already synced: {}", game_in_remote_storage);
            }

            info!("total games synced: {}", total_games_synced);
        }

        let game_moves_in_remote_storage = storage.remote_list_game_moves_files().await.unwrap();
        let mut total_game_moves_synced = 0;
        for game_move_in_remote_storage in game_moves_in_remote_storage {
            total_game_moves_synced += 1;
            if let Some(limit) = config.synced_game_moves_files_limit() {
                if total_game_moves_synced > *limit {
                    info!("reached limit of total synced moves");
                    break;
                }
            }

            if !keys_in_local_storage.contains(&game_move_in_remote_storage) {
                synced_keys.insert(game_move_in_remote_storage.clone());

                let file_id = file_name_from_path(&game_move_in_remote_storage);

                info!("syncing game moves {}", file_id);
                let game = storage.remote_game_data_file(&game_move_in_remote_storage).await.unwrap();

                import_data_into_table(&file_id, game, "chess_game_moves").await;

                let mut all_keys = keys_in_local_storage.clone();
                all_keys.extend(synced_keys.clone().into_iter());
                save_sync_state(&all_keys).await;
            } else {
                info!("game moves already synced: {}", game_move_in_remote_storage);
            }

            info!("total game moves synced: {}", total_game_moves_synced);
        }

        let game_eval_comments_in_remote_storage = storage.remote_list_game_comment_eval_files().await.unwrap();
        for eval_comment in game_eval_comments_in_remote_storage {
            if !keys_in_local_storage.contains(&eval_comment) {
                synced_keys.insert(eval_comment.clone());

                let file_id = file_name_from_path(&eval_comment);

                info!("syncing eval comments {}", file_id);
                let eval_comment_data = storage.remote_game_data_file(&eval_comment).await.unwrap();

                import_data_into_table(&file_id, eval_comment_data, "chess_game_comments_eval").await;
                
                let mut all_keys = keys_in_local_storage.clone();
                all_keys.extend(synced_keys.clone().into_iter());
                save_sync_state(&all_keys).await;
            } else {
                info!("game eval comments already synced: {}", eval_comment);
            }
        }

        info!("sleeping before the next iteration");
        sleep(Duration::from_secs(60 * 60)).await;
    }
}

async fn import_data_into_table(file_id: &str, data: Vec<u8>, table_name: &str) {
    hdfs_put_bytes(file_id, data).await;
    hive_load_into_table(file_id, table_name).await;
    hdfs_rm(file_id).await;
}

async fn hdfs_put_bytes(file_id: &str, data: Vec<u8>) {
    let local_file_path = format!("./{}.csv", file_id);

    fs::write(&local_file_path, data).await.unwrap();

    info!("uploading into hdfs: {}", file_id);

    hdfs_put(&local_file_path, &file_id).await;
    fs::remove_file(local_file_path).await.unwrap();
}

async fn hive_load_into_table(file_id: &str, table_name: &str) {
    let mut child = Command::new("hive")
        .arg("-e")
        .arg(format!("load data inpath '{}' into table {}", file_id, table_name))
        .spawn()
        .unwrap();
    let status = child.wait().await.unwrap();
    info!("loaded data into table with status: {}", status);
}

async fn hdfs_rm(file_id: &str) {
    let mut child = Command::new("hadoop")
        .arg("fs")
        .arg("-rm")
        .arg(format!("/tables_data/import_data/{}.csv", file_id))
        .spawn()
        .unwrap();
    let status = child.wait().await.unwrap();
    info!("removed file from hdfs import data with status: {}", status);
}

async fn hdfs_put(local_file_path: &str, file_id: &str) {
    let mut child = Command::new("hadoop")
        .arg("fs")
        .arg("-put")
        .arg(local_file_path)
        .arg(format!("/tables_data/import_data/{}.csv", file_id))
        .spawn()
        .unwrap();
    let status = child.wait().await.unwrap();
    info!("upload into hdfs finished with status: {}", status);
}

fn file_name_from_path(path: &str) -> String {
    let slash = path.rfind("/").unwrap();
    path[slash+1..].to_string()
}

async fn load_sync_state() -> HashSet<String> {
    fs::read("chess_hdfs_sync_state.json").await
        .ok()
        .and_then(|v| serde_json::from_slice(&v).unwrap())
        .unwrap_or(HashSet::new())
}

async fn save_sync_state(state: &HashSet<String>) {
    fs::write("chess_hdfs_sync_state.json", serde_json::to_vec(state).unwrap()).await.unwrap();
}