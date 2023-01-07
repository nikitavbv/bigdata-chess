// TODO: implement agent which downloads file from storage
// after downloading the file, it is uploaded to hdfs
// one both steps are ready, implement some way to tell which files are
// already synced.

use {
    std::{sync::Arc, time::Duration, collections::HashSet},
    tracing::info,
    bigdata_chess_core::storage::Storage,
    tokio::{time::sleep, fs, process::Command},
};

pub async fn hdfs_import_step(storage: Arc<Storage>) {
    info!("running hdfs import step");

    loop {
        let games_in_remote_storage = storage.remote_list_game_data_files().await.unwrap();
        let keys_in_local_storage = load_sync_state().await;
        let mut synced_keys = HashSet::new();

        for game_in_remote_storage in games_in_remote_storage {
            if !keys_in_local_storage.contains(&game_in_remote_storage) {
                synced_keys.insert(game_in_remote_storage.clone());
                
                let file_id = file_name_from_path(&game_in_remote_storage);
                let local_file_path = format!("./{}.csv", file_id);

                info!("syncing game {}", file_id);
                let game = storage.remote_game_data_file(&game_in_remote_storage).await.unwrap();
                fs::write(format!("{}.csv", local_file_path), game).await.unwrap();

                info!("uploading game into hdfs: {}", file_id);
                let mut child = Command::new("hadoop").arg("fs").arg("-put").arg("/tables_data/chess_games").spawn().unwrap();
                let status = child.wait().await.unwrap();
                info!("upload into hdfs finished with status: {}", status);

                fs::remove_file(format!("{}.csv", local_file_path)).await.unwrap();
            }
        }

        // TODO: sync game moves

        info!("sleeping before the next iteration");
        sleep(Duration::from_secs(60 * 60)).await;
    }
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