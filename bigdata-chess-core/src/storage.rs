use {
    awsregion::Region,
    s3::{Bucket, creds::Credentials},
    serde::{Serialize, Deserialize},
    crate::config::StorageConfig,
};

pub struct Storage {
    bucket: Bucket,
}

#[derive(Serialize, Deserialize)]
struct LichessDataFileMetadata {
    total_chunks: u64,
}

impl Storage {
    pub fn new(config: &StorageConfig) -> Self {
        Self {
            bucket: Bucket::new(
                "chess-data",
                Region::Custom {
                    region: "garage".to_owned(),
                    endpoint: config.endpoint(),
                },
                credentials(config)
            ).unwrap().with_path_style(),
        }
    }

    pub async fn put_lichess_data_file_metadata(&self, path: String, total_chunks: u64) {
        let metadata = serde_json::to_string(&LichessDataFileMetadata {
            total_chunks,
        }).unwrap();
        self.bucket.put_object(format!("{}/metadata", path), metadata.as_bytes()).await.unwrap();
    }

    pub async fn is_lichess_data_file_metadata_present(&self, path: String) -> bool {
        self.bucket.get_object(format!("{}/metadata", path)).await.is_ok()
    }

    pub async fn upload_lichess_data_file_chunk(&self, path: String, chunk_index: u64, data: &[u8]) {
        self.bucket.put_object(format!("{}/{}", path, chunk_index), data).await.unwrap();
    }

    pub async fn is_lichess_data_file_chunk_present(&self, path: &str, chunk_index: u64) -> bool {
        self.bucket.get_object_range(format!("{}/{}", path, chunk_index), 0, Some(8)).await.is_ok()
    }

    pub async fn get_lichess_data_file_chunk(&self, path: &str, chunk_index: u64) -> Vec<u8> {
        self.bucket.get_object(format!("{}/{}", path, chunk_index)).await.unwrap().to_vec()
    }

    pub async fn put_game_data_file(&self, key: &str, data: Vec<u8>) {
        self.bucket.put_object(format!("game-data/{}", key), &data).await.unwrap();
    }
}

fn credentials(config: &StorageConfig) -> Credentials {
    Credentials::new(Some(config.access_key.as_ref().unwrap()), Some(config.secret_key.as_ref().unwrap()), None, None, None).unwrap()
}