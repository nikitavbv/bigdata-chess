use {
    awsregion::Region,
    s3::{Bucket, creds::Credentials},
    serde::{Serialize, Deserialize},
    anyhow::{anyhow, Result},
    reqwest::StatusCode,
    crate::config::StorageConfig,
};

pub struct Storage {
    bucket: Bucket,
    remote_api_key: Option<String>,
    client: reqwest::Client,
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
            remote_api_key: config.remote_api_key().cloned(),
            client: reqwest::Client::new(),
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
        self.bucket.put_object(format!("game-data/games/{}", key), &data).await.unwrap();
    }

    pub async fn put_game_moves_data_file(&self, key: &str, data: Vec<u8>) {
        self.bucket.put_object(format!("game-data/moves/{}", key), &data).await.unwrap();
    }

    pub async fn remote_list_game_data_files(&self) -> Result<Vec<String>> {
        let res = self.remote_api_request("http://storage.nikitavbv.com/v1/chess-data/game-data/games").await?;
        Ok(res.json().await.unwrap())
    }

    pub async fn remote_list_game_moves_files(&self) -> Result<Vec<String>> {
        let res = self.remote_api_request("http://storage.nikitavbv.com/v1/chess-data/game-data/moves").await?;
        Ok(res.json().await.unwrap())
    }

    pub async fn remote_game_data_file(&self, key: &str) -> Result<Vec<u8>> {
        let res = self.remote_api_request(&format!("http://storage.nikitavbv.com/v1/chess-data/{}", key)).await?;
        Ok(res.bytes().await.unwrap().to_vec())
    }

    async fn remote_api_request(&self, url: &str) -> Result<reqwest::Response> {
        let res = self.client.get(url)
            .header("Authorization", self.authorization_header_value_for_remote_api())
            .send()
            .await
            .unwrap();

        if res.status() != StatusCode::OK {
            return Err(anyhow!("remote storage api returned status: {}", res.status().as_u16()));
        }

        Ok(res)
    }

    fn authorization_header_value_for_remote_api(&self) -> String {
        format!("Bearer {}", self.remote_api_key.as_ref().unwrap())
    }
}

fn credentials(config: &StorageConfig) -> Credentials {
    Credentials::new(Some(config.access_key().unwrap()), Some(config.secret_key().unwrap()), None, None, None).unwrap()
}