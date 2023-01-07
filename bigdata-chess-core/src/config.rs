use {
    std::fs::read_to_string,
    tracing::warn,
    serde::Deserialize,
};

#[derive(Deserialize, Debug)]
pub struct Config {
    pub steps: StepsConfig,
    pub infra: Option<InfraConfig>,
}

#[derive(Deserialize, Debug)]
pub struct StepsConfig {
    pub update_checker: UpdateCheckerStepConfig,
    pub file_downloader: Option<FileDownloaderStepConfig>,
    pub chunk_splitter: Option<ChunkSplitterStepConfig>,
    #[serde(default)]
    pub game_parser: GameParserStepConfig,
    #[serde(default)]
    pub postgres_import: PostgresImportStepConfig,
    #[serde(default)]
    pub storage_import: StorageImportStepConfig,
    #[serde(default)]
    hdfs_import: HdfsImportStepConfig,
}

#[derive(Deserialize, Clone, Debug)]
pub struct UpdateCheckerStepConfig {
    pub enabled: bool,
}

#[derive(Deserialize, Debug)]
pub struct FileDownloaderStepConfig {
    pub enabled: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ChunkSplitterStepConfig {
    pub enabled: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct GameParserStepConfig {
    pub enabled: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct PostgresImportStepConfig {
    pub enabled: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct StorageImportStepConfig {
    pub enabled: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct HdfsImportStepConfig {
    enabled: bool,
    synced_games_files_limit: Option<u32>,
    synced_game_moves_files_limit: Option<u32>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct InfraConfig {
    queue: Option<QueueConfig>,
    storage: Option<StorageConfig>,
    #[serde(default)]
    database: DatabaseConfig,
}

#[derive(Deserialize, Clone, Debug)]
pub struct QueueConfig {
    pub endpoint: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct StorageConfig {
    endpoint: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    remote_api_key: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct DatabaseConfig {
    connection_string: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            steps: StepsConfig::default(),
            infra: None,
        }
    }
}

impl Default for StepsConfig {
    fn default() -> Self {
        Self {
            update_checker: UpdateCheckerStepConfig::default(),
            file_downloader: None,
            chunk_splitter: None,
            game_parser: GameParserStepConfig::default(),
            postgres_import: PostgresImportStepConfig::default(),
            storage_import: StorageImportStepConfig::default(),
            hdfs_import: HdfsImportStepConfig::default(),
        }
    }
}

impl Default for UpdateCheckerStepConfig {
    fn default() -> Self {
        Self {
            enabled: false,
        }
    }
}

impl Default for ChunkSplitterStepConfig {
    fn default() -> Self {
        Self {
            enabled: false,
        }
    }
}

impl Default for GameParserStepConfig {
    fn default() -> Self {
        Self {
            enabled: false,
        }
    }
}

impl Default for PostgresImportStepConfig {
    fn default() -> Self {
        Self {
            enabled: false,
        }
    }
}

impl Default for StorageImportStepConfig {
    fn default() -> Self {
        Self {
            enabled: false,
        }
    }
}

impl Default for HdfsImportStepConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            synced_games_files_limit: None,
            synced_game_moves_files_limit: None,
        }
    }
}

impl HdfsImportStepConfig {
    pub fn synced_games_files_limit(&self) -> Option<&u32> {
        self.synced_games_files_limit.as_ref()
    }

    pub fn synced_game_moves_files_limit(&self) -> Option<&u32> {
        self.synced_game_moves_files_limit.as_ref()
    }
}

impl Default for InfraConfig {
    fn default() -> Self {
        Self {
            queue: Some(QueueConfig::default()),
            storage: Some(StorageConfig::default()),
            database: DatabaseConfig::default(),
        }
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            endpoint: "redpanda.default.svc.cluster.local:9092".to_owned(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            access_key: None,
            secret_key: None,
            remote_api_key: None,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            connection_string: None,
        }
    }
}

impl Config {
    pub fn load() -> Self {
        read_to_string("./config.toml")
            .or_else(|_| read_to_string("/config/config.toml"))
            .map_err(|err| err.to_string())
            .and_then(|v| toml::from_str(&v).map_err(|err| err.to_string()))
            .unwrap_or_else(|err| {
                warn!("failed to read config: {}", err);
                Config::default()
            })
    }

    pub fn infra(&self) -> InfraConfig {
        self.infra.as_ref().cloned().unwrap_or_default()
    }
}

impl StepsConfig {
    pub fn chunk_splitter(&self) -> ChunkSplitterStepConfig {
        self.chunk_splitter.as_ref().cloned().unwrap_or_default()
    }

    pub fn hdfs_import(&self) -> &HdfsImportStepConfig {
        &self.hdfs_import
    }
}

impl InfraConfig {
    pub fn queue(&self) -> QueueConfig {
        self.queue.as_ref().cloned().unwrap_or_default()
    }

    pub fn storage(&self) -> StorageConfig {
        self.storage.as_ref().cloned().unwrap_or_default()
    }

    pub fn database(&self) -> &DatabaseConfig {
        &self.database
    }
}

impl StorageConfig {
    pub fn endpoint(&self) -> String {
        self.endpoint.as_ref().cloned().unwrap_or("http://garage.default.svc.cluster.local:3900".to_owned())
    }

    pub fn access_key(&self) -> Option<&String> {
        self.access_key.as_ref()
    }

    pub fn secret_key(&self) -> Option<&String> {
        self.secret_key.as_ref()
    }

    pub fn remote_api_key(&self) -> Option<&String> {
        self.remote_api_key.as_ref()
    }
}

impl DatabaseConfig {
    pub fn connection_string(&self) -> Option<&String> {
        self.connection_string.as_ref()
    }
}
