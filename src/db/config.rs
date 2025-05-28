use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use log::{info, error, warn};
use once_cell::sync::Lazy;
use std::sync::RwLock;

// 데이터베이스 설정을 전역적으로 관리하는 싱글톤
pub static DB_CONFIG: Lazy<RwLock<DbConfig>> = Lazy::new(|| {
    let config = DbConfig::default();
    
    // 기본 설정 파일 경로에서 로드 시도
    if let Ok(config_from_file) = DbConfig::load_from_file("db.yml") {
        RwLock::new(config_from_file)
    } else {
        warn!("db.yml 설정 파일을 찾을 수 없거나 로드할 수 없습니다. 기본 설정을 사용합니다.");
        RwLock::new(config)
    }
});

/// 데이터베이스 연결 설정
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub sslmode: String,
    #[serde(default = "default_connection_pool_size")]
    pub max_connections: usize,
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_seconds: u64,
}

fn default_connection_pool_size() -> usize {
    20
}

fn default_connection_timeout() -> u64 {
    30
}

/// 파티션 설정
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionConfig {
    pub creation_interval: u32,
    pub retention_period: u32,
    pub future_partitions: u32,
}

/// 데이터베이스 설정
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbConfig {
    pub connection: ConnectionConfig,
    pub partitioning: PartitionConfig,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "alicedb".to_string(),
                user: "dbadmin".to_string(),
                password: "dbadminpass".to_string(),
                sslmode: "prefer".to_string(),
                max_connections: default_connection_pool_size(),
                connection_timeout_seconds: default_connection_timeout(),
            },
            partitioning: PartitionConfig {
                creation_interval: 30,
                retention_period: 365,
                future_partitions: 2,
            },
        }
    }
}

impl DbConfig {
    /// 설정 파일에서 DB 설정 로드
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
        let path = path.as_ref();
        info!("DB 설정 파일 로드: {}", path.display());
        
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        let config: DbConfig = serde_yaml::from_str(&contents)?;
        info!("DB 설정 로드 완료: {}:{}/{}", config.connection.host, config.connection.port, config.connection.database);
        
        Ok(config)
    }
    
    /// 전역 설정 초기화
    pub fn initialize<P: AsRef<Path>>(path: P) -> Result<(), Box<dyn Error>> {
        match Self::load_from_file(path) {
            Ok(config) => {
                if let Ok(mut global_config) = DB_CONFIG.write() {
                    *global_config = config;
                    Ok(())
                } else {
                    error!("DB 설정 글로벌 변수 잠금 획득 실패");
                    Err("DB 설정 글로벌 변수 잠금 획득 실패".into())
                }
            },
            Err(e) => {
                error!("DB 설정 파일 로드 실패: {}", e);
                Err(e)
            }
        }
    }
    
    /// 전역 설정 가져오기
    pub fn get() -> Result<Self, Box<dyn Error + Send + Sync>> {
        if let Ok(config) = DB_CONFIG.read() {
            Ok(config.clone())
        } else {
            error!("DB 설정 글로벌 변수 읽기 잠금 획득 실패");
            let err: Box<dyn Error + Send + Sync> = "DB 설정 글로벌 변수 읽기 잠금 획득 실패".into();
            Err(err)
        }
    }

    /// 연결 문자열 생성
    pub fn get_connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={} sslmode={}",
            self.connection.host,
            self.connection.port,
            self.connection.database,
            self.connection.user,
            self.connection.password,
            self.connection.sslmode
        )
    }

    /// 연결 제한 시간 설정 가져오기
    pub fn get_connection_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.connection.connection_timeout_seconds)
    }
    
    /// 연결 풀 최대 크기 가져오기
    pub fn get_max_connections(&self) -> usize {
        self.connection.max_connections
    }
} 