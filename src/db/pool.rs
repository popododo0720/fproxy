use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Config, Pool, Runtime, PoolError};
use tokio_postgres::NoTls;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use log::{debug, error, info, warn};

use super::config::DbConfig;

// 데이터베이스 연결 풀을 전역적으로 관리하는 싱글톤
pub static DB_POOL: Lazy<RwLock<Option<Arc<DatabasePool>>>> = Lazy::new(|| {
    RwLock::new(None)
});

/// 데이터베이스 풀 관리자 구조체
pub struct DatabasePool {
    pool: Pool,
    config: DbConfig,
}

impl DatabasePool {
    /// 새 DatabasePool 인스턴스 생성
    pub fn new(pool: Pool, config: DbConfig) -> Self {
        Self { 
            pool,
            config,
        }
    }
    
    /// 클라이언트 가져오기
    pub async fn get_client(&self) -> Result<deadpool::managed::Object<deadpool_postgres::Manager>, PoolError> {
        self.pool.get().await
    }
    
    /// 설정 가져오기
    pub fn get_config(&self) -> &DbConfig {
        &self.config
    }
    
    /// 풀 상태 확인
    pub fn get_pool_status(&self) -> PoolStatus {
        PoolStatus {
            max_size: self.config.get_max_connections(),
            available: self.pool.status().available,
            size: self.pool.status().size,
        }
    }
    
    /// 풀 관리 작업 실행
    pub async fn maintenance(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 현재 풀 상태 확인
        let status = self.get_pool_status();
        debug!("DB 풀 상태: 사용 가능 {}/{} (최대 {})", 
              status.available, status.size, status.max_size);
        
        // 필요시 추가 최적화 작업 구현
        
        Ok(())
    }
}

/// 풀 상태 구조체
pub struct PoolStatus {
    pub max_size: usize,
    pub available: usize,
    pub size: usize,
}

/// 데이터베이스 연결 풀 생성
pub async fn create_db_pool() -> Result<Arc<DatabasePool>, Box<dyn Error + Send + Sync>> {
    // DB 설정 로드
    let db_config = DbConfig::get()?;
    let conn_config = &db_config.connection;
    
    // deadpool-postgres 설정 생성
    let mut cfg = Config::new();
    cfg.host = Some(conn_config.host.clone());
    cfg.port = Some(conn_config.port);
    cfg.user = Some(conn_config.user.clone());
    cfg.password = Some(conn_config.password.clone());
    cfg.dbname = Some(conn_config.database.clone());
    
    // 최대 연결 수 설정
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(conn_config.max_connections));
    
    // 연결 제한 시간 설정
    let timeout = Duration::from_secs(conn_config.connection_timeout_seconds);
    cfg.connect_timeout = Some(timeout);
    
    // 연결 풀 생성
    debug!("DB 연결 풀 생성 중... (최대 연결: {})", conn_config.max_connections);
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    
    // 연결 테스트
    let client = pool.get().await?;
    client.execute("SELECT 1", &[]).await?;
    info!("DB 연결 풀 생성 완료 및 연결 테스트 성공");
    
    // 풀 관리자 생성
    let pool_manager = Arc::new(DatabasePool::new(pool, db_config.clone()));
    
    // 전역 풀 설정
    let mut global_pool = DB_POOL.write().await;
    *global_pool = Some(Arc::clone(&pool_manager));
    
    Ok(pool_manager)
}

/// 전역 데이터베이스 연결 풀 가져오기
pub async fn get_db_pool() -> Result<Arc<DatabasePool>, Box<dyn Error + Send + Sync>> {
    // 전역 풀이 이미 존재하는지 확인
    if let Some(pool) = DB_POOL.read().await.as_ref() {
        return Ok(Arc::clone(pool));
    }
    
    // 없으면 새로 생성
    create_db_pool().await
}

/// 연결 풀에서 클라이언트 가져오기
pub async fn get_client() -> Result<deadpool::managed::Object<deadpool_postgres::Manager>, PoolError> {
    match DB_POOL.read().await.as_ref() {
        Some(pool) => pool.get_client().await,
        None => {
            error!("DB 연결 풀이 초기화되지 않았습니다");
            Err(PoolError::Closed)
        }
    }
}

/// 연결 풀 초기화 및 테스트
pub async fn initialize_pool() -> Result<(), Box<dyn Error + Send + Sync>> {
    // 연결 풀 생성
    let pool = create_db_pool().await?;
    
    // 연결 테스트
    let client = pool.get_client().await?;
    let result = client.query_one("SELECT version()", &[]).await?;
    let version: String = result.get(0);
    info!("DB 연결 성공: {}", version);
    
    // 주기적인 풀 관리 작업 시작
    let pool_clone = Arc::clone(&pool);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            if let Err(e) = pool_clone.maintenance().await {
                warn!("DB 풀 관리 작업 실패: {}", e);
            }
        }
    });
    
    Ok(())
} 