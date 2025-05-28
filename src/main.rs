use std::error::Error;
use std::sync::Arc;
use std::path::Path;
#[cfg(debug_assertions)]
use std::io::Write;
#[cfg(debug_assertions)]
use chrono::Local;

use log::{LevelFilter, info, warn, error, debug};
use env_logger::Builder;
use once_cell::sync::Lazy;

mod config;
mod metrics;
mod buffer;
mod constants;
mod server;
mod session;
mod tls;
mod proxy;
mod acl;
mod db;

use config::Config;
use metrics::Metrics;
use constants::*;
use buffer::BufferPool;
use server::ProxyServer;
use tls::init_root_ca;
use acl::request_logger::RequestLogger;
use db::config::DbConfig;

// 파일 디스크립터 제한 설정
static FD_LIMIT: Lazy<u64> = Lazy::new(|| {
    std::env::var("FD_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100000) // 기본값 100K
});

// 전역 RequestLogger 인스턴스
static REQUEST_LOGGER: Lazy<Arc<tokio::sync::RwLock<RequestLogger>>> = Lazy::new(|| {
    Arc::new(tokio::sync::RwLock::new(RequestLogger::new()))
});

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 로거 초기화
    setup_logger();
    
    // 시스템 리소스 제한 설정
    setup_resource_limits();

    info!("udss-proxy 서버 시작 중...");
    let num_cpus = num_cpus::get();
    info!("시스템 코어 수: {}", num_cpus);

    // 프록시 설정 로드
    let config = load_config()?;
    
    // 데이터베이스 설정 로드 및 초기화
    setup_database().await?;

    // 메트릭스 초기화
    let metrics = Metrics::new();
    
    // 버퍼 풀 초기화
    let buffer_pool = create_buffer_pool();
    info!("버퍼 풀 초기화: 소형 {}, 중형 {}, 대형 {}", SMALL_POOL_SIZE, MEDIUM_POOL_SIZE, LARGE_POOL_SIZE);

    // RequestLogger 초기화
    initialize_request_logger().await?;

    // TLS 루트 CA 인증서 초기화
    if let Err(e) = init_root_ca() {
        error!("루트 CA 초기화 실패: {}", e);
    } else {
        info!("루트 CA 초기화 성공");
    }

    // 워커 스레드 설정
    let worker_threads = config.worker_threads.unwrap_or_else(|| num_cpus);
    info!("워커 스레드 수: {}", worker_threads);

    // 프록시 서버 시작
    let server = ProxyServer::new(config, metrics, Some(buffer_pool));
    server.run().await?;

    Ok(())
}

/// 로거 설정
fn setup_logger() {
    #[cfg(debug_assertions)]
    {
        Builder::new()
            .filter(None, LevelFilter::Trace)
            .format(|buf, record| {
                writeln!(
                    buf,
                    "[{} {} {}:{}] {}",
                    Local::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    record.level(),
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.args()
                )
            })
            .init();
    }

    #[cfg(not(debug_assertions))]
    {
        Builder::new()
            .filter(None, LevelFilter::Info)
            .init();
    }
}

/// 시스템 리소스 제한 설정
fn setup_resource_limits() {
    #[cfg(unix)]
    {
        use nix::sys::resource::{setrlimit, Resource};
        // fd 제한 늘리기
        match setrlimit(Resource::RLIMIT_NOFILE, *FD_LIMIT, *FD_LIMIT) {
            Ok(_) => {
                info!("파일 디스크립터 제한을 {}으로 설정했습니다", *FD_LIMIT);
            },
            Err(e) => {
                warn!("파일 디스크립터 제한 설정 실패: {:?}", e);
            }
        }
    }
}

/// 설정 파일 로드
fn load_config() -> Result<Arc<Config>, Box<dyn Error>> {
    let config = match std::env::var("CONFIG_FILE") {
        Ok(path) => {
            info!("설정 파일 로드: {}", path);
            Arc::new(Config::from_file(&path)?)
        },
        Err(_) => {
            info!("기본 설정 사용");
            Arc::new(Config::new())
        }
    };
    
    Ok(config)
}

/// 데이터베이스 설정 및 초기화
async fn setup_database() -> Result<(), Box<dyn Error>> {
    // DB 설정 로드
    let db_config_path = std::env::var("DB_CONFIG_FILE").unwrap_or_else(|_| "db.yml".to_string());
    if Path::new(&db_config_path).exists() {
        info!("데이터베이스 설정 로드: {}", db_config_path);
        if let Err(e) = DbConfig::initialize(&db_config_path) {
            error!("데이터베이스 설정 로드 실패: {}", e);
            return Err(e);
        }
    } else {
        info!("기본 데이터베이스 설정 사용");
    }
    
    // DB 연결 풀 초기화
    if let Err(e) = db::pool::initialize_pool().await {
        error!("데이터베이스 연결 풀 초기화 실패: {}", e);
        warn!("데이터베이스 연결 없이 계속 진행합니다. 로그가 저장되지 않을 수 있습니다.");
        return Ok(());
    }
    
    // DB 스키마 및 테이블 초기화
    initialize_database().await?;
    
    Ok(())
}

/// 버퍼 풀 생성
fn create_buffer_pool() -> Arc<BufferPool> {
    Arc::new(BufferPool::new(
        SMALL_POOL_SIZE,  // 64KB
        MEDIUM_POOL_SIZE, // 256KB
        LARGE_POOL_SIZE   // 1MB
    ))
}

/// RequestLogger 초기화
async fn initialize_request_logger() -> Result<(), Box<dyn Error>> {
    info!("RequestLogger 초기화 시작...");
    let mut logger = REQUEST_LOGGER.write().await;
    
    match logger.init().await {
        Ok(_) => {
            info!("RequestLogger 초기화 성공");
            Ok(())
        },
        Err(e) => {
            error!("RequestLogger 초기화 실패: {}", e);
            Err(e)
        }
    }
}

/// 데이터베이스 초기화 함수
async fn initialize_database() -> Result<(), Box<dyn Error>> {
    // 파티션 확인 및 생성 - 타임아웃 추가
    info!("데이터베이스 파티션 확인");
    match tokio::time::timeout(
        tokio::time::Duration::from_secs(60), // 60초 타임아웃
        db::ensure_partitions()
    ).await {
        Ok(Ok(_)) => {
            info!("데이터베이스 파티션 확인 완료");
        },
        Ok(Err(e)) => {
            debug!("파티션 확인 실패: {}", e);
        },
        Err(_) => {
            debug!("데이터베이스 파티션 확인 타임아웃, 계속 진행합니다");
        }
    }
    
    Ok(())
}

