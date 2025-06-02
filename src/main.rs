use std::path::Path;
use std::sync::Arc;
use std::io::Write;


use log::{debug, info, warn, error, LevelFilter};
use once_cell::sync::Lazy;
use chrono::Local;
use env_logger::Builder;

mod constants;
mod server;
mod session;
mod tls;
mod proxy;
mod acl;
mod db;
mod logging;
mod error;
mod config;
mod metrics;
mod buffer;

use crate::config::{Config, Settings};
use crate::db::config::DbConfig;
use crate::error::{Result, ProxyError, config_err, db_err};
use crate::acl::domain_blocker::DomainBlocker;
use crate::logging::Logger;
use crate::server::ProxyServer;
use crate::metrics::Metrics;
use crate::buffer::BufferPool;
use crate::constants::*;
use crate::tls::init_root_ca;

// 파일 디스크립터 제한 설정
static FD_LIMIT: Lazy<u64> = Lazy::new(|| {
    std::env::var("FD_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000000) // 기본값 1M
});

#[tokio::main]
async fn main() -> Result<()> {
    // 로거 초기화
    env_logger::init();
    info!("UDSS 프록시 서버 시작");
    
    // 통합 설정 로드
    let mut settings = Settings::new()?;
    
    // 환경 변수에서 설정 오버라이드
    settings.override_from_env()?;
    
    // 설정 정보 로그 출력
    settings.log_settings();
    
    // 인증서 로드
    load_trusted_certificates(&mut settings.proxy)?;
    let config = Arc::new(settings.proxy);
    
    // 데이터베이스 설정 및 초기화 (통합 설정 사용)
    setup_database(&settings.database).await?;

    // SSL 디렉토리 확인 및 생성
    ensure_ssl_directories(&config)?;
    
    // TLS 루트 CA 인증서 초기화
    if let Err(e) = init_root_ca() {
        error!("루트 CA 초기화 실패: {}", e);
    } else {
        info!("루트 CA 초기화 성공");
    }
    
    // 메트릭스 초기화
    let metrics = Metrics::new();
    
    // 버퍼 풀 초기화
    let buffer_pool = Arc::new(create_buffer_pool());
    info!("버퍼 풀 초기화: 소형 {}, 중형 {}, 대형 {}", SMALL_POOL_SIZE, MEDIUM_POOL_SIZE, LARGE_POOL_SIZE);

    // Logger 인스턴스 생성
    let mut logger = Logger::new();
    // 비동기 초기화 수행
    match logger.init().await {
        Ok(_) => info!("로거 초기화 완료"),
        Err(e) => error!("로거 초기화 실패: {}", e)
    }
    // Arc로 감싸서 공유 가능하게 함
    let logger = Arc::new(logger);
    
    // 워커 스레드 설정
    let worker_threads = config.worker_threads.unwrap_or_else(|| num_cpus::get());
    
    // DomainBlocker 인스턴스 생성 (config는 이미 Arc<Config> 타입)
    let domain_blocker = Arc::new(DomainBlocker::new(config.clone()));
    
    // DomainBlocker 초기화 (비동기 초기화 메서드 명시적 호출)
    match domain_blocker.initialize().await {
        Ok(_) => info!("도메인 차단기 초기화 완료"),
        Err(e) => error!("도메인 차단기 초기화 실패: {}", e)
    }

    info!("워커 스레드 수: {}", worker_threads);

    // 프록시 서버 시작
    let server = ProxyServer::new(config, metrics, Some(buffer_pool), logger.clone(), domain_blocker);
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

/// 인증서 로드 함수
fn load_trusted_certificates(config: &mut Config) -> Result<()> {
    info!("신뢰할 수 있는 인증서 로드");
    
    // 인증서 디렉토리 확인
    let cert_dir = Path::new(&config.ssl_dir);
    if !cert_dir.exists() || !cert_dir.is_dir() {
        warn!("인증서 디렉토리가 존재하지 않습니다: {}", config.ssl_dir);
        return Ok(());
    }
    
    // 인증서 파일 로드
    let cert_files = std::fs::read_dir(cert_dir)
        .map_err(|e| config_err(format!("인증서 디렉토리 읽기 실패: {}", e)))?;
    
    for entry in cert_files {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "pem" || ext == "crt") {
                if let Some(path_str) = path.to_str() {
                    config.trusted_certificates.push(path_str.to_string());
                    info!("인증서 로드: {}", path_str);
                }
            }
        }
    }
    
    info!("총 {} 개의 인증서 로드됨", config.trusted_certificates.len());
    Ok(())
}

/// 데이터베이스 설정 및 초기화
async fn setup_database(db_config: &DbConfig) -> Result<()> {
    // DB 설정 글로벌 변수 업데이트
    if let Ok(mut global_config) = db::config::DB_CONFIG.write() {
        *global_config = db_config.clone();
        info!("데이터베이스 설정 업데이트 완료");
    } else {
        error!("데이터베이스 글로벌 설정 업데이트 실패");
        return Err(db_err("데이터베이스 글로벌 설정 업데이트 실패"));
    }
    
    // DB 연결 풀 초기화
    if let Err(e) = db::pool::initialize_pool().await {
        error!("데이터베이스 연결 풀 초기화 실패: {}", e);
        warn!("데이터베이스 연결 없이 계속 진행합니다. 로그가 저장되지 않을 수 있습니다.");
        // 이 경우는 에러가 아닌 정상 처리로 간주
        return Ok(());
    }
    
    // DB 스키마 및 테이블 초기화
    initialize_database().await?;
    
    Ok(())
}

/// 버퍼 풀 생성
fn create_buffer_pool() -> BufferPool {
    BufferPool::new(
        SMALL_POOL_SIZE,  // 64KB
        MEDIUM_POOL_SIZE, // 256KB
        LARGE_POOL_SIZE   // 1MB
    )
}

/// 로깅 시스템 초기화
async fn initialize_logger() -> Result<()> {
    info!("로깅 시스템 초기화 중...");
    
    // 이 함수는 더 이상 사용하지 않지만 호환성을 위해 유지
    
    info!("로깅 시스템 초기화 완료");
    Ok(())
}

/// 데이터베이스 초기화
async fn initialize_database() -> Result<()> {
    // 파티션 관리 확인
    debug!("데이터베이스 파티션 확인 중...");
    match db::ensure_partitions().await {
        Ok(_) => debug!("데이터베이스 파티션 확인 완료"),
        Err(e) => {
            warn!("데이터베이스 파티션 확인 실패: {}. 계속 진행합니다.", e);
            // 파티션 확인 실패해도 계속 진행
        }
    }
    
    Ok(())
}

/// SSL 디렉토리 확인 및 생성
fn ensure_ssl_directories(config: &Config) -> Result<()> {
    let ssl_dir = &config.ssl_dir;
    let cert_dir = format!("{}/certs", ssl_dir);
    let key_dir = format!("{}/private", ssl_dir);
    let trusted_dir = format!("{}/trusted_certs", ssl_dir);
    
    for dir in &[ssl_dir, &cert_dir, &key_dir, &trusted_dir] {
        if !Path::new(dir).exists() {
            std::fs::create_dir_all(dir).map_err(|e| ProxyError::from(e))?;
            info!("디렉토리 생성: {}", dir);
        }
    }
    
    Ok(())
}

