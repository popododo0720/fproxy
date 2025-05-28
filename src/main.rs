use std::error::Error;
use std::sync::Arc;

use log::{LevelFilter, info, warn, error};
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

use config::Config;
use metrics::Metrics;
use constants::*;
use buffer::BufferPool;
use server::ProxyServer;
use tls::init_root_ca;
use acl::request_logger::RequestLogger;


static FD_LIMIT: Lazy<u64> = Lazy::new(|| {
    std::env::var("FD_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100000) // 100K
});

// 전역 RequestLogger 인스턴스
static REQUEST_LOGGER: Lazy<Arc<tokio::sync::RwLock<RequestLogger>>> = Lazy::new(|| {
    Arc::new(tokio::sync::RwLock::new(RequestLogger::new()))
});

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 로거 세팅
    #[cfg(debug_assertions)]
    {
        Builder::new()
            .filter(None, LevelFilter::Trace)  // 기본 로그 레벨 설정
            .init();
    }

    #[cfg(not(debug_assertions))]
    {
        Builder::new()
            .filter(None, LevelFilter::Info)  // 기본 로그 레벨 설정
            // .filter_module("fproxy", LevelFilter::Info)  // 모듈 레벨 설정
            .init();
    }

    // 리소스 제한설정
    #[cfg(unix)]
    {
        use nix::sys::resource::{setrlimit, Resource};
        // fd 제한 늘리기
        if let Err(e) = setrlimit(Resource::RLIMIT_NOFILE, *FD_LIMIT, *FD_LIMIT) {
            warn!("failed to set the file descriptor: {:?}", e);
        } else {
            info!("file descriptor set {:?} to nonblock rlimit", *FD_LIMIT);
        }
    }

    info!("starting udss-proxy server");
    let num_cpus = num_cpus::get();
    info!("system core count: {}", num_cpus);

    let config = match std::env::var("CONFIG_FILE") {
        Ok(path) => {
            info!("load setting file from: {}", path);
            Arc::new(Config::from_file(&path)?)
        },
        Err(_) => {
            info!("use default setting");
            Arc::new(Config::new())
        }
    };

    // 메트릭스 - 싱글톤 
    let metrics = Metrics::new();
    
    let buffer_pool = Arc::new(BufferPool::new(
        SMALL_POOL_SIZE,  // 64KB
        MEDIUM_POOL_SIZE, // 256KB
        LARGE_POOL_SIZE   // 1MB
    ));
    info!("initializing buffer pool: small {}, medium {}, large {}", SMALL_POOL_SIZE, MEDIUM_POOL_SIZE, LARGE_POOL_SIZE);

    // RequestLogger 초기화
    {
        let mut logger = REQUEST_LOGGER.write().await;
        if let Err(e) = logger.init().await {
            error!("Failed to initialize RequestLogger: {}", e);
        } else {
            info!("RequestLogger initialized successfully");
        }
    }

    // TLS 루트 CA 인증서 초기화
    if let Err(e) = init_root_ca() {
        error!("Failed to initialize root CA: {}", e);
    } else {
        info!("Root CA initialized successfully");
    }

    let worker_threads = config.worker_threads.unwrap_or_else(|| num_cpus);
    info!("worker threads count: {}", worker_threads);

    let server = ProxyServer::new(config.clone(), metrics, Some(buffer_pool));
    server.run().await?;

    Ok(())
}

