use std::sync::{Arc};
use log::{error, info};

use tokio::net::TcpListener;
use tokio::sync::{mpsc};
use num_cpus;

use crate::config::Config;
use crate::metrics::{Metrics};
use crate::buffer::BufferPool;
use crate::session::Session;
use crate::logging::Logger;
use crate::acl::domain_blocker::DomainBlocker;
use crate::error::{ProxyError, Result, internal_err};

pub struct ProxyServer {
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    buffer_pool: Option<Arc<BufferPool>>,
    logger: Arc<Logger>,
    domain_blocker: Arc<DomainBlocker>,
}

impl ProxyServer {
    pub fn new(config: Arc<Config>, metrics: Arc<Metrics>, buffer_pool: Option<Arc<BufferPool>>, logger: Arc<Logger>, domain_blocker: Arc<DomainBlocker>) -> Self {
        Self {
            config,
            metrics,
            buffer_pool,
            logger,
            domain_blocker,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.bind_host, self.config.bind_port);
        let listener = TcpListener::bind(&addr).await?;

        info!("proxy server start at: {}", addr);

        let worker_count = num_cpus::get();

        let (tx, rx) = mpsc::channel(1000);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        for worker_id in 0..worker_count {
            let worker_rx = rx.clone();
            let worker_metrics = self.metrics.clone();
            let worker_config = self.config.clone();
            let worker_buffer_pool = self.buffer_pool.clone();
            let worker_logger = self.logger.clone();
            let worker_domain_blocker = self.domain_blocker.clone();

            tokio::spawn(async move {
                info!("worker #{} start", worker_id);

                loop {
                    let (client_stream, client_addr) = {
                        let mut rx_guard = worker_rx.lock().await;
                        match rx_guard.recv().await {
                            Some(conn) => conn,
                            None => break,
                        }
                    };

                    let session = Session::new(
                        client_stream,
                        client_addr,
                        worker_metrics.clone(),
                        worker_config.clone(),
                        worker_buffer_pool.clone(),
                        worker_logger.clone(),
                        worker_domain_blocker.clone(),
                    );

                    tokio::spawn(async move {
                        if let Err(e) = session.handle().await {
                            error!("An error occurred while processing the session: {}", e);
                        }
                    });
                }
            });
        }

        // 연결 수락 및 워커에게 분배
        loop {
            match listener.accept().await {
                Ok((client_stream, client_addr)) => {
                    if let Err(e) = tx.send((client_stream, client_addr)).await {
                        error!("can't send session to rx: {}", e);
                    }
                }
                Err(e) => {
                    error!("can't accept from listener: {}", e);
                }
            }
        }
    }
}