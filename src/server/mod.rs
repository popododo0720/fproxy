use std::sync::{Arc};
use std::error::Error;
use log::{error, info};

use tokio::net::TcpListener;
use tokio::sync::{mpsc};
use num_cpus;

use crate::config::Config;
use crate::metrics::{Metrics};
use crate::buffer::BufferPool;
use crate::session::Session;



pub struct ProxyServer {
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    buffer_pool: Option<Arc<BufferPool>>,
}

impl ProxyServer {
    pub fn new(config: Arc<Config>, metrics: Arc<Metrics>, buffer_pool: Option<Arc<BufferPool>>) -> Self {
        Self {
            config,
            metrics,
            buffer_pool,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
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

                    // 연결이 생성될 때 카운트 증가
                    worker_metrics.total_connection_opened();

                    let session = Session::new(
                        client_stream,
                        client_addr,
                        worker_metrics.clone(),
                        worker_config.clone(),
                        worker_buffer_pool.clone(),
                    );

                    // 에러 처리를 위한 메트릭스 참조
                    let metrics_error = worker_metrics.clone();

                    tokio::spawn(async move {
                        if let Err(e) = session.handle().await {
                            error!("An error occurred while processing the session: {}", e);
                            metrics_error.record_error();
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