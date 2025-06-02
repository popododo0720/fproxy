use std::error::Error;
use std::sync::Arc;
use log::{debug, error, info, warn};
use tokio::sync::{mpsc::{self, Sender, Receiver}, Semaphore};
use tokio::time::{Duration, interval, timeout};

use crate::constants::{
    LOG_WORKER_COUNT, LOG_FLUSH_INTERVAL_MS, LOG_PROCESSING_TIMEOUT_MS,
    LOG_HIGH_PRIORITY_QUEUE_SIZE, LOG_MEDIUM_PRIORITY_QUEUE_SIZE, LOG_LOW_PRIORITY_QUEUE_SIZE
};
use crate::logging::message::{LogMessage, LogPriority};
use crate::logging::storage::LogStorage;

/// 로그 워커 풀 - 로그 처리 담당
pub struct WorkerPool {
    // 로그 메시지 우선순위별 송신 채널
    high_priority_sender: Sender<LogMessage>,
    medium_priority_sender: Sender<LogMessage>,
    low_priority_sender: Sender<LogMessage>,
    
    // 로그 저장소
    storage: Arc<LogStorage>,
    
    // 동시 처리 제한 세마포어
    semaphore: Arc<Semaphore>,
}

impl WorkerPool {
    /// 새 워커 풀 인스턴스 생성
    pub async fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
        debug!("WorkerPool 초기화 시작...");
        
        // 로그 저장소 생성
        let mut storage = LogStorage::new();
        
        // 로그 저장소 초기화
        storage.init().await?;
        let storage = Arc::new(storage);
        
        // 채널 생성 - 우선순위별 다른 크기
        let (high_tx, high_rx) = mpsc::channel::<LogMessage>(LOG_HIGH_PRIORITY_QUEUE_SIZE);
        let (medium_tx, medium_rx) = mpsc::channel::<LogMessage>(LOG_MEDIUM_PRIORITY_QUEUE_SIZE);
        let (low_tx, low_rx) = mpsc::channel::<LogMessage>(LOG_LOW_PRIORITY_QUEUE_SIZE);
        
        // 세마포어 생성 - 동시 처리 제한
        let semaphore = Arc::new(Semaphore::new(LOG_WORKER_COUNT));
        
        // 워커 풀 생성
        let pool = Self {
            high_priority_sender: high_tx,
            medium_priority_sender: medium_tx,
            low_priority_sender: low_tx,
            storage: storage.clone(),
            semaphore: semaphore.clone(),
        };
        
        // 우선순위별 워커 스레드 생성
        Self::spawn_priority_workers(high_rx, medium_rx, low_rx, storage.clone(), semaphore.clone()).await;
        
        // 주기적 플러시 태스크 생성
        let flush_high_sender = pool.high_priority_sender.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(LOG_FLUSH_INTERVAL_MS));
            
            loop {
                interval.tick().await;
                
                // 플러시 메시지 전송
                if let Err(e) = flush_high_sender.send(LogMessage::FlushLogs).await {
                    error!("로그 플러시 메시지 전송 실패: {}", e);
                    break;
                }
            }
        });
        
        info!("WorkerPool 초기화 완료 - 워커 스레드 수: {}", LOG_WORKER_COUNT);
        Ok(pool)
    }
    
    /// 우선순위별 워커 스레드 생성
    async fn spawn_priority_workers(
        high_rx: Receiver<LogMessage>,
        medium_rx: Receiver<LogMessage>,
        low_rx: Receiver<LogMessage>,
        storage: Arc<LogStorage>,
        semaphore: Arc<Semaphore>
    ) {
        // 고우선순위 워커 스레드 생성 (우선순위에 따라 더 많은 자원 할당)
        let high_storage = storage.clone();
        let high_semaphore = semaphore.clone();
        tokio::spawn(async move {
            Self::priority_worker_task("고우선순위", high_rx, high_storage, high_semaphore).await;
        });
        
        // 중간우선순위 워커 스레드 생성
        let medium_storage = storage.clone();
        let medium_semaphore = semaphore.clone();
        tokio::spawn(async move {
            Self::priority_worker_task("중간우선순위", medium_rx, medium_storage, medium_semaphore).await;
        });
        
        // 저우선순위 워커 스레드 생성
        tokio::spawn(async move {
            Self::priority_worker_task("저우선순위", low_rx, storage, semaphore).await;
        });
    }
    
    /// 우선순위별 워커 태스크
    async fn priority_worker_task(
        name: &'static str,
        mut rx: Receiver<LogMessage>,
        storage: Arc<LogStorage>,
        semaphore: Arc<Semaphore>
    ) {
        info!("{} 워커 시작", name);
        
        while let Some(message) = rx.recv().await {
            match &message {
                LogMessage::FlushLogs => {
                    // 로그 플러시는 세마포어 필요 없이 바로 처리
                    debug!("{} 워커: 로그 플러시 요청 처리", name);
                    Self::process_flush_logs(&storage).await;
                },
                _ => {
                    // 메시지 복제 및 필요한 값들 이동
                    let message_clone = message.clone();
                    let worker_storage = storage.clone();
                    let worker_name = name;
                    let worker_semaphore = semaphore.clone();
                    
                    // 워커 스레드로 처리 위임
                    tokio::spawn(async move {
                        // 세마포어 획득
                        let permit = match worker_semaphore.acquire().await {
                            Ok(permit) => permit,
                            Err(e) => {
                                error!("{} 워커: 세마포어 획득 실패: {}", worker_name, e);
                                return;
                            }
                        };
                        
                        // 타임아웃 설정하여 메시지 처리
                        let result = timeout(
                            Duration::from_millis(LOG_PROCESSING_TIMEOUT_MS),
                            Self::process_log_message(message_clone, &worker_storage)
                        ).await;
                        
                        if let Err(_) = result {
                            warn!("{} 워커: 로그 처리 타임아웃", worker_name);
                        } else if let Err(e) = result.unwrap() {
                            error!("{} 워커: 로그 처리 실패: {}", worker_name, e);
                        }
                        
                        // 세마포어 자동 반환 (drop)
                        drop(permit);
                    });
                }
            }
        }
        
        info!("{} 워커 종료", name);
    }
    
    /// 로그 메시지 처리
    async fn process_log_message(
        message: LogMessage,
        storage: &LogStorage
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match message {
            LogMessage::RequestLog { 
                host, method, path, header, body, timestamp, 
                session_id, client_ip, target_ip, is_rejected, is_tls, ..
            } => {
                // 요청 로그 처리
                storage.add_request_log(
                    host, method, path, header, body, timestamp, 
                    session_id, client_ip, target_ip, is_rejected, is_tls
                )?;
                
                // 배치 크기 확인 및 플러시
                if storage.should_flush_request_logs() {
                    storage.flush_request_logs().await?;
                }
                
                Ok(())
            },
            
            LogMessage::ResponseLog { 
                session_id, status_code, response_time, response_size, 
                timestamp, headers, body_preview, ..
            } => {
                // 응답 로그 처리
                storage.add_response_log(
                    session_id.clone(), status_code, response_time, response_size, 
                    timestamp, headers, body_preview
                )?;
                
                // 배치 크기 확인 및 플러시
                if storage.should_flush_response_logs() {
                    storage.flush_response_logs().await?;
                }
                
                Ok(())
            },
            
            _ => Ok(()),
        }
    }
    
    /// 로그 플러시 처리
    async fn process_flush_logs(storage: &LogStorage) {
        // 요청 로그 플러시
        if let Err(e) = storage.flush_request_logs().await {
            error!("요청 로그 플러시 실패: {}", e);
        }
        
        // 응답 로그 플러시
        if let Err(e) = storage.flush_response_logs().await {
            error!("응답 로그 플러시 실패: {}", e);
        }
    }
    
    /// 적절한 우선순위 채널로 로그 메시지 전송
    pub async fn send_log(&self, message: LogMessage) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 메시지 타입 및 우선순위에 따라 적절한 채널로 전송
        match &message {
            LogMessage::RequestLog { priority, .. } => {
                match priority {
                    LogPriority::High => {
                        if let Err(e) = self.high_priority_sender.send(message).await {
                            return Err(format!("고우선순위 로그 메시지 전송 실패: {}", e).into());
                        }
                    },
                    LogPriority::Medium => {
                        if let Err(e) = self.medium_priority_sender.send(message).await {
                            return Err(format!("중간우선순위 로그 메시지 전송 실패: {}", e).into());
                        }
                    },
                    LogPriority::Low => {
                        if let Err(e) = self.low_priority_sender.send(message).await {
                            return Err(format!("저우선순위 로그 메시지 전송 실패: {}", e).into());
                        }
                    },
                }
            },
            LogMessage::ResponseLog { priority, .. } => {
                match priority {
                    LogPriority::High => {
                        if let Err(e) = self.high_priority_sender.send(message).await {
                            return Err(format!("고우선순위 로그 메시지 전송 실패: {}", e).into());
                        }
                    },
                    LogPriority::Medium => {
                        if let Err(e) = self.medium_priority_sender.send(message).await {
                            return Err(format!("중간우선순위 로그 메시지 전송 실패: {}", e).into());
                        }
                    },
                    LogPriority::Low => {
                        if let Err(e) = self.low_priority_sender.send(message).await {
                            return Err(format!("저우선순위 로그 메시지 전송 실패: {}", e).into());
                        }
                    },
                }
            },
            LogMessage::FlushLogs => {
                // 플러시 명령은 항상 고우선순위로 처리
                if let Err(e) = self.high_priority_sender.send(message).await {
                    return Err(format!("플러시 메시지 전송 실패: {}", e).into());
                }
            }
        }
        
        Ok(())
    }
    
    /// 로그 플러시 요청
    pub async fn flush(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 플러시 메시지 전송
        if let Err(e) = self.high_priority_sender.send(LogMessage::FlushLogs).await {
            return Err(format!("플러시 메시지 전송 실패: {}", e).into());
        }
        
        // 플러시 처리를 위한 짧은 지연
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        Ok(())
    }
} 