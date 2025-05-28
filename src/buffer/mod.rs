use std::sync::RwLock;
use std::time::{Instant, Duration};
use std::sync::Arc;
use std::thread;

use bytes::{BytesMut};
use log::{info, debug, warn};
use tokio::sync::mpsc;
use rayon::prelude::*;

use crate::constants::*;

/// 버퍼 크기 분류
#[derive(Debug, Copy, Clone, PartialEq)]
enum BufferSize {
    Small,
    Medium,
    Large,
}

impl BufferSize {
    /// 버퍼 크기 결정
    fn from_size(size: usize) -> Self {
        if size <= BUFFER_SIZE_SMALL {
            BufferSize::Small
        } else if size <= BUFFER_SIZE_MEDIUM {
            BufferSize::Medium
        } else {
            BufferSize::Large
        }
    }
    
    /// 버퍼 크기 반환
    fn capacity(&self) -> usize {
        match self {
            BufferSize::Small => BUFFER_SIZE_SMALL,
            BufferSize::Medium => BUFFER_SIZE_MEDIUM,
            BufferSize::Large => BUFFER_SIZE_LARGE,
        }
    }
}

/// 버퍼 풀 통계 정보
#[derive(Debug, Clone)]
struct BufferStats {
    allocations: usize,
    reuses: usize,
    returns: usize,
    last_metrics_time: Instant,
    last_adjustment_time: Instant,
    small_pool_size: usize,
    medium_pool_size: usize,
    large_pool_size: usize,
    small_usage_rate: f64,
    medium_usage_rate: f64,
    large_usage_rate: f64,
}

impl BufferStats {
    /// 새 버퍼 통계 생성
    fn new(small_pool_size: usize, medium_pool_size: usize, large_pool_size: usize) -> Self {
        Self {
            allocations: 0,
            reuses: 0,
            returns: 0,
            last_metrics_time: Instant::now(),
            last_adjustment_time: Instant::now(),
            small_pool_size,
            medium_pool_size,
            large_pool_size,
            small_usage_rate: 0.0,
            medium_usage_rate: 0.0,
            large_usage_rate: 0.0,
        }
    }
    
    /// 할당 카운트 증가
    fn increment_allocations(&mut self) {
        self.allocations += 1;
    }
    
    /// 재사용 카운트 증가
    fn increment_reuses(&mut self) {
        self.reuses += 1;
    }
    
    /// 반환 카운트 증가
    fn increment_returns(&mut self) {
        self.returns += 1;
    }
    
    /// 통계 출력이 필요한지 확인
    fn should_print_metrics(&self) -> bool {
        Instant::now().duration_since(self.last_metrics_time).as_secs() > BUFFER_STATS_INTERVAL_SECS
    }
    
    /// 통계 출력 시간 업데이트
    fn update_metrics_time(&mut self) {
        self.last_metrics_time = Instant::now();
    }
    
    /// 버퍼 풀 조정이 필요한지 확인
    fn should_adjust_pools(&self) -> bool {
        Instant::now().duration_since(self.last_adjustment_time).as_secs() > BUFFER_ADJUSTMENT_INTERVAL_SECS
    }
    
    /// 버퍼 풀 조정 시간 업데이트
    fn update_adjustment_time(&mut self) {
        self.last_adjustment_time = Instant::now();
    }
    
    /// 사용률 업데이트
    fn update_usage_rates(&mut self, small_available: usize, medium_available: usize, large_available: usize) {
        self.small_usage_rate = 1.0 - (small_available as f64 / self.small_pool_size as f64);
        self.medium_usage_rate = 1.0 - (medium_available as f64 / self.medium_pool_size as f64);
        self.large_usage_rate = 1.0 - (large_available as f64 / self.large_pool_size as f64);
    }
    
    /// 통계 출력
    fn print_metrics(&self) {
        info!(
            "Buffer stats - Allocations: {}, Reuses: {}, Returns: {}",
            self.allocations,
            self.reuses,
            self.returns
        );
        
        info!(
            "Buffer pools - Small: {} ({:.1}% used), Medium: {} ({:.1}% used), Large: {} ({:.1}% used)",
            self.small_pool_size,
            self.small_usage_rate * 100.0,
            self.medium_pool_size,
            self.medium_usage_rate * 100.0,
            self.large_pool_size,
            self.large_usage_rate * 100.0
        );
    }
    
    /// 풀 크기 업데이트
    fn update_pool_sizes(&mut self, small: usize, medium: usize, large: usize) {
        self.small_pool_size = small;
        self.medium_pool_size = medium;
        self.large_pool_size = large;
    }
}

/// 버퍼 풀 조정 명령
enum BufferPoolCommand {
    Adjust,
    Shutdown,
}

pub struct BufferPool {
    small_buffers: RwLock<Vec<BytesMut>>,
    medium_buffers: RwLock<Vec<BytesMut>>,
    large_buffers: RwLock<Vec<BytesMut>>,
    stats: RwLock<BufferStats>,
    adjustment_tx: mpsc::Sender<BufferPoolCommand>,
}

impl BufferPool {
    pub fn new(small_capacity: usize, medium_capacity: usize, large_capacity: usize) -> Self {
        let small_buffers = Self::create_buffer_vec(small_capacity, BUFFER_SIZE_SMALL);
        let medium_buffers = Self::create_buffer_vec(medium_capacity, BUFFER_SIZE_MEDIUM);
        let large_buffers = Self::create_buffer_vec(large_capacity, BUFFER_SIZE_LARGE);
        
        let stats = BufferStats::new(small_capacity, medium_capacity, large_capacity);
        
        // 버퍼 풀 조정을 위한 채널 생성
        let (tx, rx) = mpsc::channel::<BufferPoolCommand>(100);
        
        let pool = Self {
            small_buffers: RwLock::new(small_buffers),
            medium_buffers: RwLock::new(medium_buffers),
            large_buffers: RwLock::new(large_buffers),
            stats: RwLock::new(stats),
            adjustment_tx: tx,
        };
        
        // 적응형 버퍼 풀 조정 스레드 시작
        let pool_clone = Arc::new(pool.clone());
        thread::spawn(move || {
            Self::adjustment_loop(pool_clone, rx);
        });
        
        info!("버퍼 풀 초기화 완료 - 소형: {}, 중형: {}, 대형: {}", small_capacity, medium_capacity, large_capacity);
        pool
    }
    
    /// 버퍼 풀 조정 루프
    fn adjustment_loop(pool: Arc<BufferPool>, mut rx: mpsc::Receiver<BufferPoolCommand>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
            
        runtime.block_on(async {
            loop {
                // 일정 시간마다 조정 명령 전송
                tokio::time::sleep(Duration::from_secs(BUFFER_ADJUSTMENT_INTERVAL_SECS)).await;
                
                // 조정 명령 전송
                match pool.adjustment_tx.send(BufferPoolCommand::Adjust).await {
                    Ok(_) => {},
                    Err(e) => {
                        warn!("버퍼 풀 조정 명령 전송 실패: {}", e);
                        break;
                    }
                }
                
                // 명령 수신 및 처리
                match rx.recv().await {
                    Some(BufferPoolCommand::Adjust) => {
                        pool.adjust_buffer_pools();
                    },
                    Some(BufferPoolCommand::Shutdown) => {
                        info!("버퍼 풀 조정 스레드 종료");
                        break;
                    },
                    None => {
                        warn!("버퍼 풀 조정 채널이 닫힘");
                        break;
                    }
                }
            }
        });
    }
    
    /// 버퍼 풀 크기 조정
    fn adjust_buffer_pools(&self) {
        let mut stats = self.stats.write().unwrap();
        
        // 조정 시간 확인
        if !stats.should_adjust_pools() {
            return;
        }
        
        // 현재 사용 가능한 버퍼 수 확인
        let small_available = self.small_buffers.read().unwrap().len();
        let medium_available = self.medium_buffers.read().unwrap().len();
        let large_available = self.large_buffers.read().unwrap().len();
        
        // 사용률 계산
        stats.update_usage_rates(small_available, medium_available, large_available);
        
        // 풀 크기 조정
        let mut small_size = stats.small_pool_size;
        let mut medium_size = stats.medium_pool_size;
        let mut large_size = stats.large_pool_size;
        
        // 사용률에 따라 풀 크기 조정
        if stats.small_usage_rate > BUFFER_USAGE_THRESHOLD_HIGH {
            // 사용률이 높으면 풀 크기 증가
            let increase = (small_size as f64 * BUFFER_POOL_ADJUSTMENT_RATE) as usize;
            small_size += increase;
            debug!("소형 버퍼 풀 확장: {} -> {} (사용률: {:.1}%)", 
                  stats.small_pool_size, small_size, stats.small_usage_rate * 100.0);
        } else if stats.small_usage_rate < BUFFER_USAGE_THRESHOLD_LOW {
            // 사용률이 낮으면 풀 크기 감소 (최소 SMALL_POOL_SIZE/2 유지)
            let decrease = (small_size as f64 * BUFFER_POOL_ADJUSTMENT_RATE) as usize;
            small_size = std::cmp::max(small_size.saturating_sub(decrease), SMALL_POOL_SIZE / 2);
            debug!("소형 버퍼 풀 축소: {} -> {} (사용률: {:.1}%)", 
                  stats.small_pool_size, small_size, stats.small_usage_rate * 100.0);
        }
        
        if stats.medium_usage_rate > BUFFER_USAGE_THRESHOLD_HIGH {
            let increase = (medium_size as f64 * BUFFER_POOL_ADJUSTMENT_RATE) as usize;
            medium_size += increase;
            debug!("중형 버퍼 풀 확장: {} -> {} (사용률: {:.1}%)", 
                  stats.medium_pool_size, medium_size, stats.medium_usage_rate * 100.0);
        } else if stats.medium_usage_rate < BUFFER_USAGE_THRESHOLD_LOW {
            let decrease = (medium_size as f64 * BUFFER_POOL_ADJUSTMENT_RATE) as usize;
            medium_size = std::cmp::max(medium_size.saturating_sub(decrease), MEDIUM_POOL_SIZE / 2);
            debug!("중형 버퍼 풀 축소: {} -> {} (사용률: {:.1}%)", 
                  stats.medium_pool_size, medium_size, stats.medium_usage_rate * 100.0);
        }
        
        if stats.large_usage_rate > BUFFER_USAGE_THRESHOLD_HIGH {
            let increase = (large_size as f64 * BUFFER_POOL_ADJUSTMENT_RATE) as usize;
            large_size += increase;
            debug!("대형 버퍼 풀 확장: {} -> {} (사용률: {:.1}%)", 
                  stats.large_pool_size, large_size, stats.large_usage_rate * 100.0);
        } else if stats.large_usage_rate < BUFFER_USAGE_THRESHOLD_LOW {
            let decrease = (large_size as f64 * BUFFER_POOL_ADJUSTMENT_RATE) as usize;
            large_size = std::cmp::max(large_size.saturating_sub(decrease), LARGE_POOL_SIZE / 2);
            debug!("대형 버퍼 풀 축소: {} -> {} (사용률: {:.1}%)", 
                  stats.large_pool_size, large_size, stats.large_usage_rate * 100.0);
        }
        
        // 풀 크기 조정 실행
        self.resize_pools(small_size, medium_size, large_size);
        
        // 통계 업데이트
        stats.update_pool_sizes(small_size, medium_size, large_size);
        stats.update_adjustment_time();
        stats.print_metrics();
    }
    
    /// 버퍼 풀 크기 조정 실행
    fn resize_pools(&self, small_size: usize, medium_size: usize, large_size: usize) {
        // 소형 버퍼 풀 조정
        let mut small_buffers = self.small_buffers.write().unwrap();
        self.resize_pool(&mut small_buffers, small_size, BUFFER_SIZE_SMALL);
        
        // 중형 버퍼 풀 조정
        let mut medium_buffers = self.medium_buffers.write().unwrap();
        self.resize_pool(&mut medium_buffers, medium_size, BUFFER_SIZE_MEDIUM);
        
        // 대형 버퍼 풀 조정
        let mut large_buffers = self.large_buffers.write().unwrap();
        self.resize_pool(&mut large_buffers, large_size, BUFFER_SIZE_LARGE);
    }
    
    /// 개별 버퍼 풀 크기 조정
    fn resize_pool(&self, pool: &mut Vec<BytesMut>, target_size: usize, buffer_size: usize) {
        let current_size = pool.len();
        
        if current_size < target_size {
            // 풀 확장
            let additional = target_size - current_size;
            
            // Rayon을 사용한 병렬 버퍼 생성
            let new_buffers: Vec<BytesMut> = (0..additional)
                .into_par_iter()
                .map(|_| BytesMut::with_capacity(buffer_size))
                .collect();
                
            pool.extend(new_buffers);
            
        } else if current_size > target_size {
            // 풀 축소
            pool.truncate(target_size);
        }
    }
    
    /// 지정된 크기와 용량의 버퍼 벡터 생성
    fn create_buffer_vec(capacity: usize, buffer_size: usize) -> Vec<BytesMut> {
        // Rayon을 사용한 병렬 버퍼 생성
        (0..capacity)
            .into_par_iter()
            .map(|_| BytesMut::with_capacity(buffer_size))
            .collect()
    }

    // 버퍼 할당
    pub fn get_buffer(&self, hint_size: Option<usize>) -> BytesMut {
        let size = hint_size.unwrap_or(BUFFER_SIZE_SMALL);
        let buffer_size = BufferSize::from_size(size);
        
        // 적절한 크기의 버퍼 가져오기
        let buffer = self.get_buffer_by_size(buffer_size);
        
        // 통계 업데이트 및 출력
        self.update_metrics();
        
        buffer
    }
    
    /// 특정 크기의 버퍼 가져오기
    fn get_buffer_by_size(&self, size: BufferSize) -> BytesMut {
        let mut stats = self.stats.write().unwrap();
        
        // 요청된 버퍼 크기에 해당하는 풀에서 버퍼 가져오기
        let buffer = match size {
            BufferSize::Small => {
                let mut buffers = self.small_buffers.write().unwrap();
                if let Some(buffer) = buffers.pop() {
                    stats.increment_reuses();
                    buffer
                } else {
                    stats.increment_allocations();
                    BytesMut::with_capacity(size.capacity())
                }
            },
            BufferSize::Medium => {
                let mut buffers = self.medium_buffers.write().unwrap();
                if let Some(buffer) = buffers.pop() {
                    stats.increment_reuses();
                    buffer
                } else {
                    stats.increment_allocations();
                    BytesMut::with_capacity(size.capacity())
                }
            },
            BufferSize::Large => {
                let mut buffers = self.large_buffers.write().unwrap();
                if let Some(buffer) = buffers.pop() {
                    stats.increment_reuses();
                    buffer
                } else {
                    stats.increment_allocations();
                    BytesMut::with_capacity(size.capacity())
                }
            },
        };
        
        buffer
    }
    
    /// 메트릭스 업데이트 및 출력
    fn update_metrics(&self) {
        let mut stats = self.stats.write().unwrap();
        
        // 출력 간격 확인 및 출력
        if stats.should_print_metrics() {
            let small_available = self.small_buffers.read().unwrap().len();
            let medium_available = self.medium_buffers.read().unwrap().len();
            let large_available = self.large_buffers.read().unwrap().len();
            
            stats.update_usage_rates(small_available, medium_available, large_available);
            stats.print_metrics();
            stats.update_metrics_time();
        }
    }

    // 버퍼 반환
    pub fn return_buffer(&self, mut buffer: BytesMut) {
        buffer.clear();
        
        // 버퍼 크기에 맞는 풀로 반환
        let size = BufferSize::from_size(buffer.capacity());
        self.return_buffer_to_pool(buffer, size);
        
        // 통계 업데이트
        let mut stats = self.stats.write().unwrap();
        stats.increment_returns();
    }
    
    /// 버퍼를 적절한 풀에 반환
    fn return_buffer_to_pool(&self, buffer: BytesMut, size: BufferSize) {
        match size {
            BufferSize::Small => {
                let mut buffers = self.small_buffers.write().unwrap();
                buffers.push(buffer);
            },
            BufferSize::Medium => {
                let mut buffers = self.medium_buffers.write().unwrap();
                buffers.push(buffer);
            },
            BufferSize::Large => {
                let mut buffers = self.large_buffers.write().unwrap();
                buffers.push(buffer);
            },
        }
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        let stats = self.stats.read().unwrap().clone();
        
        Self {
            small_buffers: RwLock::new(Vec::new()),
            medium_buffers: RwLock::new(Vec::new()),
            large_buffers: RwLock::new(Vec::new()),
            stats: RwLock::new(stats),
            adjustment_tx: self.adjustment_tx.clone(),
        }
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        // 마지막 인스턴스가 제거될 때 종료 명령 전송
        let _ = self.adjustment_tx.try_send(BufferPoolCommand::Shutdown);
    }
}