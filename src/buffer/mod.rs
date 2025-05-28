use std::sync::RwLock;
use std::time::Instant;

use bytes::{BytesMut};

use crate::constants::*;

/// 버퍼 크기 분류
#[derive(Debug, Copy, Clone)]
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
#[derive(Debug)]
struct BufferStats {
    allocations: usize,
    reuses: usize,
    returns: usize,
    last_metrics_time: Instant,
}

impl BufferStats {
    /// 새 버퍼 통계 생성
    fn new() -> Self {
        Self {
            allocations: 0,
            reuses: 0,
            returns: 0,
            last_metrics_time: Instant::now(),
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
    
    /// 통계 출력
    fn print_metrics(&self) {
        log::info!(
            "Buffer stats - Allocations: {}, Reuses: {}, Returns: {}",
            self.allocations,
            self.reuses,
            self.returns
        );
    }
}

pub struct BufferPool {
    small_buffers: RwLock<Vec<BytesMut>>,
    medium_buffers: RwLock<Vec<BytesMut>>,
    large_buffers: RwLock<Vec<BytesMut>>,
    stats: RwLock<BufferStats>,
}

impl BufferPool {
    pub fn new(small_capacity: usize, medium_capacity: usize, large_capacity: usize) -> Self {
        let small_buffers = Self::create_buffer_vec(small_capacity, BUFFER_SIZE_SMALL);
        let medium_buffers = Self::create_buffer_vec(medium_capacity, BUFFER_SIZE_MEDIUM);
        let large_buffers = Self::create_buffer_vec(large_capacity, BUFFER_SIZE_LARGE);

        Self {
            small_buffers: RwLock::new(small_buffers),
            medium_buffers: RwLock::new(medium_buffers),
            large_buffers: RwLock::new(large_buffers),
            stats: RwLock::new(BufferStats::new()),
        }
    }
    
    /// 지정된 크기와 용량의 버퍼 벡터 생성
    fn create_buffer_vec(capacity: usize, buffer_size: usize) -> Vec<BytesMut> {
        (0..capacity)
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