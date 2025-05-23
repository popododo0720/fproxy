use std::sync::Mutex;
use std::time::Instant;

use bytes::{BytesMut};


use crate::constants::*;

pub struct BufferPool {
    small_buffers: Mutex<Vec<BytesMut>>,
    medium_buffers: Mutex<Vec<BytesMut>>,
    large_buffers: Mutex<Vec<BytesMut>>,
    stats: Mutex<BufferStats>,
}

impl BufferPool {
    pub fn new(small_capacity: usize, medium_capacity: usize, large_capacity: usize) -> Self {
        let mut small_buffers = Vec::with_capacity(small_capacity);
        let mut medium_buffers = Vec::with_capacity(medium_capacity);
        let mut large_buffers = Vec::with_capacity(large_capacity);

        for _ in 0..small_capacity {
            small_buffers.push(BytesMut::with_capacity(BUFFER_SIZE_SMALL));
        }

        for _ in 0..medium_capacity {
            medium_buffers.push(BytesMut::with_capacity(BUFFER_SIZE_MEDIUM));
        }

        for _ in 0..large_capacity {
            large_buffers.push(BytesMut::with_capacity(BUFFER_SIZE_LARGE));
        }

        Self {
            small_buffers: Mutex::new(small_buffers),
            medium_buffers: Mutex::new(medium_buffers),
            large_buffers: Mutex::new(large_buffers),
            stats: Mutex::new(BufferStats {
                allocations: 0,
                reuses: 0,
                returns: 0,
                last_metrics_time: Instant::now(),
            })
        }
    }

    // 버퍼 할당
    pub fn get_buffer(&self, hint_size: Option<usize>) -> BytesMut {
        let size = hint_size.unwrap_or(BUFFER_SIZE_SMALL);
        let mut stats = self.stats.lock().unwrap();

        let buffer = if size <= BUFFER_SIZE_SMALL {
            let mut buffers = self.small_buffers.lock().unwrap();
            if let Some(buffer) = buffers.pop() {
                stats.reuses += 1;
                buffer
            } else {
                stats.allocations += 1;
                BytesMut::with_capacity(BUFFER_SIZE_SMALL)
            }
        } else if size <= BUFFER_SIZE_MEDIUM {
            let mut buffers = self.medium_buffers.lock().unwrap();
            if let Some(buffer) = buffers.pop() {
                stats.reuses += 1;
                buffer
            } else {
                stats.allocations += 1;
                BytesMut::with_capacity(BUFFER_SIZE_MEDIUM)
            }
        } else {
            let mut buffers = self.large_buffers.lock().unwrap();
            if let Some(buffer) = buffers.pop() {
                stats.reuses += 1;
                buffer
            } else {
                stats.allocations += 1;
                BytesMut::with_capacity(BUFFER_SIZE_LARGE)
            }
        };

        // 30초마다 통계 출력
        let now = Instant::now();
        if now.duration_since(stats.last_metrics_time).as_secs() > BUFFER_STATS_INTERVAL_SECS {
            log::info!(
                "Buffer stats - Allocations: {}, Reuses: {}, Returns: {}",
                stats.allocations,
                stats.reuses,
                stats.returns
            );
            stats.last_metrics_time = now;
        }

        buffer
    }

    // 버퍼 반환
    pub fn return_buffer(&self, mut buffer: BytesMut) {
        buffer.clear();

        let mut stats = self.stats.lock().unwrap();
        stats.returns += 1;

        let capacity = buffer.capacity();
        if capacity <= BUFFER_SIZE_SMALL {
            let mut buffers = self.small_buffers.lock().unwrap();
            buffers.push(buffer);
        } else if capacity <= BUFFER_SIZE_MEDIUM {
            let mut buffers = self.medium_buffers.lock().unwrap();
            buffers.push(buffer);
        } else {
            let mut buffers = self.large_buffers.lock().unwrap();
            buffers.push(buffer);
        }
    }
}

struct BufferStats {
    allocations: usize,
    reuses: usize,
    returns: usize,
    last_metrics_time: Instant,
}