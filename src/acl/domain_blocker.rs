use std::sync::Arc;
use log::{info, debug};
use std::num::NonZeroUsize;
use std::sync::RwLock;
use lru::LruCache;
use once_cell::sync::Lazy;

use crate::config::Config;
use crate::constants::ACL_CACHE_SIZE;

/// 도메인 매칭 결과를 나타내는 열거형
#[derive(Debug, Clone)]
enum MatchResult {
    Blocked,
    NotBlocked,
}

// 도메인 차단 결과 캐시
static DOMAIN_BLOCK_CACHE: Lazy<RwLock<LruCache<String, MatchResult>>> = 
    Lazy::new(|| RwLock::new(LruCache::new(NonZeroUsize::new(ACL_CACHE_SIZE).unwrap())));

/// 도메인 차단을 처리하는 구조체
pub struct DomainBlocker {
    config: Arc<Config>,
}

impl DomainBlocker {
    /// 새로운 DomainBlocker 인스턴스 생성
    pub fn new(config: Arc<Config>) -> Self {
        debug!("DomainBlocker 초기화: 캐시 크기 {}", ACL_CACHE_SIZE);
        Self { 
            config,
        }
    }
    
    /// 주어진 도메인이 차단 목록에 있는지 확인
    pub fn is_blocked(&self, host: &str) -> bool {
        let host_lower = host.to_lowercase();
        
        // 캐시에서 결과 확인
        if let Some(result) = self.check_cache(&host_lower) {
            match result {
                MatchResult::Blocked => {
                    info!("캐시된 차단 도메인 감지: {}", host_lower);
                    return true;
                },
                MatchResult::NotBlocked => return false,
            }
        }
        
        // 도메인 매칭 시도 - Config의 is_domain_blocked 메서드 활용
        let is_blocked = self.config.is_domain_blocked(&host_lower);
        
        // 결과를 캐시에 저장
        let result = if is_blocked {
            MatchResult::Blocked
        } else {
            MatchResult::NotBlocked
        };
        
        self.update_cache(&host_lower, result);
        
        if is_blocked {
            info!("차단된 도메인 감지: {}", host_lower);
        }
        
        is_blocked
    }
    
    /// 캐시에서 도메인 차단 결과 확인
    fn check_cache(&self, host: &str) -> Option<MatchResult> {
        if let Ok(cache) = DOMAIN_BLOCK_CACHE.read() {
            return cache.peek(host).cloned();
        }
        None
    }
    
    /// 캐시에 도메인 차단 결과 저장
    fn update_cache(&self, host: &str, result: MatchResult) {
        if let Ok(mut cache) = DOMAIN_BLOCK_CACHE.write() {
            cache.put(host.to_string(), result);
        }
    }
    
    // /// 차단된 요청을 로깅
    // pub async fn log_blocked_request(&self, request_data: &str, host: &str, ip: &str, session_id: &str) {
    //     if let Ok(logger) = REQUEST_LOGGER.try_lock() {
    //         logger.log_rejected_request(request_data, host, ip, session_id).await;
    //     } else {
    //         error!("[Session:{}] Failed to acquire RequestLogger lock for blocked request logging", session_id);
    //     }
    // }
    
    // /// 대규모 도메인 목록 로드 (향후 확장용)
    // pub fn load_blocklist(&mut self, _path: &str) -> Result<(), std::io::Error> {
    //     // 파일에서 대규모 차단 목록 로드 구현
    //     // 예: CSV, JSON 등에서 도메인 목록 로드
        
    //     debug!("도메인 차단 목록 로드 완료");
    //     Ok(())
    // }
    
    // /// 트라이(Trie) 자료구조 구축 (향후 확장용)
    // fn build_domain_trie(&self, _domains: &HashSet<String>) {
    //     // 도메인 트라이 구축 로직
    //     // 서브도메인 패턴 매칭
    // }
    
} 