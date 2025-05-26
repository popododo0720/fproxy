use std::sync::Arc;
use log::{info};

use crate::config::Config;

/// 도메인 차단을 처리하는 구조체
pub struct DomainBlocker {
    config: Arc<Config>,
}

impl DomainBlocker {
    /// 새로운 DomainBlocker 인스턴스 생성
    pub fn new(config: Arc<Config>) -> Self {
        Self { 
            config,
        }
    }
    
    /// 주어진 도메인이 차단 목록에 있는지 확인
    pub fn is_blocked(&self, host: &str) -> bool {
        let host_lower = host.to_lowercase();
        
        // 1. 정확한 도메인 매칭 (예: "naver.com")
        if self.config.blocked_domains.contains(&host_lower) {
            info!("차단된 도메인 감지 (정확히 일치): {}", host);
            return true;
        }
        
        // 2. 와일드카드 패턴 매칭 (예: "*.naver.com")
        for pattern in &self.config.blocked_patterns {
            if pattern.starts_with("*.") {
                // *.example.com 형식의 패턴
                let domain_suffix = &pattern[1..]; // "*.example.com" -> ".example.com"
                if host_lower.ends_with(domain_suffix) {
                    info!("차단된 도메인 감지 (와일드카드 패턴 {} 일치): {}", pattern, host);
                    return true;
                }
            }
            // 추가 패턴 형식을 여기에 구현할 수 있음
        }
        
        false
    }
    
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