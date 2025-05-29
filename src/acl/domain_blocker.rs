use std::sync::Arc;
use log::{info, debug, error, warn};
use std::num::NonZeroUsize;
use std::sync::RwLock;
use lru::LruCache;
use once_cell::sync::Lazy;
use tokio::time::Duration;
use std::collections::{HashSet, HashMap};

use crate::config::Config;
use crate::constants::{domain_blocks, domain_pattern_blocks, ACL_CACHE_SIZE};
use crate::db;

/// 도메인 매칭 결과를 나타내는 열거형
#[derive(Debug, Clone)]
enum MatchResult {
    Blocked,
    NotBlocked,
}

// 도메인 차단 결과 캐시
static DOMAIN_BLOCK_CACHE: Lazy<RwLock<LruCache<String, MatchResult>>> = 
    Lazy::new(|| RwLock::new(LruCache::new(NonZeroUsize::new(ACL_CACHE_SIZE).unwrap())));

// 차단된 도메인 목록 (DB에서 로드) - 정확한 도메인 URL
static BLOCKED_DOMAINS: Lazy<RwLock<HashSet<String>>> = Lazy::new(|| RwLock::new(HashSet::new()));

// 와일드카드 패턴 (*.example.com)을 위한 접미사 매핑
static BLOCKED_SUFFIXES: Lazy<RwLock<HashMap<String, bool>>> = Lazy::new(|| RwLock::new(HashMap::new()));

/// 도메인 차단을 처리하는 구조체
pub struct DomainBlocker {
    config: Arc<Config>,
}

impl DomainBlocker {
    /// 새로운 DomainBlocker 인스턴스 생성
    pub fn new(config: Arc<Config>) -> Self {
        debug!("DomainBlocker 초기화: 캐시 크기 {}", ACL_CACHE_SIZE);
        
        // 도메인 차단 테이블 초기화 및 로드
        tokio::spawn(async {
            // 정확한 도메인 테이블 초기화
            match Self::ensure_domain_block_tables().await {
                Ok(_) => {
                    info!("도메인 차단 테이블 초기화 완료");
                    // 도메인 차단 목록 로드
                    if let Err(e) = Self::load_blocked_domains_from_db().await {
                        error!("DB에서 도메인 차단 목록 로드 실패: {}", e);
                    } else {
                        debug!("DB에서 도메인 차단 목록 로드 완료");
                    }
                    
                    // 주기적으로 도메인 차단 목록 갱신 (10분 간격)
                    tokio::spawn(async {
                        let mut interval = tokio::time::interval(Duration::from_secs(600));
                        loop {
                            interval.tick().await;
                            
                            if let Err(e) = Self::load_blocked_domains_from_db().await {
                                error!("도메인 차단 목록 주기적 갱신 실패: {}", e);
                            } else {
                                debug!("도메인 차단 목록 주기적 갱신 완료");
                            }
                        }
                    });
                },
                Err(e) => {
                    error!("도메인 차단 테이블 초기화 실패: {}", e);
                }
            }
        });
        
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
        
        // Config의 차단 도메인 체크 (정적 설정)
        let is_blocked_in_config = self.config.is_domain_blocked(&host_lower);
        
        // DB에서 로드된 차단 목록 체크 (동적 설정) - 정확한 도메인 일치
        let is_blocked_in_db = if let Ok(domains) = BLOCKED_DOMAINS.read() {
            // 완전 일치 확인 - O(1) 시간 복잡도
            domains.contains(&host_lower)
        } else {
            false
        };

        // 와일드카드 패턴 확인
        let is_suffix_match = if let Ok(suffixes) = BLOCKED_SUFFIXES.read() {
            // 접미사 매치 확인
            let domain_parts: Vec<&str> = host_lower.split('.').collect();
            let domain_len = domain_parts.len();
            
            // 도메인의 모든 가능한 접미사를 검사
            // 예: test.example.com -> [.example.com, .com]
            let mut matches = false;
            for i in 1..domain_len {
                let suffix = format!(".{}", domain_parts[i..].join("."));
                if let Some(&blocked) = suffixes.get(&suffix) {
                    if blocked {
                        matches = true;
                        break;
                    }
                }
            }
            matches
        } else {
            false
        };
        
        let is_blocked = is_blocked_in_config || is_blocked_in_db || is_suffix_match;
        
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
    
    /// 도메인 차단 테이블들 생성 확인
    async fn ensure_domain_block_tables() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
                return Err(e);
            }
        };
        
        // 1. 정확한 도메인 테이블 존재 여부 확인
        let domain_table_exists = match executor.query_one(domain_blocks::CHECK_TABLE_EXISTS, &[], |row| Ok(row.get::<_, bool>(0))).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("domain_blocks 테이블 존재 여부 확인 실패: {}", e);
                false
            }
        };
        
        // 2. 패턴 도메인 테이블 존재 여부 확인
        let pattern_table_exists = match executor.query_one(domain_pattern_blocks::CHECK_TABLE_EXISTS, &[], |row| Ok(row.get::<_, bool>(0))).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("domain_pattern_blocks 테이블 존재 여부 확인 실패: {}", e);
                false
            }
        };
        
        // 정확한 도메인 테이블 생성
        if !domain_table_exists {
            debug!("domain_blocks 테이블이 존재하지 않습니다. 새로 생성합니다.");
            
            // 테이블 생성 실행
            if let Err(e) = executor.execute_query(domain_blocks::CREATE_TABLE, &[]).await {
                error!("domain_blocks 테이블 생성 실패: {}", e);
                return Err(e);
            }
            
            // 인덱스 생성 실행
            for index_query in domain_blocks::CREATE_INDICES.iter() {
                if let Err(e) = executor.execute_query(index_query, &[]).await {
                    warn!("domain_blocks 인덱스 생성 실패: {}", e);
                    // 인덱스 생성 실패는 치명적이지 않으므로 계속 진행
                }
            }
            
            info!("domain_blocks 테이블이 성공적으로 생성되었습니다.");
        } else {
            debug!("domain_blocks 테이블이 이미 존재합니다.");
        }
        
        // 패턴 도메인 테이블 생성
        if !pattern_table_exists {
            debug!("domain_pattern_blocks 테이블이 존재하지 않습니다. 새로 생성합니다.");
            
            // 테이블 생성 실행
            if let Err(e) = executor.execute_query(domain_pattern_blocks::CREATE_TABLE, &[]).await {
                error!("domain_pattern_blocks 테이블 생성 실패: {}", e);
                return Err(e);
            }
            
            // 인덱스 생성 실행
            for index_query in domain_pattern_blocks::CREATE_INDICES.iter() {
                if let Err(e) = executor.execute_query(index_query, &[]).await {
                    warn!("domain_pattern_blocks 인덱스 생성 실패: {}", e);
                    // 인덱스 생성 실패는 치명적이지 않으므로 계속 진행
                }
            }
            
            info!("domain_pattern_blocks 테이블이 성공적으로 생성되었습니다.");
        } else {
            debug!("domain_pattern_blocks 테이블이 이미 존재합니다.");
        }
        
        Ok(())
    }
    
    /// DB에서 도메인 차단 목록 로드
    async fn load_blocked_domains_from_db() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
                return Err(e);
            }
        };
        
        // 클라이언트 가져오기
        let client = match executor.pool.get_client().await {
            Ok(client) => client,
            Err(e) => {
                error!("DB 클라이언트 가져오기 실패: {}", e);
                return Err(Box::new(e));
            }
        };
        
        // 정확한 도메인 쿼리 실행
        let exact_rows = match client.query(domain_blocks::SELECT_ACTIVE_DOMAINS, &[]).await {
            Ok(rows) => rows,
            Err(e) => {
                error!("정확한 도메인 차단 목록 쿼리 실패: {}", e);
                return Err(Box::new(e));
            }
        };
        
        // 패턴 도메인 쿼리 실행
        let pattern_rows = match client.query(domain_pattern_blocks::SELECT_ACTIVE_PATTERNS, &[]).await {
            Ok(rows) => rows,
            Err(e) => {
                error!("패턴 도메인 차단 목록 쿼리 실패: {}", e);
                return Err(Box::new(e));
            }
        };
        
        // 결과 처리를 위한 임시 컬렉션
        let mut exact_domains = HashSet::new();
        let mut suffix_patterns = HashMap::new();
        
        // 정확한 도메인 처리
        for row in exact_rows {
            let domain: String = row.get(0);
            exact_domains.insert(domain.to_lowercase());
        }
        
        // 패턴 도메인 처리
        for row in pattern_rows {
            let pattern: String = row.get(0);
            
            // 와일드카드 패턴인지 확인
            if pattern.starts_with("*.") {
                // *.example.com -> .example.com 형태로 저장
                let suffix = format!(".{}", &pattern[2..]);
                suffix_patterns.insert(suffix.to_lowercase(), true);
            }
        }
        
        // 정확한 도메인 목록 저장
        if let Ok(mut blocked_domains) = BLOCKED_DOMAINS.write() {
            *blocked_domains = exact_domains.clone();
        } else {
            error!("차단 도메인 목록 잠금 획득 실패");
            return Err("차단 도메인 목록 잠금 획득 실패".into());
        }
        
        // 접미사 패턴 저장
        if let Ok(mut blocked_suffixes) = BLOCKED_SUFFIXES.write() {
            *blocked_suffixes = suffix_patterns.clone();
        } else {
            error!("차단 접미사 목록 잠금 획득 실패");
            return Err("차단 접미사 목록 잠금 획득 실패".into());
        }
        
        // 캐시 초기화
        if let Ok(mut cache) = DOMAIN_BLOCK_CACHE.write() {
            cache.clear();
            debug!("도메인 차단 캐시 초기화 완료");
        }
        
        let total_count = exact_domains.len() + suffix_patterns.len();
        debug!("DB에서 {} 개의 차단 도메인 로드 완료 (정확한 도메인: {}, 와일드카드 패턴: {})", 
             total_count, exact_domains.len(), suffix_patterns.len());
        
        Ok(())
    }
} 