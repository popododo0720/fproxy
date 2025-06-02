use std::sync::Arc;
use log::{info, debug, error, warn};
use std::num::NonZeroUsize;
use std::sync::RwLock;
use lru::LruCache;
use tokio::time::Duration;
use std::collections::HashSet;
use regex::Regex;

use crate::config::Config;
use crate::constants::{domain_blocks, domain_pattern_blocks, ACL_CACHE_SIZE};
use crate::db;

/// 도메인 매칭 결과를 나타내는 열거형
#[derive(Debug, Clone)]
enum MatchResult {
    Blocked,
    NotBlocked,
}

/// 도메인 차단을 처리하는 구조체
pub struct DomainBlocker {
    config: Arc<Config>,
    // 도메인 차단 결과 캐시
    domain_block_cache: RwLock<LruCache<String, MatchResult>>,
    // 차단된 도메인 목록
    blocked_domains: RwLock<HashSet<String>>,
    // 정규표현식 패턴
    regex_patterns: RwLock<Vec<Regex>>,
    // 초기화 완료 여부
    initialized: RwLock<bool>,
}

impl DomainBlocker {
    /// 새로운 DomainBlocker 인스턴스 생성
    pub fn new(config: Arc<Config>) -> Self {
        debug!("DomainBlocker 초기화: 캐시 크기 {}", ACL_CACHE_SIZE);
        
        Self {
            config,
            domain_block_cache: RwLock::new(LruCache::new(NonZeroUsize::new(ACL_CACHE_SIZE).unwrap())),
            blocked_domains: RwLock::new(HashSet::new()),
            regex_patterns: RwLock::new(Vec::new()),
            initialized: RwLock::new(false),
        }
    }
    
    /// 비동기 초기화 함수 - 메인에서 먼저 호출해야 함
    pub async fn initialize(self: &Arc<Self>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 이미 초기화되었는지 확인
        if *self.initialized.read().unwrap() {
            debug!("DomainBlocker 이미 초기화됨");
            return Ok(());
        }
        
        // 도메인 차단 테이블 초기화
        self.ensure_domain_block_tables().await?;
        info!("도메인 차단 테이블 초기화 완료");
        
        // 도메인 차단 목록 로드
        self.load_blocked_domains_from_db().await?;
        info!("도메인 차단 목록 로드 완료");
        
        // 초기화 완료 표시
        *self.initialized.write().unwrap() = true;
        
        // 주기적 업데이트를 위한 배경 태스크 시작
        // Arc를 클론하여 소유권을 공유
        let blocker = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // 1시간
            loop {
                interval.tick().await;
                debug!("도메인 차단 목록 업데이트 시작");
                
                if let Err(e) = blocker.load_blocked_domains_from_db().await {
                    error!("도메인 차단 목록 업데이트 실패: {}", e);
                } else {
                    debug!("도메인 차단 목록 업데이트 완료");
                }
            }
        });
        
        Ok(())
    }
    
    /// 주어진 도메인이 차단 목록에 있는지 확인
    pub fn is_blocked(&self, host: &str) -> bool {
        // 초기화 여부 확인
        if !*self.initialized.read().unwrap() {
            warn!("초기화되지 않은 DomainBlocker에 접근 시도: {}", host);
            return false; // 초기화되지 않은 경우 차단하지 않음
        }
        
        // 캐시 확인
        if let Some(result) = self.check_cache(host) {
            match result {
                MatchResult::Blocked => {
                    debug!("캐시에서 차단된 도메인 확인: {}", host);
                    return true;
                },
                MatchResult::NotBlocked => {
                    debug!("캐시에서 허용된 도메인 확인: {}", host);
                    return false;
                }
            }
        }
        
        // 정확한 도메인 일치 확인
        let blocked_domains = self.blocked_domains.read().unwrap();
        if blocked_domains.contains(host) {
            debug!("정확히 차단된 도메인: {}", host);
            self.update_cache(host, MatchResult::Blocked);
            return true;
        }
        
        // 정규표현식 패턴 매칭 확인
        let regex_patterns = self.regex_patterns.read().unwrap();
        for pattern in regex_patterns.iter() {
            if pattern.is_match(host) {
                debug!("패턴으로 차단된 도메인: {} ({})", host, pattern.as_str());
                self.update_cache(host, MatchResult::Blocked);
                return true;
            }
        }
        
        // 차단되지 않은 도메인
        self.update_cache(host, MatchResult::NotBlocked);
        false
    }
    
    /// 캐시에서 도메인 차단 결과 확인
    fn check_cache(&self, host: &str) -> Option<MatchResult> {
        let cache = self.domain_block_cache.read().unwrap();
        // LruCache의 get 메서드는 &self를 받지만, 직접 사용하면 오류가 발생하므로 다른 방법 사용
        let result = if let Some(value) = cache.peek(host) {
            Some(value.clone())
        } else {
            None
        };
        result
    }
    
    /// 캐시에 도메인 차단 결과 저장
    fn update_cache(&self, host: &str, result: MatchResult) {
        let mut cache = self.domain_block_cache.write().unwrap();
        cache.put(host.to_string(), result);
    }
    
    /// 도메인 차단 테이블들 생성 확인
    async fn ensure_domain_block_tables(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    async fn load_blocked_domains_from_db(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 쿼리 실행기 가져오기
        let executor = match db::query::QueryExecutor::get_instance().await {
            Ok(executor) => executor,
            Err(e) => {
                error!("쿼리 실행기 가져오기 실패: {}", e);
                return Err(e);
            }
        };
        
        // 정확한 도메인 차단 목록 로드
        let exact_rows = match executor.query_rows(
            domain_blocks::SELECT_ACTIVE_DOMAINS, 
            &[],
            |row| Ok(row)
        ).await {
            Ok(rows) => rows,
            Err(e) => {
                error!("정확한 도메인 차단 목록 쿼리 실패: {}", e);
                return Err(e);
            }
        };
        
        // 패턴 도메인 쿼리 실행
        let pattern_rows = match executor.query_rows(
            domain_pattern_blocks::SELECT_ACTIVE_PATTERNS, 
            &[],
            |row| Ok(row)
        ).await {
            Ok(rows) => rows,
            Err(e) => {
                error!("패턴 도메인 차단 목록 쿼리 실패: {}", e);
                return Err(e);
            }
        };
        
        // 결과 처리를 위한 임시 콜렉션
        let mut exact_domains = HashSet::new();
        let mut regex_patterns_vec = Vec::new();
        
        // 정확한 도메인 처리
        for row in exact_rows {
            let domain: String = row.get(0);
            exact_domains.insert(domain.to_lowercase());
        }
        
        // 패턴 도메인 처리
        for row in pattern_rows {
            let pattern: String = row.get(0);
            
            // 정규표현식 패턴인 경우 (regex: 접두사 제거)
            let regex_pattern = if pattern.starts_with("regex:") {
                pattern[6..].to_string() // "regex:" 접두사 제거
            } else {
                // 그 외 모든 패턴은 와일드카드로 처리
                pattern
                    .replace(".", "\\.")
                    .replace("*", ".*")
            };
            
            match Regex::new(&regex_pattern) {
                Ok(regex) => {
                    regex_patterns_vec.push(regex);
                    debug!("패턴 컴파일 성공: {}", regex_pattern);
                },
                Err(e) => {
                    error!("패턴 컴파일 실패: {} - {}", regex_pattern, e);
                }
            }
        }
        
        // 정확한 도메인 목록 저장
        {
            let mut blocked_domains = self.blocked_domains.write().unwrap();
            *blocked_domains = exact_domains.clone();
            info!("차단 도메인 목록 업데이트 완료: {} 개", blocked_domains.len());
        }
        
        // 정규표현식 패턴 저장
        {
            let mut regex_patterns = self.regex_patterns.write().unwrap();
            *regex_patterns = regex_patterns_vec.clone();
            info!("정규표현식 패턴 목록 업데이트 완료: {} 개", regex_patterns.len());
        }
        
        // 캐시 초기화
        {
            let mut cache = self.domain_block_cache.write().unwrap();
            cache.clear();
            debug!("도메인 차단 캐시 초기화 완료");
        }
        
        let total_count = exact_domains.len() + regex_patterns_vec.len();
        info!("DB에서 {} 개의 차단 도메인 로드 완료 (정확한 도메인: {}, 패턴: {})",
            total_count, exact_domains.len(), regex_patterns_vec.len());
        
        Ok(())
    }
} 