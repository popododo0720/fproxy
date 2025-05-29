use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::collections::HashSet;

use serde::{Serialize, Deserialize};

/// 프록시 서버 설정
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub bind_host: String,
    pub bind_port: u16,
    pub buffer_size: usize,
    pub timeout_ms: usize,
    pub ssl_dir: String,
    pub worker_threads: Option<usize>,
    pub tls_verify_certificate: bool,
    #[serde(default = "default_disable_verify_internal_ip")]
    pub disable_verify_internal_ip: bool,
    pub blocked_domains: HashSet<String>,
    pub blocked_patterns: HashSet<String>,
    #[serde(default)]
    pub trusted_certificates: Vec<String>,
    #[serde(default = "default_cache_enabled")]
    pub cache_enabled: bool,
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,
    #[serde(default = "default_cache_ttl_seconds")]
    pub cache_ttl_seconds: u64,
}

fn default_disable_verify_internal_ip() -> bool {
    false
}

fn default_cache_enabled() -> bool {
    true
}

fn default_cache_size() -> usize {
    1000
}

fn default_cache_ttl_seconds() -> u64 {
    300
}

impl Config {
    /// 기본 설정으로 Config 인스턴스 생성
    pub fn new() -> Self {
        Self {
            bind_host: "0.0.0.0".to_string(),
            bind_port: 50000,
            buffer_size: 8192,
            timeout_ms: 30000,
            ssl_dir: "ssl".to_string(),
            worker_threads: None,
            tls_verify_certificate: true,
            disable_verify_internal_ip: default_disable_verify_internal_ip(),
            blocked_domains: HashSet::new(),
            blocked_patterns: HashSet::new(),
            trusted_certificates: Vec::new(),
            cache_enabled: default_cache_enabled(),
            cache_size: default_cache_size(),
            cache_ttl_seconds: default_cache_ttl_seconds(),
        }
    }

    /// 기본 차단 도메인 목록 생성
    fn default_blocked_domains() -> HashSet<String> {
        HashSet::new()
    }

    /// 기본 차단 패턴 목록 생성
    fn default_blocked_patterns() -> HashSet<String> {
        HashSet::new()
    }

    /// 설정 파일에서 Config 인스턴스 로드
    pub fn from_file(path: &str) -> Result<Self, Box<dyn Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let mut config: Self = serde_yaml::from_str(&contents)?;
        
        // 파일에서 로드한 설정에 기본 차단 도메인이 없으면 추가
        if config.blocked_domains.is_empty() {
            config.blocked_domains = Self::default_blocked_domains();
        }
        
        // 파일에서 로드한 설정에 기본 차단 패턴이 없으면 추가
        if config.blocked_patterns.is_empty() {
            config.blocked_patterns = Self::default_blocked_patterns();
        }

        Ok(config)
    }
    
    /// 도메인이 차단 목록에 있는지 확인
    pub fn is_domain_blocked(&self, domain: &str) -> bool {
        self.blocked_domains.contains(domain) || 
        self.blocked_patterns.iter().any(|pattern| {
            if pattern.starts_with("*.") {
                let suffix = &pattern[1..]; 
                domain.ends_with(suffix)
            } else {
                false
            }
        })
    }
}
