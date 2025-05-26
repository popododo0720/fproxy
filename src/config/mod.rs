use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::collections::HashSet;

use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub bind_host: String,
    pub bind_port: u16,
    pub buffer_size: usize,
    pub timeout_ms: usize,
    pub tls_inspection: bool,
    pub ssl_dir: String,
    pub worker_threads: Option<usize>,
    pub cache_enabled: bool,
    pub cache_size: usize,
    pub cache_ttl_seconds: u64,
    pub tls_verify_certificate: bool,
    pub access_control: AccessControlConfig,
    pub blocked_domains: HashSet<String>,
    pub blocked_patterns: HashSet<String>,
}

impl Config {
    pub fn new() -> Self {
        let mut blocked_domains = HashSet::new();
        blocked_domains.insert("nexon.com".to_string());
        
        let mut blocked_patterns = HashSet::new();
        blocked_patterns.insert("*.nexon.com".to_string());
        
        let config = Self {
            bind_host: "0.0.0.0".to_string(),
            bind_port: 50000,
            buffer_size: 8192,
            timeout_ms: 30000,
            tls_inspection: false,
            ssl_dir: "ssl".to_string(),
            worker_threads: None,
            cache_enabled: true,
            cache_size: 1000,
            cache_ttl_seconds: 300,
            tls_verify_certificate: true,
            access_control: AccessControlConfig {},
            blocked_domains,
            blocked_patterns,
        };

        config
    }

    pub fn from_file(path: &str) -> Result<Self, Box<dyn Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let config = serde_yml::from_str(&contents)?;

        Ok(config)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessControlConfig {}
