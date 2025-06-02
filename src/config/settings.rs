
use std::path::Path;
use std::env;

use log::{info, warn};

use crate::error::{ProxyError, Result};
use crate::config::Config;
use crate::db::config::DbConfig;

/// 설정 소스 우선순위
#[derive(Clone)]
pub enum ConfigSource {
    /// 환경 변수
    Environment,
    /// 설정 파일
    File,
    /// 기본값
    Default,
}

/// 통합 설정 관리자
#[derive(Clone)]
pub struct Settings {
    /// 프록시 서버 설정
    pub proxy: Config,
    /// 데이터베이스 설정
    pub database: DbConfig,
    /// 프록시 설정 소스
    pub proxy_source: ConfigSource,
    /// 데이터베이스 설정 소스
    pub db_source: ConfigSource,
}

impl Settings {
    /// 새로운 설정 인스턴스 생성
    pub fn new() -> Result<Self> {
        let (proxy, proxy_source) = Self::load_proxy_config()?;
        let (database, db_source) = Self::load_db_config()?;
        
        Ok(Self {
            proxy,
            database,
            proxy_source,
            db_source,
        })
    }
    
    /// 프록시 설정 로드
    fn load_proxy_config() -> Result<(Config, ConfigSource)> {
        // 1. 환경 변수에서 설정 파일 경로 확인
        if let Ok(path) = env::var("CONFIG_FILE") {
            info!("환경 변수에서 설정 파일 경로 로드: {}", path);
            if Path::new(&path).exists() {
                match Config::from_file(&path) {
                    Ok(config) => return Ok((config, ConfigSource::Environment)),
                    Err(e) => {
                        warn!("환경 변수에 지정된 설정 파일 로드 실패: {}", e);
                    }
                }
            } else {
                warn!("환경 변수에 지정된 설정 파일이 존재하지 않음: {}", path);
            }
        }
        
        // 2. 현재 디렉토리의 config.yml 파일 확인
        if Path::new("config.yml").exists() {
            info!("설정 파일 로드: config.yml");
            match Config::from_file("config.yml") {
                Ok(config) => return Ok((config, ConfigSource::File)),
                Err(e) => {
                    warn!("기본 설정 파일 로드 실패: {}", e);
                }
            }
        }
        
        // 3. 기본 설정 사용
        info!("설정 파일을 찾을 수 없어 기본 설정 사용");
        Ok((Config::new(), ConfigSource::Default))
    }
    
    /// 데이터베이스 설정 로드
    fn load_db_config() -> Result<(DbConfig, ConfigSource)> {
        // 1. 환경 변수에서 설정 파일 경로 확인
        if let Ok(path) = env::var("DB_CONFIG_FILE") {
            info!("환경 변수에서 DB 설정 파일 경로 로드: {}", path);
            if Path::new(&path).exists() {
                match DbConfig::load_from_file(&path) {
                    Ok(config) => return Ok((config, ConfigSource::Environment)),
                    Err(e) => {
                        warn!("환경 변수에 지정된 DB 설정 파일 로드 실패: {}", e);
                    }
                }
            } else {
                warn!("환경 변수에 지정된 DB 설정 파일이 존재하지 않음: {}", path);
            }
        }
        
        // 2. 현재 디렉토리의 db.yml 파일 확인
        if Path::new("db.yml").exists() {
            info!("DB 설정 파일 로드: db.yml");
            match DbConfig::load_from_file("db.yml") {
                Ok(config) => return Ok((config, ConfigSource::File)),
                Err(e) => {
                    warn!("기본 DB 설정 파일 로드 실패: {}", e);
                }
            }
        }
        
        // 3. 기본 설정 사용
        info!("DB 설정 파일을 찾을 수 없어 기본 설정 사용");
        Ok((DbConfig::default(), ConfigSource::Default))
    }
    
    /// 환경 변수에서 설정 값 오버라이드
    pub fn override_from_env(&mut self) -> Result<()> {
        // 프록시 서버 설정 오버라이드
        if let Ok(host) = env::var("PROXY_BIND_HOST") {
            info!("환경 변수에서 바인딩 호스트 설정: {}", host);
            self.proxy.bind_host = host;
            self.proxy_source = ConfigSource::Environment;
        }
        
        if let Ok(port) = env::var("PROXY_BIND_PORT") {
            if let Ok(port) = port.parse::<u16>() {
                info!("환경 변수에서 바인딩 포트 설정: {}", port);
                self.proxy.bind_port = port;
                self.proxy_source = ConfigSource::Environment;
            } else {
                warn!("환경 변수 PROXY_BIND_PORT 값이 유효한 포트 번호가 아님: {}", port);
            }
        }
        
        if let Ok(buffer_size) = env::var("PROXY_BUFFER_SIZE") {
            if let Ok(size) = buffer_size.parse::<usize>() {
                info!("환경 변수에서 버퍼 크기 설정: {}", size);
                self.proxy.buffer_size = size;
                self.proxy_source = ConfigSource::Environment;
            }
        }
        
        if let Ok(timeout) = env::var("PROXY_TIMEOUT_MS") {
            if let Ok(timeout) = timeout.parse::<usize>() {
                info!("환경 변수에서 타임아웃 설정: {}ms", timeout);
                self.proxy.timeout_ms = timeout;
                self.proxy_source = ConfigSource::Environment;
            }
        }
        
        // 데이터베이스 설정 오버라이드
        if let Ok(host) = env::var("DB_HOST") {
            info!("환경 변수에서 DB 호스트 설정: {}", host);
            self.database.connection.host = host;
            self.db_source = ConfigSource::Environment;
        }
        
        if let Ok(port) = env::var("DB_PORT") {
            if let Ok(port) = port.parse::<u16>() {
                info!("환경 변수에서 DB 포트 설정: {}", port);
                self.database.connection.port = port;
                self.db_source = ConfigSource::Environment;
            }
        }
        
        if let Ok(name) = env::var("DB_NAME") {
            info!("환경 변수에서 DB 이름 설정: {}", name);
            self.database.connection.database = name;
            self.db_source = ConfigSource::Environment;
        }
        
        if let Ok(user) = env::var("DB_USER") {
            info!("환경 변수에서 DB 사용자 설정: {}", user);
            self.database.connection.user = user;
            self.db_source = ConfigSource::Environment;
        }
        
        if let Ok(password) = env::var("DB_PASSWORD") {
            info!("환경 변수에서 DB 비밀번호 설정");
            self.database.connection.password = password;
            self.db_source = ConfigSource::Environment;
        }
        
        if let Ok(max_conn) = env::var("DB_MAX_CONNECTIONS") {
            if let Ok(max) = max_conn.parse::<usize>() {
                info!("환경 변수에서 DB 최대 연결 수 설정: {}", max);
                self.database.connection.max_connections = max;
                self.db_source = ConfigSource::Environment;
            }
        }
        
        Ok(())
    }
    
    /// 설정 정보 로그 출력
    pub fn log_settings(&self) {
        info!("프록시 설정 소스: {}", match self.proxy_source {
            ConfigSource::Environment => "환경 변수",
            ConfigSource::File => "설정 파일",
            ConfigSource::Default => "기본값",
        });
        
        info!("데이터베이스 설정 소스: {}", match self.db_source {
            ConfigSource::Environment => "환경 변수",
            ConfigSource::File => "설정 파일",
            ConfigSource::Default => "기본값",
        });
        
        info!("프록시 바인딩: {}:{}", self.proxy.bind_host, self.proxy.bind_port);
        info!("데이터베이스 연결: {}:{}/{}", 
            self.database.connection.host,
            self.database.connection.port,
            self.database.connection.database
        );
    }
}
