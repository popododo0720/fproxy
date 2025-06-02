use std::fmt;
use std::error::Error as StdError;
use std::io;
use std::net::AddrParseError;
use std::sync::PoisonError;
use tokio::time::error::Elapsed;
use tokio_rustls::rustls;
use deadpool_postgres::PoolError;
use tokio_postgres::Error as PgError;
use rcgen::Error as RcgenError;

/// UDSS 프록시 서버의 모든 에러 타입을 정의합니다.
#[derive(Debug)]
pub enum ProxyError {
    /// 설정 관련 에러
    Config(String),
    
    /// 네트워크 입출력 에러
    Io(io::Error),
    
    /// 데이터베이스 관련 에러
    Database(String),
    
    /// 로깅 관련 에러
    Logging(String),
    
    /// TLS/SSL 관련 에러
    Tls(String),
    
    /// HTTP 프로토콜 관련 에러
    Http(String),
    
    /// 타임아웃 에러
    Timeout(String),
    
    /// 권한 관련 에러
    AccessControl(String),
    
    /// 내부 상태 관련 에러
    Internal(String),
    
    /// 기타 에러
    Other(String),
}

impl fmt::Display for ProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProxyError::Config(msg) => write!(f, "설정 에러: {}", msg),
            ProxyError::Io(err) => write!(f, "I/O 에러: {}", err),
            ProxyError::Database(msg) => write!(f, "데이터베이스 에러: {}", msg),
            ProxyError::Logging(msg) => write!(f, "로깅 에러: {}", msg),
            ProxyError::Tls(msg) => write!(f, "TLS 에러: {}", msg),
            ProxyError::Http(msg) => write!(f, "HTTP 에러: {}", msg),
            ProxyError::Timeout(msg) => write!(f, "타임아웃 에러: {}", msg),
            ProxyError::AccessControl(msg) => write!(f, "접근 제어 에러: {}", msg),
            ProxyError::Internal(msg) => write!(f, "내부 에러: {}", msg),
            ProxyError::Other(msg) => write!(f, "기타 에러: {}", msg),
        }
    }
}

impl StdError for ProxyError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ProxyError::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// Result 타입 별칭 정의
pub type Result<T> = std::result::Result<T, ProxyError>;

/// From 트레이트 구현으로 다양한 에러 타입을 ProxyError로 변환
impl From<io::Error> for ProxyError {
    fn from(err: io::Error) -> Self {
        ProxyError::Io(err)
    }
}

impl From<AddrParseError> for ProxyError {
    fn from(err: AddrParseError) -> Self {
        ProxyError::Config(format!("주소 파싱 에러: {}", err))
    }
}

impl From<rustls::Error> for ProxyError {
    fn from(err: rustls::Error) -> Self {
        ProxyError::Tls(format!("TLS 에러: {}", err))
    }
}

impl From<PoolError> for ProxyError {
    fn from(err: PoolError) -> Self {
        ProxyError::Database(format!("DB 풀 에러: {}", err))
    }
}

impl From<PgError> for ProxyError {
    fn from(err: PgError) -> Self {
        ProxyError::Database(format!("PostgreSQL 에러: {}", err))
    }
}

impl From<Elapsed> for ProxyError {
    fn from(err: Elapsed) -> Self {
        ProxyError::Timeout(format!("작업 타임아웃: {}", err))
    }
}

impl<T> From<PoisonError<T>> for ProxyError {
    fn from(err: PoisonError<T>) -> Self {
        ProxyError::Internal(format!("락 포이즌 에러: {}", err))
    }
}

impl From<Box<dyn StdError + Send + Sync>> for ProxyError {
    fn from(err: Box<dyn StdError + Send + Sync>) -> Self {
        ProxyError::Other(format!("{}", err))
    }
}

impl From<RcgenError> for ProxyError {
    fn from(err: RcgenError) -> Self {
        ProxyError::Tls(format!("인증서 생성 에러: {}", err))
    }
}

impl From<Box<dyn StdError>> for ProxyError {
    fn from(err: Box<dyn StdError>) -> Self {
        ProxyError::Other(format!("{}", err))
    }
}

impl From<String> for ProxyError {
    fn from(err: String) -> Self {
        ProxyError::Other(err)
    }
}

impl From<&str> for ProxyError {
    fn from(err: &str) -> Self {
        ProxyError::Other(err.to_string())
    }
}

/// 에러 처리 유틸리티 함수
pub fn config_err<E: fmt::Display>(err: E) -> ProxyError {
    ProxyError::Config(format!("{}", err))
}

pub fn db_err<E: fmt::Display>(err: E) -> ProxyError {
    ProxyError::Database(format!("{}", err))
}

pub fn log_err<E: fmt::Display>(err: E) -> ProxyError {
    ProxyError::Logging(format!("{}", err))
}

pub fn tls_err<E: fmt::Display>(err: E) -> ProxyError {
    ProxyError::Tls(format!("{}", err))
}

pub fn http_err<E: fmt::Display>(err: E) -> ProxyError {
    ProxyError::Http(format!("{}", err))
}

pub fn internal_err<E: fmt::Display>(err: E) -> ProxyError {
    ProxyError::Internal(format!("{}", err))
}
