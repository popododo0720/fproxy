use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::fs;
use std::path::Path;
use std::sync::{Mutex, RwLock};
use std::collections::HashMap;
use std::num::NonZeroUsize;

use log::{debug, error, info, warn};
use rcgen::{Certificate, CertificateParams, DistinguishedName, DnType, SanType, KeyPair};
use rustls::{ServerConfig, ClientConfig};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector, server::TlsStream as ServerTlsStream, client::TlsStream as ClientTlsStream};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use once_cell::sync::Lazy;
use lru::LruCache;
use nix::libc;

use crate::constants::*;
use crate::config::Config;

// 루트 CA 인증서와 키를 저장하는 전역 변수
static ROOT_CA: Lazy<Mutex<Option<Certificate>>> = Lazy::new(|| Mutex::new(None));

// 도메인별 인증서 캐시 - LRU 캐시로 변경
type CertKeyPair = (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>);
static CERT_CACHE: Lazy<RwLock<LruCache<String, (CertKeyPair, Instant)>>> = 
    Lazy::new(|| RwLock::new(LruCache::new(NonZeroUsize::new(CERT_CACHE_SIZE).unwrap_or(NonZeroUsize::new(1000).unwrap()))));

// TLS 세션 캐시 추가
static TLS_SESSION_CACHE: Lazy<RwLock<LruCache<String, Vec<u8>>>> =
    Lazy::new(|| RwLock::new(LruCache::new(NonZeroUsize::new(TLS_SESSION_CACHE_SIZE).unwrap_or(NonZeroUsize::new(5000).unwrap()))));

// 클라이언트 TLS 설정 캐시 (재사용을 위함)
static CLIENT_TLS_CONFIGS: Lazy<RwLock<HashMap<bool, Arc<ClientConfig>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// 루트 CA 인증서를 초기화합니다
pub fn init_root_ca() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut ca_guard = ROOT_CA.lock().unwrap();
    
    // 기존 CA 인증서가 있는지 확인
    if Path::new(CA_CERT_FILE).exists() && Path::new(CA_KEY_FILE).exists() {
        info!("Loading existing CA certificate");
        let _cert_pem = fs::read_to_string(CA_CERT_FILE)?;
        let key_pem = fs::read_to_string(CA_KEY_FILE)?;
        
        // PEM에서 인증서와 키 로드
        let key_pair = KeyPair::from_pem(&key_pem)?;
        let mut params = CertificateParams::new(vec![]);
        params.key_pair = Some(key_pair);
        let cert = Certificate::from_params(params)?;
        
        // .crt 파일이 없다면 생성
        if !Path::new(CA_CERT_CRT_FILE).exists() {
            info!("Creating .crt file from existing certificate");
            let der_data = cert.serialize_der()?;
            fs::write(CA_CERT_CRT_FILE, &der_data)?;
        }
        
        *ca_guard = Some(cert);
    } else {
        info!("Generating new CA certificate");
        // 새 CA 인증서 생성
        let mut params = CertificateParams::new(vec![]);
        params.not_before = time::OffsetDateTime::now_utc() - Duration::from_secs(60 * 60 * 24);
        params.not_after = time::OffsetDateTime::now_utc() + Duration::from_secs(60 * 60 * 24 * 365 * 10); // 10년
        
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(DnType::CommonName, "UDSS Proxy Root CA");
        distinguished_name.push(DnType::OrganizationName, "UDSS Proxy");
        params.distinguished_name = distinguished_name;
        
        // CA 인증서로 설정
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        
        // 인증서 생성
        let cert = Certificate::from_params(params)?;
        
        // 인증서와 키를 파일로 저장
        let pem_data = cert.serialize_pem()?;
        fs::write(CA_CERT_FILE, &pem_data)?;
        fs::write(CA_KEY_FILE, cert.serialize_private_key_pem())?;
        
        // DER 형식의 .crt 파일도 생성
        let der_data = cert.serialize_der()?;
        fs::write(CA_CERT_CRT_FILE, &der_data)?;
        
        info!("CA certificate generated and saved to {} and {}", CA_CERT_FILE, CA_CERT_CRT_FILE);
        info!("Please install the CA certificate in your browser's trusted root store");
        
        *ca_guard = Some(cert);
    }
    
    Ok(())
}

/// 호스트명을 기반으로 가짜 인증서를 생성합니다
pub async fn generate_fake_cert(host: &str) -> Result<CertKeyPair, Box<dyn Error + Send + Sync>> {
    // 캐시에서 인증서 확인
    {
        let mut cache = CERT_CACHE.write().unwrap();
        if let Some((cert, created_time)) = cache.get(host) {
            let age = created_time.elapsed();
            
            // 인증서가 만료되지 않았고 유효 기간의 80% 미만인 경우에만 재사용
            if age < Duration::from_secs(60 * 60 * 24 * 365 * 0.8 as u64) {
                debug!("Using cached certificate for host: {} (age: {}s)", host, age.as_secs());
                
                // 깊은 복사 수행 - 인증서 체인은 클론 가능
                let cert_chain = cert.0.clone();
                
                // 개인 키는 직접 복제해야 함
                let private_key = match &cert.1 {
                    PrivateKeyDer::Pkcs8(key) => {
                        let key_data = key.secret_pkcs8_der().to_vec();
                        PrivateKeyDer::Pkcs8(key_data.into())
                    },
                    PrivateKeyDer::Sec1(key) => {
                        let key_data = key.secret_sec1_der().to_vec();
                        PrivateKeyDer::Sec1(key_data.into())
                    },
                    PrivateKeyDer::Pkcs1(key) => {
                        let key_data = key.secret_pkcs1_der().to_vec();
                        PrivateKeyDer::Pkcs1(key_data.into())
                    },
                    _ => return Err("Unsupported private key format".into()),
                };
                
                return Ok((cert_chain, private_key));
            } else {
                debug!("Certificate for {} is too old ({}s), regenerating", host, age.as_secs());
                // 오래된 인증서는 제거하고 새로 생성
                cache.pop(host);
            }
        }
    }
    
    debug!("Generating certificate for host: {}", host);
    
    // 루트 CA 가져오기
    let ca_guard = ROOT_CA.lock().unwrap();
    let ca_cert = ca_guard.as_ref().ok_or_else(|| {
        let err = "Root CA not initialized";
        error!("{}", err);
        std::io::Error::new(std::io::ErrorKind::Other, err)
    })?;
    
    // 도메인 인증서 매개변수 설정
    let mut params = CertificateParams::new(vec![host.to_string()]);
    params.not_before = time::OffsetDateTime::now_utc() - Duration::from_secs(60 * 60 * 24);
    params.not_after = time::OffsetDateTime::now_utc() + Duration::from_secs(60 * 60 * 24 * 365); // 1년
    
    // 주체 이름 설정
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CommonName, host);
    distinguished_name.push(DnType::OrganizationName, "UDSS Proxy Generated");
    params.distinguished_name = distinguished_name;
    
    // SAN(Subject Alternative Name) 추가
    params.subject_alt_names = vec![SanType::DnsName(host.to_string())];
    
    // 인증서 생성
    let cert = Certificate::from_params(params).map_err(|e| {
        error!("Failed to generate certificate: {}", e);
        e
    })?;
    
    // CA로 서명
    let cert_der = cert.serialize_der_with_signer(ca_cert).map_err(|e| {
        error!("Failed to sign certificate with CA: {}", e);
        e
    })?;
    
    let key_der = cert.serialize_private_key_der();
    
    // 인증서 체인 구성 (도메인 인증서 + CA 인증서)
    let ca_cert_der = ca_cert.serialize_der().map_err(|e| {
        error!("Failed to serialize CA certificate: {}", e);
        e
    })?;
    
    let cert_chain = vec![
        CertificateDer::from(cert_der),
        CertificateDer::from(ca_cert_der),
    ];
    
    let private_key = PrivateKeyDer::Pkcs8(key_der.into());
    
    // 인증서를 캐시에 저장 - Instant 추가
    let private_key_for_cache = match &private_key {
        PrivateKeyDer::Pkcs8(key) => {
            let key_data = key.secret_pkcs8_der().to_vec();
            PrivateKeyDer::Pkcs8(key_data.into())
        },
        PrivateKeyDer::Sec1(key) => {
            let key_data = key.secret_sec1_der().to_vec();
            PrivateKeyDer::Sec1(key_data.into())
        },
        PrivateKeyDer::Pkcs1(key) => {
            let key_data = key.secret_pkcs1_der().to_vec();
            PrivateKeyDer::Pkcs1(key_data.into())
        },
        _ => return Err("Unsupported private key format".into()),
    };
    
    let cert_key_pair = (cert_chain.clone(), private_key_for_cache);
    {
        let mut cache = CERT_CACHE.write().unwrap();
        cache.put(host.to_string(), (cert_key_pair, Instant::now()));
    }
    
    Ok((cert_chain, private_key))
}

/// 클라이언트와 TLS 연결을 수립합니다 - 세션 재사용 지원
pub async fn accept_tls_with_cert(
    tcp_stream: TcpStream,
    cert_key_pair: CertKeyPair
) -> Result<ServerTlsStream<TcpStream>, Box<dyn Error + Send + Sync>> {
    let (certs, key) = cert_key_pair;
    
    // 서버 설정 구성 - 세션 재사용 지원
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            error!("Failed to create server config: {}", e);
            e
        })?;
    
    let server_config = Arc::new(server_config);
    let acceptor = TlsAcceptor::from(server_config);
    
    // TLS 핸드셰이크 수행
    let tls_stream = acceptor.accept(tcp_stream).await.map_err(|e| {
        error!("TLS handshake failed: {}", e);
        e
    })?;
    
    Ok(tls_stream)
}

/// 실제 서버와 TLS 연결을 수립합니다 - 세션 재사용 개선
pub async fn connect_tls(host: &str, config: &Config) -> Result<ClientTlsStream<TcpStream>, Box<dyn Error + Send + Sync>> {
    // 캐시된 클라이언트 설정 사용
    let client_config = {
        let configs = CLIENT_TLS_CONFIGS.read().unwrap();
        if let Some(config) = configs.get(&config.tls_verify_certificate) {
            Arc::clone(config)
        } else {
            drop(configs); // 읽기 락 해제
            
            // 새 클라이언트 설정 생성
            let new_config = if config.tls_verify_certificate {
                create_verified_client_config()?
            } else {
                create_unverified_client_config()?
            };
            
            // 캐시에 저장
            let mut configs = CLIENT_TLS_CONFIGS.write().unwrap();
            let config_arc = Arc::new(new_config);
            configs.insert(config.tls_verify_certificate, Arc::clone(&config_arc));
            config_arc
        }
    };
    
    // 호스트명으로 서버 이름 생성 - 문자열 복사하여 'static 라이프타임 문제 해결
    let server_name = host.to_string().try_into()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Invalid DNS name: {}", e)))?;
    
    // TLS 커넥터 생성
    let connector = TlsConnector::from(client_config);
    
    // 세션 캐시에서 기존 세션 검색
    let _session_data = {
        let mut cache = TLS_SESSION_CACHE.write().unwrap();
        cache.get(host).cloned()
    };
    
    // 서버 연결
    let tcp_stream = TcpStream::connect(format!("{}:443", host)).await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to connect to {}: {}", host, e)))?;
    
    // TCP 소켓 최적화
    set_tcp_socket_options(&tcp_stream)?;
    
    // TLS 핸드셰이크
    let tls_stream = connector.connect(server_name, tcp_stream).await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("TLS handshake with {} failed: {}", host, e)))?;
    
    // TODO: TLS 세션 데이터 추출 및 저장 구현
    // 현재 rustls-0.22 는 직접적인 세션 데이터 액세스를 제공하지 않음
    // 향후 세션 추출 메서드가 추가되면 구현
    
    Ok(tls_stream)
}

/// TCP 소켓 최적화 설정을 적용합니다
fn set_tcp_socket_options(stream: &TcpStream) -> Result<(), std::io::Error> {
    use std::os::unix::io::AsRawFd;
    
    let fd = stream.as_raw_fd();
    
    // TCP_NODELAY 설정
    if TCP_NODELAY {
        stream.set_nodelay(true)?;
    }
    
    // TCP_QUICKACK 설정 (Linux 전용)
    #[cfg(target_os = "linux")]
    {
        if TCP_QUICKACK {
            let optval: libc::c_int = 1;
            unsafe {
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_QUICKACK,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&optval) as libc::socklen_t,
                ) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
        }
    }
    
    Ok(())
}

// 인증서 검증이 활성화된 클라이언트 설정 생성
fn create_verified_client_config() -> Result<ClientConfig, Box<dyn Error + Send + Sync>> {
    debug!("TLS certificate verification enabled - using system root certificates");
    
    // 시스템의 루트 인증서 로드
    let mut root_store = rustls::RootCertStore::empty();
    
    let certs = rustls_native_certs::load_native_certs()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to load native certs: {}", e)))?;

    for cert in certs {
        root_store.add(cert)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to add cert to store: {:?}", e)))?;
    }

    debug!("Loaded {} native root certificates", root_store.len());
    
    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    
    Ok(client_config)
}

// 인증서 검증이 비활성화된 클라이언트 설정 생성
fn create_unverified_client_config() -> Result<ClientConfig, Box<dyn Error + Send + Sync>> {
    // 인증서 검증 비활성화
    warn!("TLS certificate verification DISABLED! This is insecure and should only be used for testing.");
    
    // 인증서 검증을 항상 통과시키는 검증기 구현
    use rustls::client::danger::ServerCertVerified;
    use rustls::client::danger::ServerCertVerifier;
    use rustls::pki_types::UnixTime;
    use rustls::DigitallySignedStruct;
    use rustls::SignatureScheme;
    
    // 인증서 검증을 항상 통과시키는 검증기 구현
    #[derive(Debug)]
    struct SkipCertificationVerification;
    
    impl ServerCertVerifier for SkipCertificationVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
        
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            // rustls 0.22.x에서는 이 방식으로 지원되는 서명 방식을 가져옴
            vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
            ]
        }
    }
    
    let client_config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipCertificationVerification))
        .with_no_client_auth();
    
    Ok(client_config)
}
