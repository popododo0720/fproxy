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
use rustls_pemfile;

use crate::constants::*;
use crate::config::Config;
use crate::error::{Result, tls_err, internal_err};

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
type ConfigKey = (bool, Vec<String>);
static CLIENT_TLS_CONFIGS: Lazy<RwLock<HashMap<ConfigKey, Arc<ClientConfig>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// 루트 CA 인증서를 초기화합니다
pub fn init_root_ca() -> Result<()> {
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
pub async fn generate_fake_cert(host: &str) -> Result<CertKeyPair> {
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
                    _ => return Err(internal_err("Unsupported private key format")),
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
        internal_err(err)
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
    let mut subject_alt_names = vec![SanType::DnsName(host.to_string())];
    
    // IP 주소인 경우 IP SAN 추가
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        info!("IP 주소 인증서 생성: {}", host);
        // SanType::IpAddress는 IpAddr 타입을 직접 받음
        subject_alt_names.push(SanType::IpAddress(ip));
    }
    
    params.subject_alt_names = subject_alt_names;
    
    // 인증서 생성
    let cert = Certificate::from_params(params).map_err(|e| {
        error!("Failed to generate certificate: {}", e);
        tls_err(e)
    })?;
    
    // CA로 서명
    let cert_der = cert.serialize_der_with_signer(ca_cert).map_err(|e| {
        error!("Failed to sign certificate with CA: {}", e);
        tls_err(e)
    })?;
    
    let key_der = cert.serialize_private_key_der();
    
    // 인증서 체인 구성 (도메인 인증서 + CA 인증서)
    let ca_cert_der = ca_cert.serialize_der().map_err(|e| {
        error!("Failed to serialize CA certificate: {}", e);
        tls_err(e)
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
        _ => return Err(internal_err("Unsupported private key format")),
    };
    
    let cert_key_pair = (cert_chain.clone(), private_key_for_cache);
    {
        let mut cache = CERT_CACHE.write().unwrap();
        cache.put(host.to_string(), (cert_key_pair, Instant::now()));
    }
    
    Ok((cert_chain, private_key))
}

/// 클라이언트와 TLS 연결을 수립합니다 - 세션 재사용 지원
pub async fn accept_tls_with_cert(tcp_stream: TcpStream, cert_key_pair: CertKeyPair) -> Result<ServerTlsStream<TcpStream>> {
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
pub async fn connect_tls(host: &str, config: &Config) -> Result<ClientTlsStream<TcpStream>> {
    // 포트 번호가 포함된 경우 분리
    let (host_only, port) = if let Some(idx) = host.rfind(':') {
        let (h, p) = host.split_at(idx);
        (h, p[1..].parse::<u16>().unwrap_or(443))
    } else {
        (host, 443)
    };
    
    // 추가 디버그 로그
    info!("TLS 연결 시도: {}:{}, 인증서 검증: {}", host_only, port, if config.tls_verify_certificate { "활성화" } else { "비활성화" });
    if !config.tls_verify_certificate {
        info!("인증서 검증이 비활성화되어 있으나 연결 실패할 경우 디버그를 위해 추가 설정을 확인하세요");
    } else {
        info!("신뢰할 인증서 목록: {:?}", config.trusted_certificates);
    }

    // 내부 IP 주소에 대한 인증서 검증 설정에 따라 처리
    let should_verify = if config.disable_verify_internal_ip && is_internal_ip(host_only) {
        info!("내부 IP 주소 ({})에 대해 인증서 검증이 자동으로 비활성화되었습니다", host_only);
        false
    } else {
        config.tls_verify_certificate
    };

    // 캐시된 클라이언트 설정 사용
    let client_config = {
        let configs = CLIENT_TLS_CONFIGS.read().unwrap();
        if let Some(cached_config) = configs.get(&(should_verify, config.trusted_certificates.clone())) {
            Arc::clone(cached_config)
        } else {
            drop(configs); // 읽기 락 해제
            
            // 설정값에 따라 클라이언트 설정 생성
            let new_config = if should_verify {
                info!("TLS certificate verification enabled for host: {}", host_only);
                create_verified_client_config(config)?
            } else {
                info!("TLS certificate verification disabled for host: {}", host_only);
                create_unverified_client_config()?
            };
            
            // 캐시에 저장
            let mut configs = CLIENT_TLS_CONFIGS.write().unwrap();
            let config_arc = Arc::new(new_config);
            configs.insert((should_verify, config.trusted_certificates.clone()), Arc::clone(&config_arc));
            config_arc
        }
    };
    
    // 호스트명으로 서버 이름 생성 - 문자열 복사하여 'static 라이프타임 문제 해결
    let server_name = host_only.to_string().try_into()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Invalid DNS name: {}", e)))?;
    
    // TLS 커넥터 생성
    let connector = TlsConnector::from(client_config);
    
    // 세션 캐시에서 기존 세션 검색
    let _session_data = {
        let mut cache = TLS_SESSION_CACHE.write().unwrap();
        cache.get(host_only).cloned()
    };
    
    // 서버 연결 - 포트 번호 사용
    let tcp_stream = TcpStream::connect(format!("{}:{}", host_only, port)).await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to connect to {}:{}: {}", host_only, port, e)))?;
    
    // TCP 소켓 최적화
    set_tcp_socket_options(&tcp_stream)?;
    
    // TLS 핸드셰이크
    let tls_stream = connector.connect(server_name, tcp_stream).await
        .map_err(|e| {
            if !config.tls_verify_certificate {
                error!("TLS handshake failed even with verification disabled: {}", e);
                error!("상세 오류: {:?}", e);
                error!("이 경우 서버 측에서 지원하는 TLS 버전이나 암호화 알고리즘이 호환되지 않을 수 있습니다.");
                error!("서버 로그를 확인하고, 서버 TLS 설정을 점검하세요.");
            } else {
                error!("TLS handshake failed with verification enabled: {}", e);
            }
            std::io::Error::new(std::io::ErrorKind::Other, format!("TLS handshake with {}:{} failed: {}", host_only, port, e))
        })?;
    
    Ok(tls_stream)
}

/// TCP 소켓 최적화 설정을 적용합니다
fn set_tcp_socket_options(stream: &TcpStream) -> std::result::Result<(), std::io::Error> {
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
fn create_verified_client_config(config: &Config) -> Result<ClientConfig> {
    debug!("TLS certificate verification enabled - using system root certificates");
    
    // 시스템의 루트 인증서 로드
    let mut root_store = rustls::RootCertStore::empty();
    
    // 1. 시스템 인증서 로드
    let certs = rustls_native_certs::load_native_certs()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to load native certs: {}", e)))?;

    let mut cert_count = 0;
    for cert in certs {
        if root_store.add(cert).is_ok() {
            cert_count += 1;
        }
    }
    debug!("Loaded {} native root certificates", cert_count);
    
    // 2. 사용자 지정 인증서 로드 (추가 신뢰 인증서)
    let mut custom_cert_count = 0;
    for cert_path in &config.trusted_certificates {
        debug!("Loading custom certificate from: {}", cert_path);
        
        // PEM 또는 DER 형식 인증서 로드 시도
        if let Ok(cert_data) = fs::read(cert_path) {
            if cert_path.ends_with(".pem") || cert_path.ends_with(".crt") {
                // PEM 형식 처리
                let mut cert_data_slice = cert_data.as_slice();
                let cert_iter = rustls_pemfile::certs(&mut cert_data_slice);
                for cert_result in cert_iter {
                    if let Ok(cert) = cert_result {
                        if root_store.add(cert).is_ok() {
                            custom_cert_count += 1;
                        }
                    }
                }
            } else {
                // DER 형식으로 가정하고 처리 시도
                if let Ok(_) = root_store.add(CertificateDer::from(cert_data)) {
                    custom_cert_count += 1;
                }
            }
        } else {
            warn!("Failed to read certificate file: {}", cert_path);
        }
    }
    
    if custom_cert_count > 0 {
        info!("Loaded {} additional trusted certificates", custom_cert_count);
    }

    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    
    Ok(client_config)
}

// 인증서 검증이 비활성화된 클라이언트 설정 생성
fn create_unverified_client_config() -> Result<ClientConfig> {
    // 인증서 검증 비활성화
    warn!("TLS certificate verification COMPLETELY DISABLED! All certificates will be trusted.");
    info!("인증서 검증 비활성화 모드로 TLS 설정 생성 중...");
    
    // 모든 인증서를 항상 신뢰하는 검증기 구현
    use rustls::client::danger::ServerCertVerifier;
    use rustls::client::danger::{ServerCertVerified, HandshakeSignatureValid};
    use rustls::pki_types::ServerName;
    use rustls::pki_types::UnixTime;
    use rustls::DigitallySignedStruct;
    use rustls::SignatureScheme;
    
    // 모든 인증서를 항상 무조건 신뢰하는 검증기 구현
    #[derive(Debug)]
    struct NoCertificateVerification;
    
    impl ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> std::result::Result<ServerCertVerified, rustls::Error> {
            // 무조건 통과
            info!("인증서 검증 비활성화: 서버 {:?} 인증서 검증 없이 통과 처리됨", server_name);
            Ok(ServerCertVerified::assertion())
        }
        
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            // 무조건 통과
            Ok(HandshakeSignatureValid::assertion())
        }
        
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            // 무조건 통과
            Ok(HandshakeSignatureValid::assertion())
        }
        
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            // 모든 지원 가능한 서명 방식 반환
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
    
    // 모든 TLS 버전 지원 (TLS 1.2/1.3)
    info!("모든 TLS 버전 지원 활성화 (TLS 1.2/1.3)");
    
    // rustls 0.23 버전에 맞는 방식으로 TLS 버전 설정
    // ClientConfig 생성 - 간단한 방식으로 변경
    let client_config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoCertificateVerification {}))
        .with_no_client_auth();
    
    // 직접 versions 필드에 접근하지 않고, 대신 기본 설정 사용
    // TLS 1.3과 TLS 1.2가 기본적으로 지원됨
    
    info!("인증서 검증 비활성화 모드로 클라이언트 설정 생성 완료");
    Ok(client_config)
}

/// ssl/trusted_certs 폴더에서 인증서를 자동으로 로드합니다.
pub fn load_trusted_certificates(config: &mut Config) -> Result<()> {
    let trusted_certs_dir = format!("{}/trusted_certs", config.ssl_dir);
    let trusted_certs_path = Path::new(&trusted_certs_dir);
    
    // 기존 trusted_certificates 배열 초기화 (폴더에서 모든 인증서를 다시 로드)
    config.trusted_certificates.clear();
    
    if !trusted_certs_path.exists() {
        debug!("신뢰할 인증서 폴더가 존재하지 않습니다: {}", trusted_certs_dir);
        if let Err(e) = std::fs::create_dir_all(trusted_certs_path) {
            warn!("신뢰할 인증서 폴더 생성 실패: {}", e);
        } else {
            info!("신뢰할 인증서 폴더 생성됨: {}", trusted_certs_dir);
        }
        return Ok(());
    }
    
    // 폴더 내의 모든 파일 검색
    if let Ok(entries) = std::fs::read_dir(trusted_certs_path) {
        let mut loaded_count = 0;
        
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                
                // 파일만 처리
                if path.is_file() {
                    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
                    
                    // .pem, .crt, .cer 파일만 처리
                    if extension == "pem" || extension == "crt" || extension == "cer" {
                        let path_str = path.to_string_lossy().to_string();
                        
                        // 설정에 추가
                        config.trusted_certificates.push(path_str.clone());
                        loaded_count += 1;
                        debug!("신뢰할 인증서 추가됨: {}", path_str);
                    }
                }
            }
        }
        
        if loaded_count > 0 {
            info!("ssl/trusted_certs에서 {} 개의 인증서가 자동으로 로드되었습니다", loaded_count);
        } else {
            debug!("ssl/trusted_certs 폴더에서 로드할 인증서가 없습니다");
        }
    }
    
    Ok(())
}

/// 주어진 호스트가 내부 IP 주소인지 확인합니다
fn is_internal_ip(host: &str) -> bool {
    debug!("is_internal_ip 확인: {}", host);
    
    // IP 주소 형식인지 확인
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        debug!("호스트 {}는 IP 주소 형식입니다: {:?}", host, ip);
        
        // IPv4 주소 확인
        if let std::net::IpAddr::V4(ipv4) = ip {
            // 사설 IP 범위 확인
            // 10.0.0.0/8
            if ipv4.octets()[0] == 10 {
                debug!("호스트 {}는 10.0.0.0/8 범위의 내부 IP입니다", host);
                return true;
            }
            
            // 172.16.0.0/12
            if ipv4.octets()[0] == 172 && (ipv4.octets()[1] >= 16 && ipv4.octets()[1] <= 31) {
                debug!("호스트 {}는 172.16.0.0/12 범위의 내부 IP입니다", host);
                return true;
            }
            
            // 192.168.0.0/16
            if ipv4.octets()[0] == 192 && ipv4.octets()[1] == 168 {
                debug!("호스트 {}는 192.168.0.0/16 범위의 내부 IP입니다", host);
                return true;
            }
            
            // 127.0.0.0/8 (로컬호스트)
            if ipv4.octets()[0] == 127 {
                debug!("호스트 {}는 127.0.0.0/8 범위의 로컬호스트 IP입니다", host);
                return true;
            }
            
            debug!("호스트 {}는 내부 IP 범위에 포함되지 않습니다", host);
        }
        
        // IPv6 로컬 주소 확인
        if let std::net::IpAddr::V6(ipv6) = ip {
            // ::1 (IPv6 로컬호스트)
            if ipv6.is_loopback() {
                debug!("호스트 {}는 IPv6 로컬호스트 주소입니다", host);
                return true;
            }
            
            // fe80::/10 (링크 로컬)
            if ipv6.is_unicast_link_local() {
                debug!("호스트 {}는 IPv6 링크 로컬 주소입니다", host);
                return true;
            }
            
            debug!("호스트 {}는 내부 IPv6 범위에 포함되지 않습니다", host);
        }
    } else {
        debug!("호스트 {}는 IP 주소 형식이 아닙니다", host);
    }
    
    // IP 주소가 아니라면 호스트명 확인
    // localhost 또는 .local 도메인 확인
    if host == "localhost" || host.ends_with(".local") {
        debug!("호스트 {}는 내부 도메인 이름입니다", host);
        return true;
    }
    
    debug!("호스트 {}는 내부 IP 또는 내부 도메인으로 인식되지 않습니다", host);
    false
}
