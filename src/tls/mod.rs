use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::collections::HashMap;

use log::{debug, error, info, warn};
use rcgen::{Certificate, CertificateParams, DistinguishedName, DnType, SanType, KeyPair};
use rustls::{ServerConfig, ClientConfig};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector, server::TlsStream as ServerTlsStream, client::TlsStream as ClientTlsStream};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use once_cell::sync::Lazy;

use crate::constants::*;
use crate::config::Config;

// 루트 CA 인증서와 키를 저장하는 전역 변수
static ROOT_CA: Lazy<Mutex<Option<Certificate>>> = Lazy::new(|| Mutex::new(None));

// 도메인별 인증서 캐시
type CertKeyPair = (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>);
static CERT_CACHE: Lazy<Mutex<HashMap<String, CertKeyPair>>> = 
    Lazy::new(|| Mutex::new(HashMap::new()));

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
        let cache = CERT_CACHE.lock().unwrap();
        if let Some(cert) = cache.get(host) {
            debug!("Using cached certificate for host: {}", host);
            
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
    
    // // 인증서를 캐시에 저장 - 복사본 저장
    // {
    //     let mut cache = CERT_CACHE.lock().unwrap();
    //
    //     // 키 복제
    //     let private_key_to_cache = match &private_key {
    //         PrivateKeyDer::Pkcs8(key) => {
    //             let key_data = key.secret_pkcs8_der().to_vec();
    //             PrivateKeyDer::Pkcs8(key_data.into())
    //         },
    //         PrivateKeyDer::Sec1(key) => {
    //             let key_data = key.secret_sec1_der().to_vec();
    //             PrivateKeyDer::Sec1(key_data.into())
    //         },
    //         PrivateKeyDer::Pkcs1(key) => {
    //             let key_data = key.secret_pkcs1_der().to_vec();
    //             PrivateKeyDer::Pkcs1(key_data.into())
    //         },
    //         _ => return Err("Unsupported private key format".into()),
    //     };
    //
    //     cache.insert(host.to_string(), (cert_chain.clone(), private_key_to_cache));
    // }
    
    Ok((cert_chain, private_key))
}

/// 클라이언트와 TLS 연결을 수립합니다
pub async fn accept_tls_with_cert(
    tcp_stream: TcpStream,
    cert_key_pair: CertKeyPair
) -> Result<ServerTlsStream<TcpStream>, Box<dyn Error + Send + Sync>> {
    let (certs, key) = cert_key_pair;
    
    // 서버 설정 구성
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            error!("Failed to create server config: {}", e);
            e
        })?;
    
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    
    // TLS 핸드셰이크 수행
    let tls_stream = acceptor.accept(tcp_stream).await.map_err(|e| {
        error!("TLS handshake failed: {}", e);
        e
    })?;
    
    Ok(tls_stream)
}

/// 실제 서버와 TLS 연결을 수립합니다
pub async fn connect_tls(host: &str, config: &Config) -> Result<ClientTlsStream<TcpStream>, Box<dyn Error + Send + Sync>> {
    // 클라이언트 설정 구성
    let client_config = if config.tls_verify_certificate {
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
        
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    } else {
        // 인증서 검증 비활성화
        warn!("TLS certificate verification DISABLED! This is insecure and should only be used for testing.");
        
        // rustls 0.22.x 버전에서는 다음과 같이 인증서 검증을 비활성화
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
        
        let mut config = ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
            
        // 인증서 검증을 건너뛰는 커스텀 검증기 설정
        config.dangerous().set_certificate_verifier(Arc::new(SkipCertificationVerification));
        
        config
    };
    
    let connector = TlsConnector::from(Arc::new(client_config));
    
    // 서버에 연결
    let tcp_stream = TcpStream::connect(format!("{}:443", host)).await?;
    
    // TLS 핸드셰이크 수행
    let domain = rustls::pki_types::ServerName::try_from(host)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid DNS name"))?
        .to_owned();
    
    let tls_stream = connector.connect(domain, tcp_stream).await.map_err(|e| {
        // UnknownIssuer 오류인 경우 더 자세한 설명 로깅
        if e.to_string().contains("UnknownIssuer") {
            error!("TLS connection to server '{}' failed because the server's certificate is not trusted. You can disable certificate verification for testing purposes by setting tls_verify_certificate=false in your config.", host);
        } else {
            error!("TLS connection to server '{}' failed: {}", host, e);
        }
        e
    })?;
    
    Ok(tls_stream)
}
