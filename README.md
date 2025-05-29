# UDSS Proxy

UDSS Proxy는 HTTP/HTTPS 트래픽을 중개하고 모니터링하는 프록시 서버입니다. 
도메인 기반 접근 제어, TLS 인증서 관리, 트래픽 모니터링 등의 기능을 제공합니다.

## 주요 기능

- HTTP/HTTPS 프록시 기능
- 도메인 기반 접근 제어 (데이터베이스 차단 목록)
- 실시간 트래픽 모니터링
- TLS 인증서 자동 생성 및 관리
- 내부 IP 주소에 대한 인증서 검증 옵션
- 버퍼 풀 최적화로 메모리 사용 효율화

## 설치 및 실행

### 요구 사항
- Rust 1.70 이상
- PostgreSQL 데이터베이스 (선택 사항)

### 실행 방법
```bash
cargo build --release
./target/release/udss-proxy
```

## 설정

`config.yml` 파일을 통해 프록시 서버를 설정할 수 있습니다. 설정 파일이 없는 경우 기본 설정이 사용됩니다.

### 기본 설정
```yaml
bind_host: "0.0.0.0"
bind_port: 50000
buffer_size: 32768 # 16kb 이상 사용 권장
timeout_ms: 60000   # 60초
ssl_dir: "ssl"
worker_threads: null  # null - 시스템 코어 수만큼 사용
cache_enabled: true
cache_size: 1000    # 최대 캐시 항목 수
cache_ttl_seconds: 300  # 캐시 항목 유효 시간
tls_verify_certificate: true  # TLS 인증서 검증 활성화/비활성화
disable_verify_internal_ip: true  # 내부 IP에 대한 인증서 검증 비활성화 여부
access_control: {}
blocked_domains: []
blocked_patterns: []
```

### 환경 변수
- `CONFIG_FILE`: 설정 파일 경로 지정
- `FD_LIMIT`: 파일 디스크립터 제한 설정 (기본값: 100,000)

## TLS 인증서 관리

UDSS Proxy는 HTTPS 연결을 중개하기 위해 자체 서명된 루트 CA 인증서를 생성합니다. 이 인증서는 `ssl/` 경로에 저장됩니다.

### 인증서 문제 해결

HTTPS 사이트 접속 시 인증서 오류가 발생하는 경우:

1. **클라이언트에 루트 CA 인증서 설치**
   - `ssl/ca_cert.pem` 또는 `ssl/ca_cert.crt` 파일을 클라이언트의 신뢰할 수 있는 루트 인증 기관에 설치

2. **특정 사이트의 인증서 검증 오류 해결**
   - 해당 사이트의 인증서를 브라우저에서 내보내기 (PEM 또는 CRT 형식)
   - 내보낸 인증서를 `ssl/trusted_certs/` 디렉토리에 복사
   - 서버 재시작하여 인증서 로드

3. **인증서 검증 비활성화**
   - `config.yml` 파일에서 `tls_verify_certificate: false`로 설정
   - 이 방법은 보안상 권장되지 않으며, 테스트 환경에서만 사용하세요

## 도메인 차단 설정

도메인 차단은 정규표현식을 따르며 두 가지 방법으로 설정할 수 있습니다:

1. **설정 파일 사용**
   ```yaml
   blocked_domains:
     - "example.com"
     - "ads.example.net"
   
   blocked_patterns:
     - ".*ads.*"
     - "tracker.*"
   ```

2. **데이터베이스 사용**
   - `domain_blocks` 테이블과 `domain_pattern_blocks` 테이블에 차단할 도메인 정보 저장
   - 서버가 시작될 때 자동으로 데이터베이스에서 차단 목록을 로드

## 문제 해결

### TLS 핸드셰이크 오류 (CertificateUnknown)

이 오류는 클라이언트가 프록시의 인증서를 신뢰하지 않을 때 발생합니다.

**해결 방법:**
1. 프록시 서버의 루트 CA 인증서(`ssl/ca_cert.pem`)를 클라이언트 시스템에 설치
2. 설정 파일에서 `tls_verify_certificate: false`로 설정 (테스트 환경에서만 권장)
3. 특정 사이트의 인증서를 `ssl/trusted_certs/` 디렉토리에 추가

### 내부 IP 주소 인증서 검증 오류

내부 IP 주소에 대한 인증서 검증 문제는 `disable_verify_internal_ip: true` 설정으로 해결할 수 있습니다.

## 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.
