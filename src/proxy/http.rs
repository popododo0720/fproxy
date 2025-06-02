use std::sync::Arc;
use tokio::net::TcpStream;
use log::{debug, error, info};
use std::time::Instant;

use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::logging::Logger;
use crate::metrics::Metrics;
use crate::proxy::server::HttpSession;
use crate::acl::domain_blocker::DomainBlocker;

/// 개선된 HTTP 프록시 함수 - 오류 처리 및 로깅 강화
pub async fn proxy_http_streams(
    client_stream: TcpStream,
    server_stream: TcpStream,
    metrics: Arc<Metrics>,
    session_id: &str,
    config: Option<Arc<Config>>,
    logger: Option<Arc<Logger>>,
    domain_blocker: Option<Arc<DomainBlocker>>,
) -> Result<()> {
    // 세션 시작 시간 기록
    let start_time = Instant::now();
    
    // 세션 ID를 문자열로 복제하여 일관된 사용 보장
    let session_id_str = session_id.to_string();
    
    // 소켓 설정 최적화 - TCP_NODELAY 설정
    if let Err(e) = client_stream.set_nodelay(true) {
        debug!("[Session:{}] 클라이언트 소켓 TCP_NODELAY 설정 실패: {}", session_id, e);
        // 오류를 무시하고 계속 진행
    }
    
    if let Err(e) = server_stream.set_nodelay(true) {
        debug!("[Session:{}] 서버 소켓 TCP_NODELAY 설정 실패: {}", session_id, e);
        // 오류를 무시하고 계속 진행
    }
    
    info!("[Session:{}] HTTP 프록시 세션 시작", session_id);
    
    // HTTP 세션 생성 (가변으로 선언)
    let mut session = HttpSession::new(
        client_stream,
        server_stream,
        session_id_str.clone(),
        config,
        logger.clone(),
        metrics.clone(),
        domain_blocker,
    );
    
    // 세션 처리 시도
    match session.process().await {
        Ok(_) => {
            // 성공적인 세션 완료
            let elapsed = start_time.elapsed().as_millis() as u64;
            metrics.record_response_time(elapsed);
            info!("[Session:{}] HTTP 프록시 세션 완료 (소요시간: {}ms)", session_id, elapsed);
            Ok(())
        },
        Err(e) => {
            // 세션 처리 중 오류 발생
            error!("[Session:{}] HTTP 프록시 세션 오류: {}", session_id, e);
            
            // 오류 로깅 - 로그 라이브러리를 통해 직접 로깅
            error!("[Session:{}] HTTP 프록시 세션 오류 상세: {}", session_id, e);
            
            // 메트릭에 오류 카운트 증가
            metrics.increment_error_count();
            
            Err(ProxyError::Http(format!("HTTP 프록시 세션 오류: {}", e)))
        }
    }
}
