// 데이터베이스 관리 모듈
// 데이터베이스 연결, 설정 로드 및 쿼리 실행을 담당합니다.

pub mod config;
pub mod pool;
pub mod partition;
pub mod query;

// 외부로 노출할 항목들
pub use partition::ensure_partitions; // ensure_partitions는 여러 곳에서 사용 