use std::error::Error;
use std::sync::Arc;
use log::trace;
use tokio_postgres::Row;
use tokio_postgres::types::ToSql;

use super::pool::{get_db_pool, DatabasePool};

/// 쿼리 실행기 구조체
pub struct QueryExecutor {
    pool: Arc<DatabasePool>,
}

impl QueryExecutor {
    /// 새 QueryExecutor 인스턴스 생성
    pub async fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let pool = get_db_pool().await?;
        Ok(Self { pool })
    }
    
    /// 전역 인스턴스 가져오기
    pub async fn get_instance() -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new().await
    }
    
    /// 단일 쿼리 실행
    pub async fn execute_query(
        &self,
        query: &str, 
        params: &[&(dyn ToSql + Sync)]
    ) -> Result<u64, Box<dyn Error + Send + Sync>> {
        let client = self.pool.get_client().await?;
        
        trace!("쿼리 실행: {}", query);
        let result = client.execute(query, params).await?;
        
        Ok(result)
    }
    
    /// 단일 행 쿼리 실행 후 결과 반환
    pub async fn query_one<T, F>(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
        row_mapper: F
    ) -> Result<T, Box<dyn Error + Send + Sync>> 
    where
        F: Fn(Row) -> Result<T, Box<dyn Error + Send + Sync>>,
    {
        let client = self.pool.get_client().await?;
        
        trace!("단일 행 쿼리 실행: {}", query);
        let row = client.query_one(query, params).await?;
        
        row_mapper(row)
    }
} 