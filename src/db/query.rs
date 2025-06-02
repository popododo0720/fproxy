use std::error::Error;
use std::sync::Arc;
use log::debug;
use tokio_postgres::Row;
use tokio_postgres::types::ToSql;

use crate::db::pool::{DatabasePool, get_db_pool};

/// 쿼리 실행기 구조체
pub struct QueryExecutor {
    pub pool: Arc<DatabasePool>,
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
    
    /// DB 클라이언트 가져오기
    pub async fn get_client(&self) -> Result<deadpool_postgres::Client, Box<dyn Error + Send + Sync>> {
        // 풀에서 클라이언트 객체 가져오기
        let pool_client = self.pool.get_client().await?;
        
        // deadpool 클라이언트를 반환
        Ok(pool_client)
    }
    
    /// 단일 쿼리 실행
    pub async fn execute_query(
        &self,
        query: &str, 
        params: &[&(dyn ToSql + Sync)]
    ) -> Result<u64, Box<dyn Error + Send + Sync>> {
        let client = self.pool.get_client().await?;
        
        debug!("쿼리 실행: {}", query);
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
        
        debug!("단일 행 쿼리 실행: {}", query);
        let row = client.query_one(query, params).await?;
        
        row_mapper(row)
    }
    
    /// 여러 행 쿼리 실행
    pub async fn query_rows<T, F>(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
        row_mapper: F
    ) -> Result<Vec<T>, Box<dyn Error + Send + Sync>> 
    where
        F: Fn(Row) -> Result<T, Box<dyn Error + Send + Sync>>,
    {
        let client = self.pool.get_client().await?;
        
        debug!("다중 행 쿼리 실행: {}", query);
        let rows = client.query(query, params).await?;
        
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            results.push(row_mapper(row)?);
        }
        
        Ok(results)
    }
}