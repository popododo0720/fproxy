use std::error::Error;
use std::sync::Arc;
use log::{debug, trace};
use tokio_postgres::{Statement, Row};
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
    
    /// 준비된 구문 실행
    pub async fn execute_prepared(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)]
    ) -> Result<u64, Box<dyn Error + Send + Sync>> {
        let client = self.pool.get_client().await?;
        
        trace!("준비된 구문 실행");
        let result = client.execute(statement, params).await?;
        
        Ok(result)
    }
    
    /// 쿼리 실행 후 결과 반환
    pub async fn query<T, F>(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
        row_mapper: F
    ) -> Result<Vec<T>, Box<dyn Error + Send + Sync>> 
    where
        F: Fn(Row) -> Result<T, Box<dyn Error + Send + Sync>>,
    {
        let client = self.pool.get_client().await?;
        
        trace!("쿼리 실행 및 결과 매핑: {}", query);
        let rows = client.query(query, params).await?;
        
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            results.push(row_mapper(row)?);
        }
        
        Ok(results)
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
    
    /// 여러 쿼리를 배치로 실행
    pub async fn execute_batch(
        &self,
        queries: &[String]
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let client = self.pool.get_client().await?;
        
        debug!("배치 쿼리 실행: {} 개의 쿼리", queries.len());
        
        let mut batch = String::new();
        for query in queries {
            batch.push_str(query);
            batch.push_str(";\n");
        }
        
        client.batch_execute(&batch).await?;
        
        Ok(())
    }
    
    /// SQL 파일 실행
    pub async fn execute_sql_file(
        &self,
        file_path: &str
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        use std::fs::File;
        use std::io::Read;
        
        debug!("SQL 파일 실행: {}", file_path);
        
        // 파일 읽기
        let mut file = File::open(file_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        // 클라이언트 가져오기
        let client = self.pool.get_client().await?;
        
        // 실행
        client.batch_execute(&contents).await?;
        
        Ok(())
    }
}

// 전역 함수들 - 이전 코드와의 호환성을 위해 유지
/// 단일 쿼리 실행
pub async fn execute_query(
    query: &str, 
    params: &[&(dyn ToSql + Sync)]
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let executor = QueryExecutor::get_instance().await?;
    executor.execute_query(query, params).await
}

/// 여러 쿼리를 배치로 실행
pub async fn execute_batch(
    queries: &[String]
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let executor = QueryExecutor::get_instance().await?;
    executor.execute_batch(queries).await
} 