use chrono::{DateTime, Utc, Datelike};
use std::fmt::Write;

/// 로그 포맷터 - 로그 데이터 형식 변환 담당
pub struct LogFormatter;

impl LogFormatter {
    /// 요청 헤더에서 중요 정보 추출
    pub fn extract_headers(headers: &str) -> Vec<(String, String)> {
        let mut result = Vec::new();
        
        for line in headers.lines() {
            if line.is_empty() || !line.contains(':') {
                continue;
            }
            
            if let Some(idx) = line.find(':') {
                let key = line[0..idx].trim().to_lowercase();
                let value = line[idx+1..].trim().to_string();
                
                // 중요 헤더만 추출
                if ["user-agent", "referer", "origin", "host", "content-type", "content-length"]
                    .contains(&key.as_str()) {
                    result.push((key, value));
                }
            }
        }
        
        result
    }
    
    /// 헤더 문자열에서 Content-Length 추출
    pub fn extract_content_length(headers: &str) -> Option<usize> {
        for line in headers.lines() {
            if line.to_lowercase().starts_with("content-length:") {
                let value = line.split(':').nth(1)?.trim();
                return value.parse::<usize>().ok();
            }
        }
        None
    }
    
    /// 요청 본문 요약 (너무 큰 경우 일부만 저장)
    pub fn summarize_body(body: &str, max_length: usize) -> String {
        if body.len() <= max_length {
            body.to_string()
        } else {
            format!("{}... [truncated {} bytes]", 
                &body[0..max_length], 
                body.len() - max_length
            )
        }
    }
    
    /// 날짜를 파티션 이름 형식으로 변환 (YYYY_MM_DD)
    pub fn format_date_for_partition(date: &DateTime<Utc>) -> String {
        format!("{:04}_{:02}_{:02}", 
            date.year(), 
            date.month(), 
            date.day()
        )
    }
    
    /// 로그 메시지에서 중요 정보 추출
    pub fn extract_important_info(log_message: &str) -> String {
        let mut result = String::new();
        
        // 첫 줄 (요청 라인)
        if let Some(first_line) = log_message.lines().next() {
            let _ = writeln!(result, "Request: {}", first_line);
        }
        
        // 호스트 헤더
        for line in log_message.lines() {
            if line.to_lowercase().starts_with("host:") {
                let _ = writeln!(result, "Host: {}", line.split(':').nth(1).unwrap_or("").trim());
                break;
            }
        }
        
        result
    }
} 