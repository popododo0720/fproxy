# 로그 관리 시스템

PostgreSQL을 사용한 고성능 로그 관리 시스템입니다. 이 시스템은 객체지향적 설계와 함수형 스타일을 적용하여 로그 데이터를 효율적으로 관리합니다.

## 주요 기능

- 요청 로그 및 차단 로그 관리
- 자동 파티션 관리 (시간 기반 파티셔닝)
- 설정 파일 기반 구성 (YAML)
- 이벤트 트리거를 통한 자동 파티션 생성 및 삭제
- 성능 최적화된 인덱싱
- 유용한 통계 및 분석 뷰

## 설치 방법

1. PostgreSQL 데이터베이스 서버가 설치되어 있어야 합니다.
2. `db.yml` 파일을 설정합니다.
3. 초기화 스크립트를 실행합니다:

```bash
psql -U postgres -d your_database -f init.sql
```

## 설정 파일 (db.yml)

```yaml
# PostgreSQL 데이터베이스 접속 정보
connection:
  host: localhost
  port: 5432
  database: logs_db
  user: postgres
  password: your_password_here
  sslmode: prefer

# 로그 파티션 설정
partitioning:
  # 파티션 생성 주기 (일)
  creation_interval: 30
  # 파티션 보관 기간 (일)
  retention_period: 90
  # 미리 생성할 파티션 수
  future_partitions: 2
```

## 주요 테이블

- `logs.request_logs`: 요청 로그를 저장합니다.
- `logs.reject_logs`: 차단된 요청 로그를 저장합니다.
- `logs.partition_config`: 파티션 관리 설정을 저장합니다.

## 주요 함수

- `logs.initialize_partitions()`: 초기 파티션을 생성합니다.
- `logs.manage_partitions()`: 파티션을 관리합니다 (생성 및 삭제).
- `logs.get_partition_status()`: 파티션 상태를 확인합니다.
- `logs.run_partition_maintenance()`: 수동으로 파티션 관리를 실행합니다.
- `logs.update_partition_config_from_yaml()`: YAML 파일에서 설정을 업데이트합니다.

## 자동 파티션 관리

이 시스템은 다음과 같은 방식으로 파티션을 자동으로 관리합니다:

1. 데이터베이스 이벤트 트리거: 테이블 생성/변경/삭제 시 파티션 관리 함수가 실행됩니다.
2. 로그 삽입 트리거: 로그 삽입 시 일정 확률(1%)로 파티션 관리 함수가 실행됩니다.
3. 설정 기반 관리: `db.yml` 파일에서 파티션 생성 주기, 보관 기간 등을 설정할 수 있습니다.

## 사용 예시

### 로그 삽입

```sql
-- 요청 로그 삽입
INSERT INTO logs.request_logs 
    (host, content, timestamp, session_id, request_path, request_method, response_code, response_time)
VALUES 
    ('example.com', '요청 내용', NOW(), 'session-123', '/api/data', 'GET', 200, 0.123);

-- 차단 로그 삽입
INSERT INTO logs.reject_logs 
    (host, ip, content, timestamp, session_id, reason, block_rule)
VALUES 
    ('example.com', '192.168.1.1', '차단된 요청', NOW(), 'session-456', '악성 트래픽', 'RULE-001');
```

### 파티션 상태 확인

```sql
-- 파티션 상태 확인
SELECT * FROM logs.get_partition_status();

-- 파티션 통계 확인
SELECT * FROM logs.collect_partition_stats();
```

### 수동 파티션 관리

```sql
-- 수동으로 파티션 관리 실행
SELECT logs.run_partition_maintenance();

-- 설정 업데이트 후 파티션 관리 실행
SELECT logs.update_partition_config_from_yaml('db.yml');
SELECT logs.run_partition_maintenance();
```

## 성능 최적화

- 모든 파티션에는 자동으로 인덱스가 생성됩니다.
- 파티션은 날짜 기반으로 관리되어 쿼리 성능을 향상시킵니다.
- 오래된 데이터는 자동으로 삭제되어 데이터베이스 크기를 관리합니다.
- 통계 뷰를 통해 로그 데이터를 효율적으로 분석할 수 있습니다. 