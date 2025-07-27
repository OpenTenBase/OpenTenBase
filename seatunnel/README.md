# OpenTenBase Integration with Apache SeaTunnel

## Overview

This document describes how to integrate OpenTenBase with Apache SeaTunnel to achieve efficient data transformation and synchronization between OpenTenBase and other data sources.

## Technical Architecture

```
OpenTenBase (PostgreSQL Compatible) ←→ Apache SeaTunnel ←→ MySQL/Other Data Sources
    Distributed HTAP Database            Data Integration Platform    Target Data Sources
```

## Deployment Steps

### 1. Deploy Apache SeaTunnel

1. Download SeaTunnel 2.3.11 release package, refer to [Apache Seatunnel quickstart](https://seatunnel.incubator.apache.org/zh-CN/docs/2.3.11/start-v2/locally/deployment)

2. Install necessary JDBC drivers:
```bash
cd $SEATUNNEL_HOME/lib

# Download PostgreSQL JDBC driver (compatible with OpenTenBase)
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.24/postgresql-42.2.24.jar

# Download MySQL JDBC driver
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar
```

### 2. Deploy OpenTenBase Cluster

1. Start the OpenTenBase cluster according to the instructions in `opentenbase/example/1c_2d_cluster`
2. Configure DN port mapping in `example/1c_2d_cluster/docker-compose.yaml`

### 3. Prepare Data Structures

#### 3.1 OpenTenBase Data Structure

```sql
-- Connect to OpenTenBase
psql -h localhost -p 30004 -U opentenbase -d postgres

-- Create basic test table
CREATE TABLE foo(id bigint, str text) DISTRIBUTE BY shard(id);
INSERT INTO foo VALUES(1, 'tencent'), (2, 'shenzhen');

-- Create complex data type test table
CREATE TABLE test_data_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INTEGER,
    salary DECIMAL(10,2),
    birth_date DATE,
    created_time TIMESTAMP,
    is_active BOOLEAN,
    description TEXT,
    metadata JSONB
) DISTRIBUTE BY shard(id);

INSERT INTO test_data_types (name, age, salary, birth_date, created_time, is_active, description, metadata)
VALUES 
    ('zhangsan', 25, 8500.50, '1998-05-15', '2024-01-01 10:30:00', true, 'test user 1', '{"dept": "IT", "level": "junior"}'),
    ('lisi', 30, 12000.00, '1993-12-20', '2024-01-02 14:20:00', true, 'test user 2', '{"dept": "Sales", "level": "senior"}'),
    ('wangwu', 28, 9500.75, '1995-08-10', '2024-01-03 09:15:00', false, 'test user 3', '{"dept": "HR", "level": "middle"}');
```

#### 3.2 MySQL Data Structure

```sql
-- Connect to MySQL
mysql -u root -p

-- Create test database
CREATE DATABASE test;
USE test;

-- Create corresponding target table
CREATE TABLE foo (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    value INT
);

-- Create complex data type target table
CREATE TABLE test_data_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    salary DECIMAL(10,2),
    level VARCHAR(20),
    birth_year INT
);
```

### 4. Run SeaTunnel Tasks

```bash
cd apache-seatunnel-2.3.11

# Run basic synchronization task
./bin/seatunnel.sh --config ../seatunnel/jobs/opentenbase_basic_test.conf -e local

# Run data transformation task
./bin/seatunnel.sh --config ../seatunnel/jobs/opentenbase_transform_test.conf -e local
```

### 5. Verify Results

```sql
-- Connect to MySQL to verify data
mysql -u root -p test

-- Check basic synchronization results
SELECT * FROM foo;

-- Check data transformation results
SELECT * FROM test_data_types;
```

## Test Scenarios

### Basic Data Synchronization Test

**Configuration File**: `opentenbase_basic_test.conf`

**Function Verification**:
- ✅ Basic connection between OpenTenBase and SeaTunnel
- ✅ Simple table structure data reading and writing (bigint, text types)
- ✅ Pure synchronization without data transformation

### Advanced Data Transformation Test

**Configuration File**: `opentenbase_transform_test.conf`

**Function Verification**:
- ✅ Complex SQL queries (CASE WHEN, EXTRACT functions)
- ✅ Field aliases and computed fields
- ✅ WHERE condition filtering and ORDER BY sorting
- ✅ Transform field mapping
- ✅ Complex table structure support (multiple data types)

## Summary

Through the above steps, you have successfully completed the integration of OpenTenBase with Apache SeaTunnel and achieved cross-data-source data synchronization functionality. This solution supports various application scenarios from basic data synchronization to complex data transformation.
