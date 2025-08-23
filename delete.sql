-- 测试表结构定义
CREATE TABLE departments (
    dept_id SERIAL PRIMARY KEY,
    dept_name TEXT NOT NULL
);

CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    dept_id INTEGER REFERENCES departments(dept_id) ON DELETE CASCADE,
    emp_name TEXT NOT NULL,
    salary NUMERIC(10,2)
);

CREATE TABLE projects (
    project_id SERIAL PRIMARY KEY,
    lead_emp_id INTEGER REFERENCES employees(emp_id) ON DELETE SET NULL,
    project_name TEXT NOT NULL
);

-- 测试数据准备
INSERT INTO departments (dept_name) VALUES 
    ('研发部'), ('市场部'), ('财务部');

INSERT INTO employees (dept_id, emp_name, salary) VALUES
    (1, '张三', 15000),
    (1, '李四', 12000),
    (2, '王五', 10000);

INSERT INTO projects (lead_emp_id, project_name) VALUES
    (1, 'PostgreSQL 16优化'),
    (2, 'AI数据库研发'),
    (3, '市场分析系统');

-- 测试1: 基础DELETE操作
DELETE FROM employees WHERE emp_id = 3;
SELECT * FROM employees ORDER BY emp_id;
SELECT * FROM projects ORDER BY project_id;

-- 测试2: 级联删除测试
DELETE FROM departments WHERE dept_id = 1;
SELECT * FROM departments ORDER BY dept_id;
SELECT * FROM employees ORDER BY emp_id;
SELECT * FROM projects ORDER BY project_id;
