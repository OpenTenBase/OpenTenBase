--
-- Regression test for DELETE with ON DELETE CASCADE and ON DELETE RESTRICT
--

-- =================================================================
-- SETUP: A correct implementation for a distributed database like OpenTenBase
-- =================================================================

-- Parent table: Distributed by its primary key
CREATE TABLE departments (
    id INT PRIMARY KEY,
    name TEXT NOT NULL
) DISTRIBUTE BY HASH (id);

-- Child table: 
-- 1. Primary key is composite to include the foreign key column.
-- 2. Distributed by the foreign key column to satisfy FK constraints.
CREATE TABLE employees (
    id INT,
    name TEXT NOT NULL,
    dept_id INT NOT NULL REFERENCES departments(id) ON DELETE CASCADE,
    PRIMARY KEY (id, dept_id)
) DISTRIBUTE BY HASH (dept_id);

-- Grandchild table: Follows the same pattern as the child table.
CREATE TABLE project_assignments (
    id INT,
    emp_id INT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    project_name TEXT NOT NULL,
    PRIMARY KEY (id, emp_id)
) DISTRIBUTE BY HASH (emp_id);

-- Tables for RESTRICT test, following the same logic
CREATE TABLE departments_restrict (
  id INT PRIMARY KEY,
  name TEXT NOT NULL
) DISTRIBUTE BY HASH (id);

CREATE TABLE employees_restrict (
  id INT,
  name TEXT NOT NULL,
  dept_id INT NOT NULL REFERENCES departments_restrict(id) ON DELETE RESTRICT,
  PRIMARY KEY (id, dept_id)
) DISTRIBUTE BY HASH (dept_id);


-- Populate with test data
INSERT INTO departments VALUES (1, 'Engineering'), (2, 'HR');
INSERT INTO employees VALUES (101, 'Alice', 1), (102, 'Bob', 1), (103, 'Charlie', 2);
INSERT INTO project_assignments VALUES (1001, 101, 'Project T-Rex');
INSERT INTO departments_restrict VALUES (1, 'Finance');
INSERT INTO employees_restrict VALUES (201, 'David', 1);

-- =================================================================
-- TEST 1: Verify ON DELETE CASCADE
-- =================================================================
-- Check initial state
SELECT 'Initial State' as description, count(*) as row_count FROM employees WHERE dept_id = 1;
SELECT 'Initial State' as description, count(*) as row_count FROM project_assignments WHERE emp_id = 101;

-- Perform the cascading delete
DELETE FROM departments WHERE name = 'Engineering';

-- Verify the results
SELECT 'After CASCADE DELETE' as description, count(*) as row_count FROM departments WHERE id = 1;
SELECT 'After CASCADE DELETE' as description, count(*) as row_count FROM employees WHERE dept_id = 1;
SELECT 'After CASCADE DELETE' as description, count(*) as row_count FROM project_assignments WHERE emp_id = 101;
SELECT name FROM departments ORDER BY id;

-- =================================================================
-- TEST 2: Verify ON DELETE RESTRICT
-- =================================================================
DELETE FROM departments_restrict WHERE id = 1;


-- =================================================================
-- CLEANUP
-- =================================================================
DROP TABLE project_assignments;
DROP TABLE employees;
DROP TABLE departments;
DROP TABLE employees_restrict;
DROP TABLE departments_restrict;