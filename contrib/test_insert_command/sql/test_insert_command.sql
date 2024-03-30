-- 测试用例 1：使用 VALUES 插入单条记录
INSERT INTO students (name, student_id, gender, age)
VALUES 
    ('Alice', 'S001', 'Female', 20);

-- 预期结果：成功插入一条数据记录

-- 测试用例 2：使用 VALUES 插入多条记录
INSERT INTO students (name, student_id, gender, age)
VALUES 
    ('Bob', 'S002', 'Male', 22),
    ('Charlie', 'S003', 'Male', 21),
    ('David', 'S004', 'Male', 23);

-- 预期结果：成功插入三条数据记录

-- 测试用例 3：使用子查询插入数据
INSERT INTO students (name, student_id, gender, age)
SELECT 
    name, 
    student_id, 
    gender, 
    age
FROM 
    temp_students
WHERE 
    age > 20;

-- 预期结果：根据子查询结果成功插入相应的数据记录

-- 测试用例 4：使用 VALUES 插入数据并返回插入的数据
INSERT INTO students (name, student_id, gender, age)
VALUES 
    ('Eve', 'S005', 'Female', 21)
RETURNING *;

-- 预期结果：成功插入一条数据记录，并返回插入的数据记录

-- 测试用例 5：使用子查询插入数据并返回插入的数据
INSERT INTO students (name, student_id, gender, age)
SELECT 
    name, 
    student_id, 
    gender, 
    age
FROM 
    temp_students
WHERE 
    age > 20
RETURNING *;

-- 预期结果：根据子查询结果成功插入相应的数据记录，并返回插入的数据记录
