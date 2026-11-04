# opentenbase_mysql_compat（MySQL 兼容函数扩展）

`opentenbase_mysql_compat` 是一个 OpenTenBase 扩展，以 MySQL 语义实现最常用的
MySQL 内置函数，让为 MySQL 编写的 SQL 可以在 OpenTenBase 上以极小的改写成本运行。
它是独立的 `contrib` 插件，不修改内核。

本扩展实现 OpenTenBase 综合优化路线图（[issue #230](https://github.com/OpenTenBase/OpenTenBase/issues/230)）
中的模块 4（MySQL 兼容函数扩展）。

## 提供的函数

### 字符串函数

| 函数 | 签名 | 说明 |
|---|---|---|
| `concat` | `concat(VARIADIC "any") -> text` | 任一参数为 NULL 则整体返回 NULL（与 PostgreSQL 内置 `concat` 不同）；非文本参数隐式转字符串 |
| `concat_ws` | `concat_ws(text, VARIADIC "any") -> text` | 分隔符为 NULL 返回 NULL；NULL 参数被跳过 |
| `find_in_set` | `find_in_set(text, text) -> int` | 返回在逗号分隔列表中的位置（从 1 开始），找不到为 0 |
| `elt` | `elt(int, VARIADIC text) -> text` | 返回列表第 N 个元素 |
| `field` | `field(text, VARIADIC text) -> int` | 返回值在列表中的序号（从 1 开始），找不到为 0 |
| `insert` | `"insert"(text, int, int, text) -> text` | 替换子串；`INSERT` 是 SQL 保留字，调用时需加引号或使用别名 `mysql_insert()` |
| `substring_index` | `substring_index(text, text, int) -> text` | 按分隔符第 N 次出现截取前/后部分 |
| `locate` | `locate(text, text [, int]) -> int` | 不区分大小写（ASCII）查找位置（从 1 开始），找不到为 0 |
| `instr` | `instr(text, text) -> int` | 参数顺序相反的 `locate(substr, str)` |
| `reverse` | `reverse(text) -> text` | 反转字符串，支持多字节字符 |
| `space` | `space(int) -> text` | 生成 N 个空格 |
| `unhex` | `unhex(text) -> bytea` | 十六进制串转字节串；长度为奇数或含非法字符时返回 NULL |

### 日期时间函数

| 函数 | 签名 | 说明 |
|---|---|---|
| `date_format` | `date_format(timestamp \| date, text) -> text` | MySQL `%` 格式说明符（英文月/星期名） |
| `str_to_date` | `str_to_date(text, text) -> timestamp` | `date_format` 的反函数（常用说明符） |
| `datediff` | `datediff(timestamp \| date, timestamp \| date) -> int` | 日期部分的天数差（`a - b`） |
| `timestampdiff` | `timestampdiff(text, timestamp, timestamp) -> bigint` | 单位：MICROSECOND、SECOND、MINUTE、HOUR、DAY、WEEK、MONTH、QUARTER、YEAR |
| `timestampadd` | `timestampadd(text, bigint, timestamp) -> timestamp` | 同上单位；月份运算像 MySQL 一样截断（1 月 31 日 + 1 月 = 2 月 28 日） |
| `last_day` | `last_day(timestamp \| date) -> date` | 当月最后一天 |
| `quarter` | `quarter(timestamp \| date) -> int` | 季度（1-4） |
| `dayofweek` | `dayofweek(timestamp) -> int` | 1 = 周日 … 7 = 周六 |
| `weekday` | `weekday(timestamp) -> int` | 0 = 周一 … 6 = 周日 |
| `dayofmonth`、`dayofyear`、`weekofyear` | `(timestamp) -> int` | `weekofyear` 为 ISO-8601 周号，与 MySQL 一致 |

### 数学函数

| 函数 | 签名 | 说明 |
|---|---|---|
| `format` | `format(numeric \| double precision \| int, int) -> text` | 四舍五入并加千分位逗号 |
| `truncate` | `"truncate"(numeric \| double precision \| int, int) -> numeric` | MySQL 截断语义；`TRUNCATE` 是保留字，需加引号或用 `mysql_truncate()` / 内置 `trunc()` |
| `bin`、`oct` | `(bigint) -> text` | 进制转换；负数按 64 位补码渲染 |

### 流程控制

| 函数 | 签名 | 说明 |
|---|---|---|
| `if` | `if(boolean, anyelement, anyelement) -> anyelement` | 两个分支需为相同类型 |
| `ifnull` | `ifnull(anyelement, anyelement) -> anyelement` | 双参数 `coalesce` |
| `isnull` | `isnull("any") -> int` | 参数为 NULL 返回 1，否则 0 |

### 聚合函数

| 函数 | 签名 | 说明 |
|---|---|---|
| `group_concat` | `group_concat(anyelement [, text]) -> text` | 用 `,` 或指定分隔符连接非 NULL 值；排序使用聚合 `ORDER BY` 语法 |

## 安装

在 OpenTenBase 源码树中编译安装：

```sh
./configure ...            # 按 OpenTenBase 文档执行
make -C contrib/opentenbase_mysql_compat
make -C contrib/opentenbase_mysql_compat install
```

然后在目标数据库执行：

```sql
CREATE EXTENSION opentenbase_mysql_compat;
```

扩展可重定位（relocatable）。若希望与 PostgreSQL 内置同名的函数
（`concat`、`concat_ws`）使用 MySQL 语义的版本，可将扩展安装到独立
schema，并把该 schema 放在 `search_path` 中 `pg_catalog` 之前：

```sql
CREATE SCHEMA mysql_compat;
CREATE EXTENSION opentenbase_mysql_compat SCHEMA mysql_compat;
SET search_path = mysql_compat, pg_catalog, public;
```

## 迁移教程

以下是一组典型的 MySQL 查询：

```sql
-- MySQL
SELECT CONCAT(first_name, ' ', last_name)          AS full_name,
       IFNULL(nickname, first_name)                AS display_name,
       FORMAT(salary, 2)                           AS salary_fmt,
       IF(salary > 5000, 'high', 'low')            AS band,
       DATE_FORMAT(hired_at, '%Y-%m-%d')           AS hired,
       TIMESTAMPDIFF(YEAR, hired_at, NOW())        AS tenure_years
FROM employees;

SELECT dept, GROUP_CONCAT(name ORDER BY name SEPARATOR '; ')
FROM employees GROUP BY dept;

SELECT DATEDIFF('2020-01-02', '2020-01-01');
SELECT STR_TO_DATE('01,5,2013', '%d,%m,%Y');
```

在 OpenTenBase 上等价写法（扩展已加入 `search_path`）：

```sql
-- OpenTenBase
SELECT concat(first_name, ' ', last_name)          AS full_name,
       ifnull(nickname, first_name)                AS display_name,
       format(salary, 2)                           AS salary_fmt,
       if(salary > 5000, 'high', 'low')            AS band,
       date_format(hired_at, '%Y-%m-%d')           AS hired,
       timestampdiff('YEAR', hired_at, now()::timestamp) AS tenure_years
FROM employees;

SELECT dept, group_concat(name, '; ' ORDER BY name)
FROM employees GROUP BY dept;

SELECT datediff('2020-01-02'::timestamp, '2020-01-01'::timestamp);
SELECT str_to_date('01,5,2013', '%d,%m,%Y');
```

机械改写规则：

1. **函数名**一一对应（小写），扩展加入 `search_path` 后直接可用。
2. `GROUP_CONCAT(x SEPARATOR s)` 改写为 `group_concat(x, s)`。
3. `GROUP_CONCAT(x ORDER BY y)` 保持聚合 `ORDER BY` 语法不变。
4. 时间单位作为字符串传入：`TIMESTAMPDIFF(YEAR, a, b)` →
   `timestampdiff('YEAR', a, b)`。
5. 与日期比较的字符串字面量建议显式转换（`'2020-01-02'::timestamp`）：
   MySQL 隐式解析日期字符串，PostgreSQL 系内核要求显式类型。
6. `INSERT()` 和 `TRUNCATE()` 与 SQL 保留字冲突：写成 `"insert"(...)`
   / `"truncate"(...)`，或使用不带引号的别名 `mysql_insert()` /
   `mysql_truncate()`。

## 与 MySQL 的已知差异

* 不区分大小写的匹配（`locate`、`instr`、`field`、`find_in_set`）仅覆盖
  ASCII 范围；非 ASCII 字符按字节精确比较，需要时请在查询中调整排序规则。
* `str_to_date` 恒返回 `timestamp`。MySQL 有时返回 `date`，需要纯日期时请
  对结果 `::date`。格式中缺失的日期字段默认为 `0001-01-01`，因为
  PostgreSQL 系内核没有"零日期"。
* `concat` 对 `bytea` 参数按 PostgreSQL bytea 输出（十六进制）渲染。
* MySQL 的 `GROUP_CONCAT` 专属后缀语法改为聚合标准语法
  （`group_concat(x ORDER BY x)`）。
* 周号格式说明符（`%U`、`%u`、`%V`、`%v`、`%X`、`%x`）遵循 MySQL 定义；
  `str_to_date` 不支持解析这些说明符（MySQL 自身同样限制）。
* `str_to_date` 与 MySQL 一致，忽略格式匹配结束后剩余的输入字符。
* `if()`/`ifnull()` 使用多态参数：当两个分支都是无类型字面量
  （如 `ifnull(NULL, NULL)`）时，请至少给一个加显式转换，否则内核无法
  解析结果类型。

## 测试

在运行中的 OpenTenBase 实例上：

```sh
make -C contrib/opentenbase_mysql_compat installcheck
```

## 许可证

与 OpenTenBase 一致（BSD 3-Clause），见仓库 `LICENSE.txt`。
