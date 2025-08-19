# normalize_path 自定义函数测试

本测试用例用于验证自定义路径标准化函数 `normalize_path()` 的逻辑正确性与健壮性，覆盖变量声明、数据处理和异常处理三个方面。

## 函数说明

```c
int normalize_path(const char *input, char *output, int out_size);

输入参数

input: 原始路径字符串（例如 /home//user/./docs/）

output: 用于接收处理后的标准路径

out_size: output 缓冲区大小，防止溢出

返回值

0: 正常处理成功

-1: 参数非法

-2/-3: 缓冲区不足或写入失败

该函数支持基本的路径清洗操作，包括：

合并多重斜杠

忽略 /./ 无效路径段

保留合法结构（如 /a/b/c/）

测试覆盖内容
测试名称	场景示例	覆盖点
test_normal_case	输入：/home//user/./docs/	数据处理逻辑
test_empty_input	输入：NULL	异常参数处理
test_overflow_case	输入：超长路径	缓冲区溢出检测
所有测试均使用 assert 和 strcmp 进行验证，输出测试结果到终端。

编译 & 运行方式
bash
gcc custom_function_test.c -o run_test
./run_test
或者：

bash
gcc -Wall -Wextra -O2 custom_function_test.c -o normalize_test && ./normalize_test

提交说明
本测试文件用于解决 Issue #14：

设计一个自定义函数的测试用例，包括变量声明、数据操作、异常处理等。