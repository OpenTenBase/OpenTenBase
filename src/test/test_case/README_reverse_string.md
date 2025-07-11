# reverse_string 自定义函数测试

本测试用例用于验证自定义字符串反转函数 `reverse_string()` 的逻辑正确性与健壮性，覆盖变量声明、数据处理和异常处理三个方面。

## 函数说明

```c
int reverse_string(const char *input, char *output, int out_size);

输入参数
input: 原始字符串（如 "OpenTenBase"）
output: 用于接收反转后的字符串
out_size: output 缓冲区大小，防止溢出

返回值
0: 正常处理成功
-1: 参数非法
-2: 缓冲区不足
```

该函数支持基本的字符串反转操作，并对异常参数和缓冲区溢出进行检测。

## 测试覆盖内容
| 测试名称             | 场景示例           | 覆盖点           |
|----------------------|--------------------|------------------|
| test_reverse_normal  | 输入: "OpenTenBase"| 正常数据处理     |
| test_reverse_null    | 输入: NULL         | 异常参数处理     |
| test_reverse_overflow| 输入: "1234567890", output 长度不足 | 缓冲区溢出检测 |
| test_reverse_empty   | 输入: ""           | 空字符串处理     |

所有测试均使用 assert 和 strcmp 进行验证，输出测试结果到终端。

## 编译 & 运行方式

```bash
gcc CustomFunc_test_case.c -o run_reverse_test
./run_reverse_test
```

或：

```bash
gcc -Wall -Wextra -O2 CustomFunc_test_case.c -o reverse_test && ./reverse_test
```

## 说明
本测试文件用于演示自定义函数的测试用例设计，包括变量声明、数据操作、异常处理等。 