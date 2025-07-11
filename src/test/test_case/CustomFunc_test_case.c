#include <stdio.h>
#include <string.h>
#include <assert.h>

// 自定义字符串反转函数
// 返回值：0 正常，-1 参数非法，-2 缓冲区不足
int reverse_string(const char* input, char* output, int out_size) {
    if (!input || !output || out_size <= 0) return -1;
    int len = strlen(input);
    if (len >= out_size) return -2;
    for (int i = 0; i < len; ++i) {
        output[i] = input[len - 1 - i];
    }
    output[len] = '\0';
    return 0;
}

// 正常用例
void test_reverse_normal() {
    char input[] = "OpenTenBase";
    char output[64];
    int status = reverse_string(input, output, sizeof(output));
    printf("Input: %s\nOutput: %s\n", input, output);
    assert(status == 0);
    assert(strcmp(output, "esaBneTnepO") == 0);
    printf("test_reverse_normal passed\n");
}

// 空指针输入
void test_reverse_null() {
    char output[32];
    int status = reverse_string(NULL, output, sizeof(output));
    assert(status == -1);
    printf("test_reverse_null passed\n");
}

// 输出缓冲区不足
void test_reverse_overflow() {
    char input[] = "1234567890";
    char output[5];
    int status = reverse_string(input, output, sizeof(output));
    assert(status == -2);
    printf("test_reverse_overflow passed\n");
}

// 输入空字符串
void test_reverse_empty() {
    char input[] = "";
    char output[8];
    int status = reverse_string(input, output, sizeof(output));
    assert(status == 0);
    assert(strcmp(output, "") == 0);
    printf("test_reverse_empty passed\n");
}

int main() {
    test_reverse_normal();
    test_reverse_null();
    test_reverse_overflow();
    test_reverse_empty();
    printf("All reverse_string tests are complete.\n");
    return 0;
} 