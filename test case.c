#include <stdio.h>
#include <stdlib.h>
#include <math.h>

// 自定义函数：计算数组元素的平均值
double calculate_average(int arr[], int size) {
    // 异常处理：检查数组是否为空
    if (size <= 0) {
        fprintf(stderr, "警告：数组大小必须大于0，返回NaN\n");
        return NAN; // 返回Not a Number表示错误
    }

    int sum = 0;
    // 数据操作：计算数组元素的总和
    for (int i = 0; i < size; i++) {
        sum += arr[i];
    }

    // 数据操作：计算平均值
    return (double)sum / size;
}

int main() {
    // 变量声明
    int valid_array[] = {10, 20, 30, 40, 50};
    int valid_size = sizeof(valid_array) / sizeof(valid_array[0]);

    double average;

    // 测试有效数组
    printf("测试有效数组:\n");
    average = calculate_average(valid_array, valid_size);
    printf("平均值: %.2f\n\n", average);

    // 移除对空数组的测试，避免程序退出

    return 0;
}    