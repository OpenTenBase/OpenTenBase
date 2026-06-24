#include "file.h"
#include <cstdlib>
#include <limits.h>
#include <string>
#include <iostream>
#include <fstream>
#include <map>
#include <algorithm>
#include <cctype>

/**
 * @brief 将给定的相对路径或绝对路径转换为绝对路径。
 *
 * 此函数接受一个路径字符串（可以是相对路径或绝对路径），并返回其对应的绝对路径。
 * 如果输入的路径不存在或非法，函数将返回一个空字符串。根据需求，你也可以选择在此情况下抛出异常。
 *
 * @param path 输入的路径字符串，可以是相对路径（例如 "./dir/file.txt"）或绝对路径（例如 "/usr/local/bin"）。
 * @return std::string 返回转换后的绝对路径字符串。如果路径不存在或非法，返回空字符串 ""。
 *
 * @note
 * - 此函数依赖于 POSIX 标准库函数 `realpath`，因此仅在类 Unix 系统（如 Linux 和 macOS）上可用。
 * - 如果需要跨平台支持，建议使用 C++17 引入的 `<filesystem>` 库中的 `std::filesystem::absolute` 或 `std::filesystem::canonical` 函数。
 */
std::string to_absolute_path(const std::string& path) {
    char* resolved = realpath(path.c_str(), nullptr);
    if (resolved == nullptr) {
        // 路径不存在或非法，根据需求返回空或抛异常
        return ""; 
    }
    std::string abs_path(resolved);
    free(resolved); // 释放由 realpath 分配的内存
    return abs_path;
}

/**
 * @brief 尝试删除指定的文件，如果删除成功则返回 true，失败则忽略错误并返回 false。
 * 
 * @param filePath 要删除的文件路径
 * @return true 删除成功
 * @return false 删除失败或文件不存在
 */
bool try_delete_file(const std::string& filePath) {

    // 检查文件是否存在
    std::ifstream infile(filePath);
    if (!infile.good()) {
        // 文件不存在，返回 false
        return false;
    }
    infile.close();

    // 尝试删除文件
    if (std::remove(filePath.c_str()) != 0) {
        // 删除失败，返回 false
        return false;
    }

    // 删除成功，返回 true
    return true;
}

// 去除字符串左侧的空白字符
std::string ltrim(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
    }
    return s.substr(start);
}

// 去除字符串右侧的空白字符
std::string rtrim(const std::string& s) {
    size_t end = s.size();
    while (end > 0 && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(0, end);
}

// 去除字符串两侧的空白字符
std::string trim(const std::string& s) {
    return rtrim(ltrim(s));
}

// 解析配置文件并将键值对存入map
bool parseConfigFile(const std::string& filename, std::map<std::string, std::string>& configMap) {
    std::ifstream infile(filename);
    if (!infile.is_open()) {
        std::cerr << "can not open file: " << filename << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(infile, line)) {
        // 去除行首和行尾的空白字符
        std::string trimmedLine = trim(line);

        // 跳过空行
        if (trimmedLine.empty()) {
            continue;
        }

        // 查找等号的位置
        size_t equalsPos = trimmedLine.find('=');
        if (equalsPos == std::string::npos) {
            // 如果没有等号，跳过该行
            continue;
        }

        // 提取键和值
        std::string key = trim(trimmedLine.substr(0, equalsPos));
        std::string value = trim(trimmedLine.substr(equalsPos + 1));

        // 查找注释的位置（#）
        size_t commentPos = value.find('#');
        if (commentPos != std::string::npos) {
            // 如果有注释，截取注释前的部分
            value = trim(value.substr(0, commentPos));
        }

        // 插入到map中，如果键已存在，后面的会覆盖前面的
        configMap[key] = value;
    }

    infile.close();
    return true;
}
