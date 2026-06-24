/**
 * @file file.h
 * @brief 文件或文件路径相关的操作
 */

#ifndef FILE_H
#define FILE_H

#include <string>
#include <map>

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
std::string to_absolute_path(const std::string& path);

/**
 * @brief 尝试删除指定的文件，如果删除成功则返回 true，失败则忽略错误并返回 false。
 * 
 * @param filePath 要删除的文件路径
 * @return true 删除成功
 * @return false 删除失败或文件不存在
 */
bool try_delete_file(const std::string& filePath);

// 去除字符串左侧的空白字符
std::string ltrim(const std::string& s);

// 去除字符串右侧的空白字符
std::string rtrim(const std::string& s);

// 去除字符串两侧的空白字符
std::string trim(const std::string& s);

// 解析配置文件并将键值对存入map
bool parseConfigFile(const std::string& filename, std::map<std::string, std::string>& configMap);

#endif // FILE_H