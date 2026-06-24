#ifndef LOG_H
#define LOG_H

#include <string>
#include <stdio.h>
#include <mutex>

// 日志级别
#define LOG_LEVEL_DEBUG "DEBUG"
#define LOG_LEVEL_INFO  "INFO"
#define LOG_LEVEL_ERROR "ERROR"

#define LOG_DEBUG_FMT(fmt, ...) log_debug_fmt(fmt, __FILE__, __LINE__, ##__VA_ARGS__)
#define LOG_INFO_FMT(fmt, ...) log_info_fmt(fmt, __FILE__, __LINE__, ##__VA_ARGS__)
#define LOG_ERROR_FMT(fmt, ...) log_error_fmt(fmt, __FILE__, __LINE__, ##__VA_ARGS__)
#define LOG_WARN_FMT(fmt, ...) log_warn_fmt(fmt, __FILE__, __LINE__, ##__VA_ARGS__)

/**
 * 设置日志输出级别
 * @param level 日志级别（DEBUG/INFO/ERROR）
 * @return void
 */
void set_log_level(const char* level);

/**
 * 设置日志输出目标
 * @param output 输出文件指针
 * @return void
 */
void set_log_output(FILE* output);

/**
 * 设置日志时间戳
 * @param timestamp 时间戳字符串
 * @return void
 */
void set_timestamp(const std::string& timestamp);

/**
 * 获取当前日志时间戳
 * @return 当前时间戳字符串
 */
std::string get_timestamp();

/**
 * 输出DEBUG级别的日志
 * @param message 日志消息
 * @param file 源文件名
 * @param line 源代码行号
 * @return void
 */
void log_debug(const char* message, const char* file, int line);

/**
 * 输出INFO级别的日志
 * @param message 日志消息
 * @param file 源文件名
 * @param line 源代码行号
 * @return void
 */
void log_info(const char* message, const char* file, int line);

/**
 * 输出ERROR级别的日志
 * @param message 日志消息
 * @param file 源文件名
 * @param line 源代码行号
 * @return void
 */
void log_error(const char* message, const char* file, int line);

/**
 * 输出WARN级别的日志
 * @param message 日志消息
 * @param file 源文件名
 * @param line 源代码行号
 * @return void
 */
void log_warn(const char* message, const char* file, int line);

/**
 * 输出DEBUG级别的格式化日志
 * @param format 格式化字符串
 * @param file 源文件名
 * @param line 源代码行号
 * @param ... 可变参数列表
 * @return void
 */
void log_debug_fmt(const char* format, const char* file, int line, ...);

/**
 * 输出INFO级别的格式化日志
 * @param format 格式化字符串
 * @param file 源文件名
 * @param line 源代码行号
 * @param ... 可变参数列表
 * @return void
 */
void log_info_fmt(const char* format, const char* file, int line, ...);

/**
 * 输出ERROR级别的格式化日志
 * @param format 格式化字符串
 * @param file 源文件名
 * @param line 源代码行号
 * @param ... 可变参数列表
 * @return void
 */
void log_error_fmt(const char* format, const char* file, int line, ...);

/**
 * 输出WARN级别的格式化日志
 * @param format 格式化字符串
 * @param file 源文件名
 * @param line 源代码行号
 * @param ... 可变参数列表
 * @return void
 */
void log_warn_fmt(const char* format, const char* file, int line, ...);

extern std::mutex log_mutex;

#endif // LOG_H