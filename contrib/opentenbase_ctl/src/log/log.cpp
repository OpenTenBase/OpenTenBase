#include <iostream>
#include <fstream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <cstring>
#include <sys/stat.h>
#include <cstdarg>
#include <cerrno>
#include <ctime>
#include <locale>
#include "log.h"
#include <mutex>

// Global log level
static const char* current_log_level = LOG_LEVEL_INFO;
static std::string current_timestamp;

// Log directory path
static const std::string log_dir = "./logs";

std::mutex log_mutex;

// Set log level
void 
set_log_level(const char* level) {
    current_log_level = level;
}

void 
set_timestamp(const std::string& timestamp) {
    current_timestamp = timestamp;
}

std::string 
get_timestamp() {
    return current_timestamp;
}

// Check if should record this level log
static bool 
should_log(const char* level) {
    if (strcmp(level, LOG_LEVEL_DEBUG) == 0 && strcmp(current_log_level, LOG_LEVEL_DEBUG) != 0) {
        return false;
    }
    return true;
}

// 检查并创建日志目录
bool ensure_log_directory_exists() {
    struct stat info;
    if (stat(log_dir.c_str(), &info) != 0) {
        // 目录不存在，尝试创建
        if (mkdir(log_dir.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) != 0) {
            // 创建失败，记录错误日志
            std::lock_guard<std::mutex> lock(log_mutex);
            std::cerr << "Failed to create log directory: " << log_dir << " (errno: " << errno << ")" << std::endl;
            return false;
        }
    } else if (!(info.st_mode & S_IFDIR)) {
        // 路径存在但不是目录
        std::lock_guard<std::mutex> lock(log_mutex);
        std::cerr << "Log path exists but is not a directory: " << log_dir << std::endl;
        return false;
    }
    return true;
}

void 
log_message(const char* level, const char* message, const char* file, int line) {
    std::lock_guard<std::mutex> lock(log_mutex);
    if (!should_log(level)) {
        return;
    }
    
    // 确保日志目录存在
    if (!ensure_log_directory_exists()) {
        return;
    }

    // Get current time
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    struct tm* timeinfo = std::localtime(&in_time_t);
    
    // Format timestamp
    char timestamp[32];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", timeinfo);
    
    // Format date (for filename)
    char date_str[32];
    strftime(date_str, sizeof(date_str), "%Y-%m-%d-%H-%M", timeinfo);
    std::string log_filename = log_dir + "/opentenbase_ctl_" + date_str + ".log";
    
    // Open log file
    std::ofstream log_file;
    log_file.open(log_filename, std::ios_base::app);
    log_file << "[" << get_timestamp() << "] [" << timestamp << "] [" << level << "] [" << file << ":" << line << "] " << message << std::endl;
    log_file.close();

    // Only error logs output to terminal, and only output error content
    if (strcmp(level, LOG_LEVEL_ERROR) == 0) {
        fprintf(stderr, "[%s:%d] %s\n", file, line, message);
    }
}

// Basic log function implementation
void 
log_debug(const char* message, const char* file, int line) {
    log_message(LOG_LEVEL_DEBUG, message, file, line);
}

void 
log_info(const char* message, const char* file, int line) {
    log_message(LOG_LEVEL_INFO, message, file, line);
}

void 
log_error(const char* message, const char* file, int line) {
    log_message(LOG_LEVEL_ERROR, message, file, line);
}

void 
log_warn(const char* message, const char* file, int line) {
    log_message(LOG_LEVEL_ERROR, message, file, line);
}

// Format log function implementation
void 
log_debug_fmt(const char* format, const char* file, int line, ...) {
    if (strcmp(current_log_level, LOG_LEVEL_DEBUG) != 0) {
        return;
    }
    char buffer[4096];
    va_list args;
    va_start(args, line);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    log_message(LOG_LEVEL_DEBUG, buffer, file, line);
}

void 
log_info_fmt(const char* format, const char* file, int line, ...) {
    char buffer[4096];
    va_list args;
    va_start(args, line);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    log_message(LOG_LEVEL_INFO, buffer, file, line);
}

void 
log_error_fmt(const char* format, const char* file, int line, ...) {
    char buffer[4096];
    va_list args;
    va_start(args, line);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    log_message(LOG_LEVEL_ERROR, buffer, file, line);
}

void 
log_warn_fmt(const char* format, const char* file, int line, ...) {
    char buffer[4096];
    va_list args;
    va_start(args, line);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    log_message(LOG_LEVEL_ERROR, buffer, file, line);
}
