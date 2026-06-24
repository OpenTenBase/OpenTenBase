#ifndef REMOTE_SSH_H
#define REMOTE_SSH_H

#include <string>
#include <cstdio>
#include <cstdlib>

/**
 * 在远程服务器上执行SSH命令
 * @param ip 远程服务器IP地址
 * @param port SSH端口号
 * @param username SSH用户名
 * @param password SSH密码
 * @param command 要执行的命令
 * @param result 命令执行结果
 * @return 0: 执行成功; 非0: 执行失败
 */
int remote_ssh_exec(const std::string& ip, int port, const std::string& username, 
                   const std::string& password, const std::string& command,
                   std::string& result);

/**
 * 使用SCP传输文件到远程服务器
 * @param ip 远程服务器IP地址
 * @param port SSH端口号
 * @param username SSH用户名
 * @param password SSH密码
 * @param local_path 本地文件路径
 * @param remote_path 远程文件路径
 * @return 0: 传输成功; 非0: 传输失败
 */
int remote_scp_file(const std::string& ip, int port, const std::string& username,
                   const std::string& password, const std::string& local_path,
                   const std::string& remote_path);

/**
 * 使用SCP/CP传输文件到远程服务器或本地其他目录
 * @param ip 远程服务器IP地址
 * @param port SSH端口号
 * @param username SSH用户名
 * @param password SSH密码
 * @param local_path 本地文件路径
 * @param remote_path 远程文件路径
 * @return 0: 传输成功; 非0: 传输失败
 */
int excute_cp_file(const std::string& ip, int port, const std::string& username,
const std::string& password, const std::string& local_path,
const std::string& remote_path);
          
/**
 * 获取服务器的用户主目录
 * @param ip 远程服务器IP地址
 * @param port SSH端口号
 * @param username SSH用户名
 * @param password SSH密码
 * @param home_dir 获取到的主目录路径
 * @return 0: 获取成功; 非0: 获取失败
 */
int get_home_dir(const std::string& ip, int port, const std::string& username,
                      const std::string& password, std::string& home_dir);
                      
// 判断IP是否为本地IP
/**
 * 判断IP是否为本地IP
 * @param ip 服务器IP地址
 * @return true: 本地; false: 远端
 */
bool is_local_ip(const std::string& ip);

/**
 * 在本地执行命令并获取输出
 * @param command 要执行的命令
 * @param result 命令执行结果
 * @return 0: 执行成功; 非0: 执行失败
 */
int local_exec(const std::string& command, std::string& result);

/**
 * 在服务器上执行SSH命令(自动识别IP是否是本地还是远程)
 * @param ip 远程服务器IP地址
 * @param port SSH端口号
 * @param username SSH用户名
 * @param password SSH密码
 * @param command 要执行的命令
 * @param result 命令执行结果
 * @return 0: 执行成功; 非0: 执行失败
 */
int execute_command(const std::string& ip, int port, const std::string& username, 
                const std::string& password, const std::string& command, std::string& result);

/**
 * Execute sql by psql (remote or local)
 * @param ip 远程服务器IP地址
 * @param port SSH端口号
 * @param username SSH用户名
 * @param password SSH密码
 * @param command 要执行的命令
 * @param result 命令执行结果
 * @return 0: 执行成功; 非0: 执行失败
 */
int execute_sql_by_psql(const std::string& ip, int port, const std::string& username, 
                const std::string& password, const std::string& command, std::string& result);

/**
 * 在本地，指定某个用户下执行命令并获取输出
 * @param command 要执行的命令
 * @param result 命令执行结果
 * @return 0: 执行成功; 非0: 执行失败
 */
int local_exec_as_user(const std::string& user, const std::string& command, std::string& result);

/**
 * 将二进制字节数据编码为 Base64 格式字符串
 * 该函数接收一段二进制数据（以 unsigned char 数组形式表示），将其按照 Base64
 * 编码规则转换为可打印的 ASCII 字符串。常用于将二进制数据（如加密结果、文件内容、
 * 网络数据包等）编码为文本形式，以便存储、传输或嵌入到文本协议中。
 * @param bytes_to_encode 指向待编码的二进制数据的指针（字节数组）
 * @param in_len          待编码数据的字节长度（即数组的有效长度）
 * @return std::string      返回编码后的 Base64 字符串（通常不包含换行符）
 * @note
 * - Base64 编码是一种将 3 字节二进制数据转换为 4 个 ASCII 字符的编码方式，
 *   常用于处理二进制数据的文本化表示。
 * - 本函数输出的 Base64 字符串通常为连续格式，不包含换行符，适合直接用于
 *   HTTP、Shell 命令（如 base64 --decode）、JSON、配置等场景。
 * - 输入数据为空（in_len == 0）时，可能返回空字符串，具体取决于实现。
 * - 该函数通常与 base64_decode() 配对使用，实现编解码能力。
 */
std::string base64_encode(const unsigned char* bytes_to_encode, unsigned int in_len);

/**
 * 将输入的字符串进行 Base64 编码
 * 该函数接收一个原始字符串，使用 Base64 编码算法将其转换为可安全传输或存储的
 * Base64 字符串。常用于编码二进制数据或特殊字符，以便嵌入文本协议、命令行、URL等场景。
 * @param input 需要进行 Base64 编码的原始字符串
 * @return std::string 返回编码后的 Base64 字符串（不含换行符）
 * @note
 * - 编码结果通常可用于 HTTP Basic Auth、命令行参数传递、数据存储等场景。
 * - 本实现通常依赖 OpenSSL 的 Base64 编码接口（如 BIO_f_base64），确保高效和标准兼容。
 * - 返回的 Base64 字符串为连续格式，不包含换行符，适合直接用于管道或嵌入文本。
 */
std::string string_to_base64(const std::string& input);

/**
 * 检查端口是否可用
 * @param ip 目标主机IP地址
 * @param port 要检查的端口号
 * @param username 用户名
 * @param password 密码
 * @param ssh_port SSH端口号
 * @return 0: 端口可用; -1: 端口不可用或发生错误
 */
int check_port_available(const char *ip, int port, const char *username, const char *password, int ssh_port);

#endif // REMOTE_SSH_H