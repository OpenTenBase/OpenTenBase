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
 * 获取远程服务器的用户主目录
 * @param ip 远程服务器IP地址
 * @param port SSH端口号
 * @param username SSH用户名
 * @param password SSH密码
 * @param home_dir 获取到的主目录路径
 * @return 0: 获取成功; 非0: 获取失败
 */
int get_remote_home_dir(const std::string& ip, int port, const std::string& username,
                      const std::string& password, std::string& home_dir);
                      
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
 * 在本地，指定某个用户下执行命令并获取输出
 * @param command 要执行的命令
 * @param result 命令执行结果
 * @return 0: 执行成功; 非0: 执行失败
 */
int local_exec_as_user(const std::string& user, const std::string& command, std::string& result);

#endif // REMOTE_SSH_H