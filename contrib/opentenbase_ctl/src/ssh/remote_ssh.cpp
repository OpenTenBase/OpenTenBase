#include "remote_ssh.h"
#include <string>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <memory>
#include <array>

#include <iostream>
#include <stdexcept>

#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>

#include "../log/log.h"

// Execute remote command
int 
remote_ssh_exec(const std::string& ip, int port, const std::string& username, 
                   const std::string& password, const std::string& command,
                   std::string& result) {
                        // 将命令进行 Base64 编码
    std::string encoded_command = string_to_base64(command);
    std::string cmd = "sshpass -p '" + password + "' ssh -o StrictHostKeyChecking=no -p " + 
                    std::to_string(port) + " " + username + "@" + ip + 
                    " 'echo " + encoded_command + " | base64 --decode | bash' 2>&1";

    LOG_INFO_FMT("Executing remote command on %s: %s", ip.c_str(), command.c_str());
    
    std::array<char, 4096> buffer;
    std::stringstream ss;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    
    if (!pipe) {
        LOG_ERROR_FMT("Failed to execute command on %s", ip.c_str());
        return -1;
    }
    
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        ss << buffer.data();
    }
    
    result = ss.str();
    if (!result.empty()) {
        LOG_INFO_FMT("Command output from %s: %s", ip.c_str(), result.c_str());
    }
    return 0;
}

// Transfer file to remote host
int 
remote_scp_file(const std::string& ip, int port, const std::string& username,
                   const std::string& password, const std::string& local_path,
                   const std::string& remote_path) {
    std::string cmd = "sshpass -p '" + password + "' scp -o StrictHostKeyChecking=no -P " + 
                     std::to_string(port) + " " + local_path + " " + 
                     username + "@" + ip + ":" + remote_path + " 2>&1";
    
    LOG_INFO_FMT("Transferring file to %s: %s -> %s", ip.c_str(), local_path.c_str(), remote_path.c_str());
    
    try {
        std::array<char, 4096> buffer;
        std::stringstream ss;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
        
        if (!pipe) {
            LOG_ERROR_FMT("Failed to start SCP transfer to %s", ip.c_str());
            return -1;
        }
        
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            ss << buffer.data();
        }
        
        std::string output = ss.str();
        if (!output.empty()) {
            LOG_INFO_FMT("SCP transfer output: %s", output.c_str());
        }
        
        int ret = pclose(pipe.release());
        if (ret != 0) {
            LOG_ERROR_FMT("SCP transfer failed with exit code %d", ret);
            return -1;
        }
    }
    catch (const std::system_error& e) {
        std::cerr << "捕获到 system_error: " << e.what() << '\n';
        std::cerr << "错误码: " << e.code() << '\n';
        std::cerr << "错误码值: " << e.code().value() << '\n';
        std::cerr << "错误类别: " << e.code().category().name() << '\n';
    }

    LOG_INFO_FMT("File transfer completed successfully to %s", ip.c_str());
    return 0;
}

// Transfer file to remote host
int 
excute_cp_file(const std::string& ip, int port, const std::string& username,
                   const std::string& password, const std::string& local_path,
                   const std::string& remote_path) {

    // 如果明确指定是本地执行，或者IP是本地IP，则本地执行
    bool should_execute_local = is_local_ip(ip);
    
    if (should_execute_local) {

        // Local execution
        if (local_path == remote_path)
        {
            return 0;
        }
        
        std::string command = " cp " + local_path + " " + remote_path;
        std::string result = "";
        LOG_INFO_FMT("Executing local command: %s", command.c_str());
        int ret = local_exec_as_user(username, command, result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to execute command locally: %s", command.c_str());
            return -1;
        }

    } else {

        // Remote execution
        int ret = remote_scp_file(ip, port, username, password, local_path, remote_path);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to transfer package %s to %s (%s)", local_path, ip, remote_path);
            return -1;
        }
    }

    LOG_INFO_FMT("File transfer completed successfully to %s", ip.c_str());
    return 0;
}


// Get home directory
int 
get_home_dir(const std::string& ip, int port, const std::string& username,
                       const std::string& password, std::string& home_dir) {

    // 如果明确指定是本地执行，或者IP是本地IP，则本地执行
    bool should_execute_local = is_local_ip(ip);
    std::string result;
    
    if (should_execute_local) {

        // Local execution
        std::string command = "echo \\$HOME";
        int ret = local_exec_as_user(username, command, result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to execute command locally: %s", command.c_str());
            return -1;
        }

    } else {

        // Remote execution
        std::string command = "echo $HOME";
        if (remote_ssh_exec(ip, port, username, password, command, result) != 0) {
            LOG_ERROR_FMT("Failed to execute command Remote : %s", command.c_str());
            return -1;
        }
    }

    // 移除结果中的换行符
    if (!result.empty() && result[result.length()-1] == '\n') {
        result.erase(result.length()-1);
    }
    
    home_dir = result;
    LOG_INFO_FMT("Got home directory for %s@%s: %s", username.c_str(), ip.c_str(), home_dir.c_str());
    return 0;
}

// Execute local command
int 
local_exec(const std::string& command, std::string& result) {
    LOG_INFO_FMT("Executing local command: %s", command.c_str());
    
    std::array<char, 4096> buffer;
    std::stringstream ss;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(command.c_str(), "r"), pclose);
    
    if (!pipe) {
        LOG_ERROR_FMT("Failed to execute local command");
        return -1;
    }
    
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        ss << buffer.data();
    }
    
    result = ss.str();
    if (!result.empty()) {
        LOG_INFO_FMT("Command output: %s", result.c_str());
    }
    return 0;
} 

// 将字符串IP转换为in_addr结构
bool ip_to_in_addr(const std::string& ip_str, in_addr& addr) {
    return inet_pton(AF_INET, ip_str.c_str(), &addr) == 1;
}

// 判断IP是否为本地IP
bool is_local_ip(const std::string& ip) {
    // 1. 检查是否是特殊本地地址
    if (ip == "127.0.0.1" || ip == "localhost" || ip == "::1") {
        return true;
    }

    // 2. 检查是否是IPv4地址
    in_addr addr;
    if (!ip_to_in_addr(ip, addr)) {
        // 不是有效的IPv4地址，可能是IPv6或其他格式
        // 这里简化处理，实际应用中可能需要支持IPv6
        return false;
    }


    // 3. 检查是否是私有地址范围
    // 私有地址范围:
    // 10.0.0.0 - 10.255.255.255
    // 172.16.0.0 - 172.31.255.255
    // 192.168.0.0 - 192.168.255.255
    //uint32_t ip_num = ntohl(addr.s_addr);
    // if ((ip_num >= 0x0A000000 && ip_num <= 0x0AFFFFFF) ||  // 10.0.0.0/8
    //     (ip_num >= 0xAC100000 && ip_num <= 0xAC1FFFFF) ||  // 172.16.0.0/12
    //     (ip_num >= 0xC0A80000 && ip_num <= 0xC0A8FFFF)) {  // 192.168.0.0/16
    //     return true;
    // }

    // 4. 检查是否是当前主机的接口IP
    struct ifaddrs *ifaddr, *ifa;
    if (getifaddrs(&ifaddr) == -1) {
        LOG_ERROR_FMT("Failed to obtain IP address from local network, please confirm if the network card is abnormal");
        return false;
    }

    bool is_local = false;
    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) {
            continue;
        }

        int family = ifa->ifa_addr->sa_family;
        if (family == AF_INET) {
            struct sockaddr_in* sa = (struct sockaddr_in*)ifa->ifa_addr;
            char host[INET_ADDRSTRLEN];
            if (inet_ntop(AF_INET, &(sa->sin_addr), host, INET_ADDRSTRLEN) != nullptr) {
                in_addr interface_addr;
                if (inet_pton(AF_INET, host, &interface_addr) == 1) {
                    if (interface_addr.s_addr == addr.s_addr) {
                        is_local = true;
                        break;
                    }
                }
            }
        }
    }

    freeifaddrs(ifaddr);
    return is_local;
}

// Execute command (remote or local)
int execute_command(const std::string& ip, int port, const std::string& username, 
                const std::string& password, const std::string& command, std::string& result) {

    // 如果明确指定是本地执行，或者IP是本地IP，则本地执行
    bool should_execute_local = is_local_ip(ip);
    
    if (should_execute_local) {

        // Local execution
        LOG_INFO_FMT("Executing local command: %s", command.c_str());
        int ret = local_exec_as_user(username, command, result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to execute command locally: %s", command.c_str());
            return -1;
        }

    } else {

        // Remote execution
        if (remote_ssh_exec(ip, port, username, password, command, result) != 0) {
            LOG_ERROR_FMT("Failed to execute command Remote : %s", command.c_str());
            return -1;
        }
    }

    LOG_INFO_FMT("Command output: %s", result.c_str());
    return 0;
}

int execute_sql_by_psql(const std::string& ip, int port, const std::string& username, 
                const std::string& password, const std::string& command, std::string& result) {

    LOG_INFO_FMT("Executing local command: %s", command.c_str());

    // 如果明确指定是本地执行，或者IP是本地IP，则本地执行
    bool should_execute_local = is_local_ip(ip);
    if (should_execute_local) {
        // Local execution
        int ret = local_exec_as_user(username, command, result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to excute(%s) on server %s, err: %s",command.c_str(),ip.c_str(),result.c_str());
            return -1;
        }

    } else {
        // Remote execution
        if (remote_ssh_exec(ip, port, username, password, command, result) != 0) {
            LOG_ERROR_FMT("Failed to excute(%s) on server %s, err: %s",command.c_str(),ip.c_str(),result.c_str());
            return -1;
        }
    }

    LOG_DEBUG_FMT("Command output: %s", result.c_str());
    return 0;
}

int local_exec_as_user(const std::string& user, const std::string& command, std::string& result) {
    LOG_INFO_FMT("Executing local command as user '%s': %s", user.c_str(), command.c_str());
    
    // 使用sudo -u以指定用户身份执行命令
    std::string full_command = "sudo -u " + user + " bash -c \"" + command + "\"";
    
    std::array<char, 4096> buffer;
    std::stringstream ss;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(full_command.c_str(), "r"), pclose);
    
    if (!pipe) {
        LOG_ERROR_FMT("Failed to execute command as user '%s'", user.c_str());
        return -1;
    }
    
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        ss << buffer.data();
    }
    
    result = ss.str();
    if (!result.empty()) {
        LOG_INFO_FMT("Command output from user '%s': %s", user.c_str(), result.c_str());
    }
    
    return 0;
}

// Base64 编码表
const std::string base64_chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

// 将三个字节转换为四个Base64字符
std::string base64_encode(const unsigned char* bytes_to_encode, unsigned int in_len) {
    std::string ret;
    int i = 0;
    int j = 0;
    unsigned char char_array_3[3];
    unsigned char char_array_4[4];

    while (in_len--) {
        char_array_3[i++] = *(bytes_to_encode++);
        if (i == 3) {
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;

            for(i = 0; (i <4) ; i++)
                ret += base64_chars[char_array_4[i]];
            i = 0;
        }
    }

    if (i)
    {
        for(j = i; j < 3; j++)
            char_array_3[j] = '\0';

        char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
        char_array_4[3] = char_array_3[2] & 0x3f;

        for (j = 0; (j < i + 1); j++)
            ret += base64_chars[char_array_4[j]];

        while((i++ < 3))
            ret += '=';
    }

    return ret;
}

/**
 * @brief 将输入的字符串进行 Base64 编码
 * 
 * 该函数接收一个原始字符串，使用 Base64 编码算法将其转换为可安全传输或存储的
 * Base64 字符串。常用于编码二进制数据或特殊字符，以便嵌入文本协议、命令行、URL等场景。
 *
 * @param input 需要进行 Base64 编码的原始字符串
 * @return std::string 返回编码后的 Base64 字符串（不含换行符）
 * 
 * @note
 * - 编码结果通常可用于 HTTP Basic Auth、命令行参数传递、数据存储等场景。
 * - 本实现通常依赖 OpenSSL 的 Base64 编码接口（如 BIO_f_base64），确保高效和标准兼容。
 * - 返回的 Base64 字符串为连续格式，不包含换行符，适合直接用于管道或嵌入文本。
 */
std::string string_to_base64(const std::string& input) {
    return base64_encode(reinterpret_cast<const unsigned char*>(input.c_str()), input.length());
}


// Check if port is available
int 
check_port_available(const char *ip, int port, const char *username, const char *password, int ssh_port) {
    std::string cmd = "export PATH=/usr/local/bin:/usr/bin:/usr/sbin:$PATH && ss -tln | grep -q ':" + std::to_string(port) + "'";
    std::string result;
    
    int ret = remote_ssh_exec(ip, ssh_port, username, password, cmd, result);
    
    if (ret == 0) {
        // Command executed successfully, check return value
        if (result.empty()) {
            // No port found, means port is available
            return 0;
        } else {
            // Port found, means port is occupied
            return 1;
        }
    } else {
        // Command execution failed
        LOG_ERROR_FMT("Failed to check port %d on %s: %s", port, ip, result.c_str());
        return -1;
    }
}
