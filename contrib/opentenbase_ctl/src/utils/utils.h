#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <vector>
#include "../types/types.h"

/**
 * 从包名中提取版本号
 * @param package_name 包名
 * @return 版本号
 */
std::string extract_version_from_package_name(const std::string& package_name);

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

/**
 * 为节点分配可用的端口
 * @param nodes 节点信息数组
 * @param username SSH用户名
 * @param password SSH密码
 * @param ssh_port SSH端口
 * @return 0: 分配成功; 非0: 分配失败
 */
int assign_ports_for_nodes(std::vector<NodeInfo>& nodes, const std::string& username, 
                          const std::string& password, int ssh_port);

/**
 * 检查并获取可用的端口对（node port和pooler port）
 * @param ip 目标主机IP
 * @param start_port 起始端口号
 * @param node_port 返回分配的节点端口
 * @param pooler_port 返回分配的连接池端口
 * @param username SSH用户名
 * @param password SSH密码
 * @param ssh_port SSH端口
 * @return 0: 分配成功; 非0: 分配失败
 */
int get_available_port_pair(const std::string& ip, int start_port, int& node_port, int& pooler_port,
                          const std::string& username, const std::string& password, int ssh_port);

/**
 * 判断软件包名是否是rpm包
 * @param package_name 软件包名称
 * @return true 是; false: 否
 */  
bool is_rpm_package(const std::string& package_name);

/**
 * 判断节点是否是gtm master节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */                          
bool is_master_gtm(std::string node_type);

/**
 * 判断节点是否是gtm slave节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */                          
bool is_slave_gtm(std::string node_type);

/**
 * 判断节点是否是gtm节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */
bool is_gtm_node(std::string node_type);

/**
 * 判断节点是否是cn master节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */
bool is_master_cn(std::string node_type);

/**
 * 判断节点是否是cn slave节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */
bool is_slave_cn(std::string node_type);

/**
 * 判断节点是否是cn节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */
bool is_cn_node(std::string node_type);

/**
 * 判断节点是否是dn master 节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */
bool is_master_dn(std::string node_type);

/**
 * 判断节点是否是dn slave节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */
bool is_slave_dn(std::string node_type);

/**
 * 判断节点是否是dn节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */
bool is_dn_node(std::string node_type);

/**
 * 判断节点是否是 master 节点
 * @param node_type 节点类型
 * @return true 是; false: 否
 */
bool is_master_node(std::string node_type);


/**
 * 是否是融合版本（5.21.*的版本）
 * 说明：与融合版本相对的是较期的 2.15.*、5.05或5.06 的版本
 * @param version 实例版本
 * @return true 是; false: 否
 */
bool is_fusion_version(std::string version);

/**
 * 判断实例是否是集中式
 * 说明：集中式实例只有一组dn节点，没有gtm和cn节点。
 * 集中式节点的guc中多了 allow_dml_on_datanode等几个配置项。
 * @param instance_type 实例类型
 * @return true 是; false: 否
 */
bool is_Centralized_instance(std::string instance_type);

#endif // UTILS_H 