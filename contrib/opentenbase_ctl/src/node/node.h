#ifndef NODE_H
#define NODE_H

#include "../types/types.h"

// 创建节点目录
int create_node_directories(NodeInfo *node, OpentenbaseConfig *install);

// 删除节点目录
int delete_node_directories(NodeInfo *node, OpentenbaseConfig *install);

// 配置 PostgreSQL
int configure_postgresql_for_cdw(NodeInfo *node, OpentenbaseConfig *install);

// 配置 PostgreSQL
int configure_postgresql_for_mpp(NodeInfo *node, OpentenbaseConfig *install);

// 配置 gtm 主节点
int configure_gtm_node(NodeInfo *node, OpentenbaseConfig *install);

// 配置 gtm 备节点
int configure_gtm_slave_node(NodeInfo *node, OpentenbaseConfig *install);

// 配置 cn 主节点
int configure_cn_node(NodeInfo *node, OpentenbaseConfig *install);

// 配置 cn 备节点
int configure_cn_slave_node(NodeInfo *node, OpentenbaseConfig *install);

// 配置 dn 主节点
int configure_dn_node(NodeInfo *node, OpentenbaseConfig *install);

// 配置 dn 备节点
int configure_dn_slave_node(NodeInfo *node, OpentenbaseConfig *install);

// 配置 pg_hba.conf
int configure_pg_hba(NodeInfo *node, OpentenbaseConfig *install);

// 启动节点
int start_node(NodeInfo *node, OpentenbaseConfig *install);

// 停止节点
int stop_node(NodeInfo *node, OpentenbaseConfig *install);

// 传输并解压安装包
int transfer_and_extract_package(NodeInfo *node, OpentenbaseConfig *install);

// 并发执行命令
int excute_cmd_concurrency(OpentenbaseConfig *configInfo, const std::vector<std::string>& server_list);

// excute sql concurrency
int excute_sql_concurrency(OpentenbaseConfig *configInfo, const std::vector<NodeInfo>& node_list);

/**
 * @brief 筛选满足 is_op_node 条件的节点
 * 
 * @param config OpentenbaseConfig 结构体的引用，包含所有节点信息
 * @param is_op_node 布尔值，用于筛选节点的 is_op_node 属性
 * @return std::vector<NodeInfo> 满足条件的节点集合
 */
std::vector<NodeInfo> filterNodesByOpStatus(const OpentenbaseConfig& config, bool is_op_node);

// 辅助函数：转义字符串中的双引号
std::string escapeQuotes(const std::string& input);

// 主函数：更新 config_items 中的值
void updateConfigItems(
    std::vector<std::pair<std::string, std::string>>& config_items,
    const std::map<std::string, std::string>& guc_cfg);

#endif // NODE_H 