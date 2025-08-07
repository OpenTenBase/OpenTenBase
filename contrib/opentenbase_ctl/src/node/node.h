#ifndef NODE_H
#define NODE_H

#include <pqxx/pqxx>
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

// 创建数据库连接
pqxx::connection* createConnection(const std::string& conninfo);

// 销毁数据库连接
void destroyConnection(pqxx::connection* conn);

// 执行查询SQL并返回结果
pqxx::result executeQuery(pqxx::connection& conn, const std::string& sql);

// 执行DDL语句（创建表、修改表等）
void executeDDL(pqxx::connection& conn, const std::string& ddl);

#endif // NODE_H 