#include "config/config.h"
#include "cluster/cluster.h"
#include "command/command.h"
#include "log/log.h"
#include "types/types.h"
#include <iostream>
#include <string>

int main(int argc, char** argv) {

    // 设置日志级别为 DEBUG
    set_log_level(LOG_LEVEL_DEBUG);

    // 获取当前时间戳
    time_t now = time(nullptr);
    set_timestamp(std::to_string(now));

    // 解析配置文件、命令行参数
    OpentenbaseConfig config;
    CommandLineArgs args;

    // 解析命令行参数
    if (!parse_command_line(argc, argv, args, config)) {
        return 1;
    }

    // 打印 config，为了分析节点分布情况，一般用于debug
    printOpentenbaseConfig(config);

    // 根据命令执行相应操作
    int ret = 0;
    if (Constants::COMMAND_TYPE_INSTALL == args.command) {
        ret = install_command(&config);
    }
    else if (Constants::COMMAND_TYPE_DELETE == args.command) {
        ret = delete_command(&config);
    }
    else if (Constants::COMMAND_TYPE_START == args.command) {
        ret = start_command(&config);
    }
    else if (Constants::COMMAND_TYPE_STOP == args.command) {
        ret = stop_command(&config);
    }
    else if (Constants::COMMAND_TYPE_STATUS == args.command) {
        ret = status_command(&config);
    }
    
    LOG_INFO_FMT("The execution of the opentenbase_ctl tool has ended.");
    return ret;
} 
