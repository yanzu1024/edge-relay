#pragma once

#include "edge/config/AgentConfig.h"

namespace edge {

struct AgentRunOptions {
    bool jsonl = false;
    bool events_jsonl = false;

    // 只处理既有 Outbox, 不监听 TCP, 也不录制原始流
    bool drain_only = false;
};

// config 已通过配置加载器验证, 输出路径已规范化
// 主线程接收、入库和采样；上传线程使用独立连接
// 运行输出由 owner 管理, 不能由上传线程直接打印
int runAgent(
    const AgentConfig& config,
    const AgentRunOptions& options = {}
);

// 离线导出：持有单实例锁，不启动运行流程。
int exportDead(
    const AgentConfig& config,
    const std::filesystem::path& output_path
);

} // namespace edge