#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

namespace edge {

inline constexpr std::int32_t kMaxTcpConnections = 8;

// 请求总超时必须小于正常停止预算
// 后续运行时停止流程也使用这个统一常量
inline constexpr std::int32_t kShutdownBudgetMs = 8000;

struct AgentSettings {
    std::string id = "demo-agent";
    std::string log_level = "info";
};

struct TcpConfig {
    std::string bind = "127.0.0.1";

    std::int32_t port = 19090;
    std::int32_t max_connections = kMaxTcpConnections;
    std::int32_t max_payload_bytes = 8192;

    std::int32_t partial_timeout_ms = 3000;
    std::int32_t silent_after_ms = 30000;
};

struct StorageConfig {
    // 相对路径后续由 YAML 加载器以配置文件所在目录为基准解析。
    std::filesystem::path path = "../run/agent/outbox.db";

    // 与 SQLite 的整数计数类型对应，使用 int64_t。
    std::int64_t max_pending_records = 10000;
    std::int64_t max_total_records = 50000;

    // 这两个保留值都要求为正数。
    std::int32_t acked_keep_days = 7;
    std::int64_t acked_keep_max_records = 20000;
};

struct UploadConfig {
    std::string url =
        "http://127.0.0.1:18080/v1/events/batch";

    std::int32_t batch_size = 20;

    // 不足一批时，允许等待的最长聚合时间。
    std::int32_t flush_interval_ms = 1000;

    // 连接阶段包含在整个请求的时间预算中。
    std::int32_t connect_timeout_ms = 1000;
    std::int32_t request_timeout_ms = 3000;

    std::int32_t retry_initial_ms = 500;
    std::int32_t retry_max_ms = 10000;

    // 达到阈值后告警，实际重试行为在后续上传模块实现。
    std::int32_t failure_warn_attempts = 5;
};

struct RecordingConfig {
    bool enabled = false;

    std::filesystem::path directory = "../run/agent/raw";

    // 单个录制文件的大小上限，单位为字节。
    std::int64_t max_file_bytes = 4 * 1024 * 1024;

    // 全局保留的已关闭录制文件数量。
    // 0 表示不保留已关闭文件。
    std::int32_t keep_closed_files = 8;
};

struct StatusConfig {
    std::int32_t interval_ms = 1000;

    std::filesystem::path file = "../run/agent/status.json";

    // 待处理记录达到该数量时告警。
    std::int64_t pending_warn_records = 1000;

    // 磁盘可用空间告警阈值，后续按 1 MiB = 1024 * 1024 字节解释。
    std::int64_t disk_free_warn_mb = 200;
};

struct AgentConfig {
    std::int32_t version = 1;

    AgentSettings agent;
    TcpConfig tcp;
    StorageConfig storage;
    UploadConfig upload;
    RecordingConfig recording;
    StatusConfig status;
};

} // namespace edge