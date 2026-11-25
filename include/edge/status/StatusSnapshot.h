#pragma once

#include "edge/config/AgentConfig.h"
#include "edge/storage/OutboxStore.h"
#include "edge/recording/RawRecorder.h"
#include "edge/status/TcpActivityTracker.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace edge {

inline constexpr std::int32_t kStatusSnapshotVersion = 1;

enum class StatusProcessState {
    Unknown,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed
};

enum class StatusSampleState {
    Unknown,
    Valid,
    Stale,
    Error
};

// Valid / Stale：value 和 checked_at_ms 必须有值，error 为空。
// Error：value 无值，checked_at_ms 有值，error 非空。
// Unknown：value、checked_at_ms 无值，error 为空。
//
// Stale 的 checked_at_ms 保留原数据的采样时间，
// 不能改成当前序列化时间来掩盖数据过期。
template <typename T>
struct StatusSample {
    StatusSampleState state = StatusSampleState::Unknown;

    std::optional<T> value;
    std::optional<std::int64_t> checked_at_ms;

    std::string error;
};

// 由主线程取得 UploadWorkerStatus 后复制。
// initialized 对应 ready；failed 对应 failure 是否存在。
// stop_requested 由持有 worker 的 owner 提供。
// blocked 及状态码由主线程已经消费的报告提供。
//
// 这里不保存线程、数据库、exception_ptr 或其他对象的引用。
struct StatusUploadObservation {
    bool initialized = false;
    bool finished = false;
    bool failed = false;
    bool stop_requested = false;

    // 本进程曾进入上传阻塞后保持为 true，直到进程结束。
    bool blocked = false;

    // blocked=false 时为 0；blocked=true 时保存触发状态码。
    long blocked_http_status = 0;
};

// 字段沿用现有最终统计的命名。
// 全部是本次运行累计，不是数据库当前存量。
struct StatusCounters {
    std::uint64_t completed_frames = 0;
    std::uint64_t valid_events = 0;
    std::uint64_t json_errors = 0;
    std::uint64_t validation_errors = 0;
    std::uint64_t partial_timeouts = 0;

    std::uint64_t stored_events = 0;
    std::uint64_t duplicate_events = 0;
    std::uint64_t identity_conflicts = 0;
    std::uint64_t capacity_rejections = 0;
    std::uint64_t serialization_errors = 0;
    std::uint64_t storage_errors = 0;
    std::uint64_t storage_skipped = 0;
    std::uint64_t acked_deleted = 0;
    std::uint64_t recovered_sending = 0;

    std::uint64_t upload_cycles = 0;
    std::uint64_t upload_claimed = 0;
    std::uint64_t upload_acked = 0;
    std::uint64_t upload_retry = 0;
    std::uint64_t upload_dead = 0;
    std::uint64_t upload_request_failures = 0;
    std::uint64_t upload_http_failures = 0;
    std::uint64_t upload_protocol_failures = 0;
    std::uint64_t upload_errors = 0;
    std::uint64_t upload_warnings = 0;
};

struct StatusMonitoringCounters {
    std::uint64_t outbox_query_errors = 0;
    std::uint64_t disk_query_errors = 0;

    // 包含快照编码、目标路径检查及文件更新失败。
    std::uint64_t output_errors = 0;

    // 不包含正在序列化的这次写入。
    std::uint64_t previous_successful_writes = 0;

    // 上一份成功发布快照的采样时间。
    std::optional<std::int64_t>
        previous_successful_snapshot_at_ms;
};

struct StatusConsoleObservation {
    bool jsonl = false;
    bool stdout_enabled = false;
    bool stderr_enabled = false;

    std::uint64_t encoding_errors = 0;
    std::uint64_t stdout_text_dropped = 0;
    std::uint64_t diagnostic_dropped = 0;
    std::uint64_t text_truncated = 0;

    std::uint64_t io_errors = 0;
    std::uint64_t status_coalesced = 0;
    std::uint64_t record_queue_drops = 0;
    std::uint64_t record_rate_drops = 0;
    std::uint64_t abandoned_lines = 0;
    std::uint64_t partial_lines_abandoned = 0;

    int stdout_error = 0;
    int stderr_error = 0;

    std::string last_diagnostic;
};

// 拥有自身数据的快照值对象。
// 调用方复制完数据后，再执行序列化或后续文件 I/O。
struct StatusSnapshot {
    std::string agent_id;

    // 空字符串表示没有提供运行身份。
    std::string run_id;

    std::optional<StatusConsoleObservation> console;

    std::int64_t checked_at_ms = 0;
    std::uint64_t uptime_ms = 0;

    StatusProcessState process_state = StatusProcessState::Unknown;

    // nullopt 表示尚未取得可靠的观察值；0 是有效连接数。
    std::optional<std::size_t> active_connections;

    // nullopt 表示上传状态未知。
    std::optional<StatusUploadObservation> upload;

    // nullopt 表示尚未取得录制管理器的观察值。
    // 禁用录制使用 enabled=false，不能用 nullopt 代替。
    std::optional<RawRecorderStats> recording;

    // nullopt 表示尚未取得连接活动观察值
    // 已知没有连接时，保存 connected_count=0 的有效快照
    std::optional<TcpActivitySnapshot> tcp_activity;

    StatusCounters counters;

    // 只统计展示输出损失，不表示业务事件或上传报告丢失。
    std::uint64_t display_dropped = 0;

    StatusMonitoringCounters monitoring;

    StatusSample<OutboxStateCounts> outbox;

    // 后续由采样层查询业务数据库所在文件系统的可用空间。
    StatusSample<std::uint64_t> disk_available_bytes;
};

// 纯序列化：不查询数据库、磁盘、时钟，不写文件或控制台。
// 返回紧凑 JSON 文本，不附加换行。
// 不合法的模型或阈值抛 std::invalid_argument。
// JSON 编码异常继续向调用方传播。
std::string serializeStatusSnapshot(
    const StatusSnapshot& snapshot,
    const StatusConfig& config
);

} // namespace edge