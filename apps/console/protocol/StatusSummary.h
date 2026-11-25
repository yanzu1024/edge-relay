#pragma once

#include "JsonlMessageParser.h"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace edge::console {

enum class ProcessState {
    Unknown,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed
};

enum class SampleState {
    Unknown,
    Valid,
    Stale,
    Error
};

enum class UploadState {
    Initializing,
    Enabled,
    Blocked,
    Stopping,
    Stopped,
    Failed
};

struct UploadObservation {
    UploadState state = UploadState::Initializing;

    bool initialized = false;
    bool finished = false;
    bool failed = false;
    bool stop_requested = false;

    // 本次运行的阻塞观察，停止后仍可能保留。
    bool blocked = false;
    std::optional<int> blocked_http_status;
};

enum class RecordingState {
    Disabled,
    Active,
    Degraded,
    Failed,
    Stopped
};

enum class RecordingErrorStage {
    None,
    Startup,
    Event,
    Session,
    Retention
};

enum class RecordingOperation {
    None,
    Input,
    Metadata,
    Open,
    Write,
    Close,
    Notify,
    Limit
};

struct RecordingObservation {
    RecordingState state = RecordingState::Active;

    bool enabled = false;
    bool operational = false;
    bool finished = false;

    std::uint64_t connections_seen = 0;

    std::uint64_t bytes_received = 0;
    std::uint64_t bytes_written = 0;
    std::uint64_t bytes_skipped = 0;

    std::uint64_t opened_files = 0;
    std::uint64_t closed_files = 0;

    std::uint64_t active_sessions = 0;
    std::uint64_t active_files = 0;

    std::uint64_t files_deleted = 0;

    // 未知与已知保留 0 个文件分开。
    std::optional<std::uint64_t> retained_closed_files;

    std::uint64_t startup_errors = 0;
    std::uint64_t event_errors = 0;
    std::uint64_t session_errors = 0;
    std::uint64_t retention_errors = 0;

    RecordingErrorStage last_error_stage =
        RecordingErrorStage::None;

    int last_error_code = 0;
    std::uint64_t last_error_connection = 0;

    // 仅 last_error_stage == Session 时有会话操作含义。
    RecordingOperation last_session_operation =
        RecordingOperation::None;
};

enum class TcpConnectionState {
    Waiting,
    Active,
    Silent
};

struct TcpConnectionObservation {
    std::uint64_t slot = 0;
    std::uint64_t connection_id = 0;

    TcpConnectionState state = TcpConnectionState::Waiting;

    // 从本次连接接入开始经过的时间。
    std::uint64_t connected_ms = 0;

    // nullopt 表示尚未收到非空数据；0 是有效间隔。
    std::optional<std::uint64_t> last_data_age_ms;

    std::uint64_t bytes_received = 0;
};

struct TcpActivityObservation {
    std::int32_t silent_after_ms = 0;

    // 本次运行累计，包含已经关闭的连接。
    std::uint64_t accepted_connections = 0;
    std::uint64_t closed_connections = 0;
    std::uint64_t rejected_connections = 0;
    std::uint64_t client_errors = 0;
    std::uint64_t bytes_received = 0;

    // 当前连接数量及状态分布。
    std::uint64_t connected_count = 0;
    std::uint64_t waiting_count = 0;
    std::uint64_t active_count = 0;
    std::uint64_t silent_count = 0;

    // 仅保存当前连接，按槽位递增排列。
    std::vector<TcpConnectionObservation> connections;
};

template <typename T>
struct SampleValue {
    SampleState state = SampleState::Unknown;

    std::optional<std::int64_t> checked_at_ms;
    std::optional<T> value;

    // UNKNOWN、VALID、STALE 时为空。
    std::string error;
};

struct OutboxCounts {
    std::int64_t pending = 0;
    std::int64_t sending = 0;
    std::int64_t retry = 0;
    std::int64_t acked = 0;
    std::int64_t dead = 0;

    std::int64_t total = 0;
    std::int64_t unacked = 0;
    std::int64_t backlog = 0;
};

// 展示所需的基础摘要，不拥有线程、数据库或控件。
struct StatusSummary {
    std::string agent_id;
    std::string run_id;

    std::uint64_t sequence = 0;
    std::int64_t checked_at_ms = 0;
    std::uint64_t uptime_ms = 0;

    ProcessState process_state = ProcessState::Unknown;

    // 未知与有效的 0 分开表达。
    std::optional<std::uint64_t> active_connections;

    // nullopt 对应协议中的 upload.state=UNKNOWN。
    // 有值时，各布尔字段均为已知观察值。
    std::optional<UploadObservation> upload;

    // nullopt 对应 recording.state=UNKNOWN。
    std::optional<RecordingObservation> recording;

    // nullopt 对应 tcp_activity.state=UNKNOWN。
    std::optional<TcpActivityObservation> tcp_activity;

    // 本次运行累计；与数据库当前存量分开。
    std::map<std::string, std::uint64_t> counters;

    std::uint64_t display_dropped = 0;

    SampleValue<OutboxCounts> outbox;
    SampleValue<std::uint64_t> disk_available_bytes;

    std::int64_t unacked_warn_records = 0;
    std::uint64_t disk_available_warn_bytes = 0;

    // nullopt 表示无法依据当前采样判断告警。
    std::optional<bool> unacked_warning;
    std::optional<bool> disk_warning;

    // 仅 stopped 消息有值，0 是有效退出码。
    std::optional<int> exit_code;
};

struct StatusDecodeError {
    std::string path;
    std::string message;
};

using StatusSummaryResult = std::variant<
    StatusSummary,
    StatusDecodeError
>;

// 输入来自消息解析器，不重新解析 JSON 文本。
// 纯转换函数：不读取时钟，不执行文件或数据库操作。
[[nodiscard]]
StatusSummaryResult decodeStatusSummary(
    const JsonlMessage& message
);

} // namespace edge::console
