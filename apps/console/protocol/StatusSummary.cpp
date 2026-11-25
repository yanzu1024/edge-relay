#include "StatusSummary.h"
#include "edge/config/AgentConfig.h"

#include <array>
#include <limits>
#include <utility>
#include <initializer_list>

namespace edge::console {

namespace {

using Json = nlohmann::json;

[[noreturn]]
void fail(std::string path, const char* message)
{
    throw StatusDecodeError{
        std::move(path),
        message
    };
}

void require(
    bool condition,
    const std::string& path,
    const char* message
)
{
    if (!condition) {
        fail(path, message);
    }
}

const Json& required(
    const Json& object,
    const char* name,
    const std::string& path
)
{
    require(
        object.is_object(),
        path,
        "Expected an object"
    );

    const auto found = object.find(name);

    require(
        found != object.end(),
        path + "." + name,
        "Required field is missing"
    );

    return *found;
}

std::uint64_t readUnsigned(
    const Json& value,
    const std::string& path
)
{
    // 先检查 unsigned，避免将 UINT64_MAX 转成 int64。
    if (value.is_number_unsigned()) {
        return value.get<std::uint64_t>();
    }

    if (value.is_number_integer()) {
        const auto number = value.get<std::int64_t>();

        if (number >= 0) {
            return static_cast<std::uint64_t>(number);
        }
    }

    fail(path, "Expected a nonnegative integer");
}

std::int64_t readNonnegativeInt64(
    const Json& value,
    const std::string& path
)
{
    const auto number = readUnsigned(value, path);

    require(
        number <= static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max()
        ),
        path,
        "Integer exceeds int64 range"
    );

    return static_cast<std::int64_t>(number);
}

std::int64_t readPositiveInt64(
    const Json& value,
    const std::string& path
)
{
    const auto number = readNonnegativeInt64(value, path);

    require(number > 0, path, "Expected a positive integer");

    return number;
}

std::string readText(
    const Json& value,
    const std::string& path
)
{
    require(value.is_string(), path, "Expected a string");
    return value.get<std::string>();
}

std::optional<bool> readOptionalBool(
    const Json& value,
    const std::string& path
)
{
    if (value.is_null()) {
        return std::nullopt;
    }

    require(value.is_boolean(), path, "Expected boolean or null");
    return value.get<bool>();
}

std::optional<UploadObservation> readUploadObservation(
    const Json& object,
    const std::string& path
)
{
    const auto state_name = readText(
        required(object, "state", path),
        path + ".state"
    );

    if (state_name == "UNKNOWN") {
        constexpr const char* names[] = {
            "initialized",
            "finished",
            "failed",
            "stop_requested",
            "blocked",
            "blocked_http_status"
        };

        for (const char* name : names) {
            require(
                required(object, name, path).is_null(),
                path + "." + name,
                "Unknown upload observation requires null fields"
            );
        }

        return std::nullopt;
    }

    const auto readBoolMember = [&](const char* name) {
        const auto& value = required(object, name, path);

        require(
            value.is_boolean(),
            path + "." + name,
            "Expected a boolean upload field"
        );

        return value.get<bool>();
    };

    UploadObservation result;

    result.initialized = readBoolMember("initialized");
    result.finished = readBoolMember("finished");
    result.failed = readBoolMember("failed");
    result.stop_requested = readBoolMember("stop_requested");
    result.blocked = readBoolMember("blocked");

    const auto& http_status =
        required(object, "blocked_http_status", path);

    if (result.blocked) {
        const auto code = readUnsigned(
            http_status,
            path + ".blocked_http_status"
        );

        require(
            code >= 100 && code <= 599,
            path + ".blocked_http_status",
            "Blocked upload requires an HTTP status from 100 to 599"
        );

        result.blocked_http_status = static_cast<int>(code);
    } else {
        require(
            http_status.is_null(),
            path + ".blocked_http_status",
            "Unblocked upload requires a null HTTP status"
        );
    }

    require(
        !result.failed || result.finished,
        path + ".failed",
        "Failed upload worker must have finished"
    );

    // 与发送端使用相同的状态优先级。
    const char* expected_state = "INITIALIZING";
    result.state = UploadState::Initializing;

    if (result.failed) {
        expected_state = "FAILED";
        result.state = UploadState::Failed;
    } else if (result.finished) {
        expected_state = "STOPPED";
        result.state = UploadState::Stopped;
    } else if (result.stop_requested) {
        expected_state = "STOPPING";
        result.state = UploadState::Stopping;
    } else if (result.blocked) {
        expected_state = "BLOCKED";
        result.state = UploadState::Blocked;
    } else if (result.initialized) {
        expected_state = "ENABLED";
        result.state = UploadState::Enabled;
    }

    require(
        state_name == expected_state,
        path + ".state",
        "Upload state disagrees with its observation fields"
    );

    return result;
}

int readSignedInt(
    const Json& value,
    const std::string& path
)
{
    if (value.is_number_unsigned()) {
        const auto number = value.get<std::uint64_t>();

        require(
            number <= static_cast<std::uint64_t>(
                std::numeric_limits<int>::max()
            ),
            path,
            "Integer exceeds int range"
        );

        return static_cast<int>(number);
    }

    require(
        value.is_number_integer(),
        path,
        "Expected an integer"
    );

    const auto number = value.get<std::int64_t>();

    require(
        number >= std::numeric_limits<int>::min()
            && number <= std::numeric_limits<int>::max(),
        path,
        "Integer exceeds int range"
    );

    return static_cast<int>(number);
}

template <typename Enum>
Enum readNamedEnum(
    const Json& value,
    const std::string& path,
    std::initializer_list<std::pair<const char*, Enum>> names
)
{
    const auto name = readText(value, path);

    for (const auto& entry : names) {
        if (name == entry.first) {
            return entry.second;
        }
    }

    fail(path, "Unknown enumeration value");
}

std::optional<RecordingObservation> readRecordingObservation(
    const Json& object,
    const std::string& path
)
{
    const auto state_name = readText(
        required(object, "state", path),
        path + ".state"
    );

    if (state_name == "UNKNOWN") {
        // 录制未知时，发送端只输出 {"state":"UNKNOWN"}。
        require(
            object.size() == 1,
            path,
            "Unknown recording observation must contain only state"
        );

        return std::nullopt;
    }

    const auto readBoolMember = [&](const char* name) {
        const auto& value = required(object, name, path);

        require(
            value.is_boolean(),
            path + "." + name,
            "Expected a boolean recording field"
        );

        return value.get<bool>();
    };

    const auto readCount = [&](const char* name) {
        return readUnsigned(
            required(object, name, path),
            path + "." + name
        );
    };

    RecordingObservation result;

    result.enabled = readBoolMember("enabled");
    result.operational = readBoolMember("operational");
    result.finished = readBoolMember("finished");

    result.connections_seen = readCount("connections_seen");

    result.bytes_received = readCount("bytes_received");
    result.bytes_written = readCount("bytes_written");
    result.bytes_skipped = readCount("bytes_skipped");

    result.opened_files = readCount("opened_files");
    result.closed_files = readCount("closed_files");

    result.active_sessions = readCount("active_sessions");
    result.active_files = readCount("active_files");

    result.files_deleted = readCount("files_deleted");

    const auto& retained =
        required(object, "retained_closed_files", path);

    if (!retained.is_null()) {
        result.retained_closed_files = readUnsigned(
            retained,
            path + ".retained_closed_files"
        );
    }

    result.startup_errors = readCount("startup_errors");
    result.event_errors = readCount("event_errors");
    result.session_errors = readCount("session_errors");
    result.retention_errors = readCount("retention_errors");

    result.last_error_stage =
        readNamedEnum<RecordingErrorStage>(
            required(object, "last_error_stage", path),
            path + ".last_error_stage",
            {
                {"NONE", RecordingErrorStage::None},
                {"STARTUP", RecordingErrorStage::Startup},
                {"EVENT", RecordingErrorStage::Event},
                {"SESSION", RecordingErrorStage::Session},
                {"RETENTION", RecordingErrorStage::Retention}
            }
        );

    result.last_error_code = readSignedInt(
        required(object, "last_error_code", path),
        path + ".last_error_code"
    );

    result.last_error_connection =
        readCount("last_error_connection");

    result.last_session_operation =
        readNamedEnum<RecordingOperation>(
            required(object, "last_session_operation", path),
            path + ".last_session_operation",
            {
                {"NONE", RecordingOperation::None},
                {"INPUT", RecordingOperation::Input},
                {"METADATA", RecordingOperation::Metadata},
                {"OPEN", RecordingOperation::Open},
                {"WRITE", RecordingOperation::Write},
                {"CLOSE", RecordingOperation::Close},
                {"NOTIFY", RecordingOperation::Notify},
                {"LIMIT", RecordingOperation::Limit}
            }
        );

    // 先比较再相减，避免损坏输入造成无符号下溢。
    require(
        result.bytes_written <= result.bytes_received,
        path + ".bytes_written",
        "Written bytes exceed received bytes"
    );

    require(
        result.bytes_skipped
            == result.bytes_received - result.bytes_written,
        path + ".bytes_skipped",
        "Skipped byte count is inconsistent"
    );

    require(
        result.closed_files <= result.opened_files,
        path + ".closed_files",
        "Closed files exceed opened files"
    );

    require(
        result.active_files <= result.active_sessions,
        path + ".active_files",
        "Active files exceed active sessions"
    );

    require(
        result.enabled || !result.operational,
        path + ".operational",
        "Disabled recording cannot be operational"
    );

    if (result.finished) {
        require(
            !result.operational,
            path + ".operational",
            "Finished recording cannot remain operational"
        );

        require(
            result.active_sessions == 0,
            path + ".active_sessions",
            "Finished recording contains active sessions"
        );

        require(
            result.active_files == 0,
            path + ".active_files",
            "Finished recording contains active files"
        );
    }

    // 与发送端一致：禁用、初始化失败、结束、历史错误依次判断。
    const char* expected_state = "ACTIVE";
    result.state = RecordingState::Active;

    if (!result.enabled) {
        expected_state = "DISABLED";
        result.state = RecordingState::Disabled;
    } else if (
        result.startup_errors != 0
        || (!result.operational && !result.finished)
    ) {
        expected_state = "FAILED";
        result.state = RecordingState::Failed;
    } else if (result.finished) {
        expected_state = "STOPPED";
        result.state = RecordingState::Stopped;
    } else if (
        result.event_errors != 0
        || result.session_errors != 0
        || result.retention_errors != 0
    ) {
        expected_state = "DEGRADED";
        result.state = RecordingState::Degraded;
    }

    require(
        state_name == expected_state,
        path + ".state",
        "Recording state disagrees with its observation fields"
    );

    return result;
}

std::optional<TcpActivityObservation> readTcpActivity(
    const Json& object,
    const std::string& path
)
{
    const auto state_name = readText(
        required(object, "state", path),
        path + ".state"
    );

    if (state_name == "UNKNOWN") {
        require(
            object.size() == 1,
            path,
            "Unknown TCP activity must contain only state"
        );

        return std::nullopt;
    }

    require(
        state_name == "VALID",
        path + ".state",
        "Unsupported TCP activity state"
    );

    const auto readCount = [&](const char* name) {
        return readUnsigned(
            required(object, name, path),
            path + "." + name
        );
    };

    TcpActivityObservation result;

    const auto silent_after_ms = readPositiveInt64(
        required(object, "silent_after_ms", path),
        path + ".silent_after_ms"
    );

    require(
        silent_after_ms
            <= std::numeric_limits<std::int32_t>::max(),
        path + ".silent_after_ms",
        "Silent threshold exceeds int32 range"
    );

    result.silent_after_ms =
        static_cast<std::int32_t>(silent_after_ms);

    result.accepted_connections =
        readCount("accepted_connections");

    result.closed_connections =
        readCount("closed_connections");

    result.rejected_connections =
        readCount("rejected_connections");

    result.client_errors = readCount("client_errors");
    result.bytes_received = readCount("bytes_received");

    result.connected_count = readCount("connected_count");
    result.waiting_count = readCount("waiting_count");
    result.active_count = readCount("active_count");
    result.silent_count = readCount("silent_count");

    const auto& rows = required(object, "connections", path);

    require(
        rows.is_array(),
        path + ".connections",
        "Expected a connection array"
    );

    const auto capacity =
        static_cast<std::size_t>(::edge::kMaxTcpConnections);

    require(
        rows.size() <= capacity,
        path + ".connections",
        "Connection array exceeds the supported capacity"
    );

    require(
        result.connected_count
            == static_cast<std::uint64_t>(rows.size()),
        path + ".connected_count",
        "Connection count disagrees with the array size"
    );

    require(
        result.closed_connections <= result.accepted_connections,
        path + ".closed_connections",
        "Closed connections exceed accepted connections"
    );

    require(
        result.client_errors <= result.closed_connections,
        path + ".client_errors",
        "Client errors exceed closed connections"
    );

    // 先检查大小，再相减，避免无符号下溢。
    require(
        result.accepted_connections - result.closed_connections
            == result.connected_count,
        path + ".connected_count",
        "Accepted and closed counts disagree with current connections"
    );

    result.connections.reserve(rows.size());

    std::optional<std::uint64_t> previous_slot;

    std::uint64_t waiting = 0;
    std::uint64_t active = 0;
    std::uint64_t silent = 0;

    std::uint64_t remaining_bytes = result.bytes_received;

    for (std::size_t index = 0; index < rows.size(); ++index) {
        const auto& row = rows[index];

        const auto row_path =
            path + ".connections[" + std::to_string(index) + "]";

        const auto readRowCount = [&](const char* name) {
            return readUnsigned(
                required(row, name, row_path),
                row_path + "." + name
            );
        };

        TcpConnectionObservation connection;

        connection.slot = readRowCount("slot");

        require(
            connection.slot
                < static_cast<std::uint64_t>(
                    ::edge::kMaxTcpConnections
                ),
            row_path + ".slot",
            "Connection slot exceeds the supported range"
        );

        require(
            !previous_slot.has_value()
                || connection.slot > *previous_slot,
            row_path + ".slot",
            "Connection slots must be strictly increasing"
        );

        previous_slot = connection.slot;

        connection.connection_id = readRowCount("connection_id");

        require(
            connection.connection_id != 0
                && connection.connection_id
                    <= result.accepted_connections,
            row_path + ".connection_id",
            "Invalid connection identity"
        );

        for (const auto& earlier : result.connections) {
            require(
                earlier.connection_id != connection.connection_id,
                row_path + ".connection_id",
                "Duplicate connection identity"
            );
        }

        connection.state = readNamedEnum<TcpConnectionState>(
            required(row, "state", row_path),
            row_path + ".state",
            {
                {"WAITING", TcpConnectionState::Waiting},
                {"ACTIVE", TcpConnectionState::Active},
                {"SILENT", TcpConnectionState::Silent}
            }
        );

        connection.connected_ms = readRowCount("connected_ms");
        connection.bytes_received = readRowCount("bytes_received");

        require(
            connection.bytes_received <= remaining_bytes,
            row_path + ".bytes_received",
            "Current connection bytes exceed the cumulative total"
        );

        remaining_bytes -= connection.bytes_received;

        const auto& age =
            required(row, "last_data_age_ms", row_path);

        if (!age.is_null()) {
            connection.last_data_age_ms = readUnsigned(
                age,
                row_path + ".last_data_age_ms"
            );
        }

        const bool has_data =
            connection.last_data_age_ms.has_value();

        require(
            has_data == (connection.bytes_received != 0),
            row_path + ".last_data_age_ms",
            "Receive age disagrees with the received byte count"
        );

        if (has_data) {
            require(
                *connection.last_data_age_ms
                    <= connection.connected_ms,
                row_path + ".last_data_age_ms",
                "Receive age exceeds connection age"
            );
        }

        const auto idle_ms =
            connection.last_data_age_ms.value_or(
                connection.connected_ms
            );

        auto expected = TcpConnectionState::Waiting;

        if (idle_ms >= static_cast<std::uint64_t>(
                result.silent_after_ms
            )) {
            expected = TcpConnectionState::Silent;
        } else if (has_data) {
            expected = TcpConnectionState::Active;
        }

        require(
            connection.state == expected,
            row_path + ".state",
            "Connection state disagrees with its receive age"
        );

        switch (connection.state) {
        case TcpConnectionState::Waiting:
            ++waiting;
            break;

        case TcpConnectionState::Active:
            ++active;
            break;

        case TcpConnectionState::Silent:
            ++silent;
            break;
        }

        result.connections.push_back(std::move(connection));
    }

    // 通过实际连接逐项计数后比较，避免累加不可信输入。
    require(
        result.waiting_count == waiting,
        path + ".waiting_count",
        "Waiting count disagrees with connection rows"
    );

    require(
        result.active_count == active,
        path + ".active_count",
        "Active count disagrees with connection rows"
    );

    require(
        result.silent_count == silent,
        path + ".silent_count",
        "Silent count disagrees with connection rows"
    );

    // 没有关闭过连接时，总字节应全部归属于当前连接。
    // 已关闭过连接时，剩余字节可以来自历史连接。
    require(
        result.closed_connections != 0 || remaining_bytes == 0,
        path + ".bytes_received",
        "Cumulative bytes disagree with current connections"
    );

    return result;
}

ProcessState readProcessState(
    const Json& value,
    const std::string& path
)
{
    const auto name = readText(value, path);

    if (name == "UNKNOWN") {
        return ProcessState::Unknown;
    }
    if (name == "STARTING") {
        return ProcessState::Starting;
    }
    if (name == "RUNNING") {
        return ProcessState::Running;
    }
    if (name == "STOPPING") {
        return ProcessState::Stopping;
    }
    if (name == "STOPPED") {
        return ProcessState::Stopped;
    }
    if (name == "FAILED") {
        return ProcessState::Failed;
    }

    fail(path, "Unknown process state");
}

SampleState readSampleState(
    const Json& value,
    const std::string& path
)
{
    const auto name = readText(value, path);

    if (name == "UNKNOWN") {
        return SampleState::Unknown;
    }
    if (name == "VALID") {
        return SampleState::Valid;
    }
    if (name == "STALE") {
        return SampleState::Stale;
    }
    if (name == "ERROR") {
        return SampleState::Error;
    }

    fail(path, "Unknown sample state");
}

template <typename T, typename Decoder>
SampleValue<T> readSample(
    const Json& object,
    const std::string& path,
    Decoder decode
)
{
    SampleValue<T> result;

    result.state = readSampleState(
        required(object, "state", path),
        path + ".state"
    );

    const auto& time = required(object, "checked_at_ms", path);
    const auto& value = required(object, "value", path);
    const auto& error = required(object, "error", path);

    if (result.state == SampleState::Unknown) {
        require(
            time.is_null() && value.is_null() && error.is_null(),
            path,
            "Unknown sample must contain null time, value and error"
        );

        return result;
    }

    result.checked_at_ms = readPositiveInt64(
        time,
        path + ".checked_at_ms"
    );

    if (result.state == SampleState::Error) {
        require(
            value.is_null(),
            path + ".value",
            "Failed sample must not contain a value"
        );

        result.error = readText(error, path + ".error");

        require(
            !result.error.empty(),
            path + ".error",
            "Failed sample requires a nonempty error"
        );

        return result;
    }

    require(
        error.is_null(),
        path + ".error",
        "Valid or stale sample must not contain an error"
    );

    require(
        !value.is_null(),
        path + ".value",
        "Valid or stale sample requires a value"
    );

    result.value = decode(value, path + ".value");

    return result;
}

OutboxCounts readOutboxCounts(
    const Json& object,
    const std::string& path
)
{
    const auto read = [&](const char* name) {
        return readNonnegativeInt64(
            required(object, name, path),
            path + "." + name
        );
    };

    OutboxCounts result{
        read("pending"),
        read("sending"),
        read("retry"),
        read("acked"),
        read("dead"),
        read("total"),
        read("unacked"),
        read("backlog")
    };

    // 用逐项减法核对总数，避免无效输入导致加法溢出。
    std::int64_t remaining = result.total;

    const std::array<std::int64_t, 5> states{
        result.pending,
        result.sending,
        result.retry,
        result.acked,
        result.dead
    };

    for (const auto count : states) {
        require(
            count <= remaining,
            path,
            "Outbox state counts exceed total"
        );

        remaining -= count;
    }

    require(
        remaining == 0
            && result.unacked == result.total - result.acked
            && result.dead <= result.unacked
            && result.backlog == result.unacked - result.dead,
        path,
        "Inconsistent Outbox total, unacked or backlog"
    );

    return result;
}

std::map<std::string, std::uint64_t> readCounters(
    const Json& object,
    const std::string& path
)
{
    require(object.is_object(), path, "Expected counters object");

    std::map<std::string, std::uint64_t> result;

    for (auto it = object.begin(); it != object.end(); ++it) {
        result.emplace(
            it.key(),
            readUnsigned(it.value(), path + "." + it.key())
        );
    }

    // 协议 v1 已有字段必须存在，缺失不能默认补成 0。
    constexpr const char* names[] = {
        "completed_frames",
        "valid_events",
        "json_errors",
        "validation_errors",
        "partial_timeouts",
        "stored_events",
        "duplicate_events",
        "identity_conflicts",
        "capacity_rejections",
        "serialization_errors",
        "storage_errors",
        "storage_skipped",
        "acked_deleted",
        "recovered_sending",
        "upload_cycles",
        "upload_claimed",
        "upload_acked",
        "upload_retry",
        "upload_dead",
        "upload_request_failures",
        "upload_http_failures",
        "upload_protocol_failures",
        "upload_errors",
        "upload_warnings"
    };

    for (const char* name : names) {
        require(
            result.find(name) != result.end(),
            path + "." + name,
            "Required counter is missing"
        );
    }

    return result;
}

} // namespace

StatusSummaryResult decodeStatusSummary(
    const JsonlMessage& message
)
{
    try {
        StatusSummary result;

        const Json* snapshot = nullptr;
        std::string path;

        if (message.kind == JsonlMessageKind::Status) {
            snapshot = &message.data;
            path = "data";
        } else if (message.kind == JsonlMessageKind::Stopped) {
            const auto exit_code = readUnsigned(
                required(message.data, "exit_code", "data"),
                "data.exit_code"
            );

            require(
                exit_code <= 1,
                "data.exit_code",
                "Unsupported Agent runtime exit code"
            );

            result.exit_code = static_cast<int>(exit_code);

            snapshot = &required(
                message.data,
                "snapshot",
                "data"
            );

            path = "data.snapshot";
        } else {
            fail("type", "Expected status or stopped message");
        }

        const auto& object = *snapshot;

        const auto readUnsignedMember = [&](const char* name) {
            return readUnsigned(
                required(object, name, path),
                path + "." + name
            );
        };

        require(
            readUnsignedMember("snapshot_version") == 1,
            path + ".snapshot_version",
            "Unsupported snapshot version"
        );

        result.agent_id = readText(
            required(object, "agent_id", path),
            path + ".agent_id"
        );

        result.run_id = readText(
            required(object, "run_id", path),
            path + ".run_id"
        );

        require(
            !result.agent_id.empty(),
            path + ".agent_id",
            "Agent ID must not be empty"
        );

        require(
            !result.run_id.empty()
                && result.run_id == message.run_id,
            path + ".run_id",
            "Snapshot and message run IDs disagree"
        );

        result.sequence = message.sequence;

        result.checked_at_ms = readPositiveInt64(
            required(object, "checked_at_ms", path),
            path + ".checked_at_ms"
        );

        result.uptime_ms = readUnsignedMember("uptime_ms");

        result.process_state = readProcessState(
            required(object, "process_state", path),
            path + ".process_state"
        );

        const auto& active =
            required(object, "active_connections", path);

        if (!active.is_null()) {
            result.active_connections = readUnsigned(
                active,
                path + ".active_connections"
            );
        }

        result.upload = readUploadObservation(
            required(object, "upload", path),
            path + ".upload"
        );

        result.recording = readRecordingObservation(
            required(object, "recording", path),
            path + ".recording"
        );

        result.tcp_activity = readTcpActivity(
            required(object, "tcp_activity", path),
            path + ".tcp_activity"
        );

        if (result.tcp_activity.has_value()
            && result.active_connections.has_value()) {
            require(
                result.tcp_activity->connected_count
                    == *result.active_connections,
                path + ".active_connections",
                "Top-level connection count disagrees with TCP activity"
            );
        }

        result.counters = readCounters(
            required(object, "counters", path),
            path + ".counters"
        );

        result.display_dropped =
            readUnsignedMember("display_dropped");

        result.outbox = readSample<OutboxCounts>(
            required(object, "outbox", path),
            path + ".outbox",
            readOutboxCounts
        );

        result.disk_available_bytes = readSample<std::uint64_t>(
            required(object, "disk_available_bytes", path),
            path + ".disk_available_bytes",
            readUnsigned
        );

        const auto& thresholds =
            required(object, "thresholds", path);

        const auto threshold_path = path + ".thresholds";

        result.unacked_warn_records = readPositiveInt64(
            required(
                thresholds,
                "unacked_warn_records",
                threshold_path
            ),
            threshold_path + ".unacked_warn_records"
        );

        result.disk_available_warn_bytes = readUnsigned(
            required(
                thresholds,
                "disk_available_warn_bytes",
                threshold_path
            ),
            threshold_path + ".disk_available_warn_bytes"
        );

        require(
            result.disk_available_warn_bytes > 0,
            threshold_path + ".disk_available_warn_bytes",
            "Disk warning threshold must be positive"
        );

        const auto& warnings = required(object, "warnings", path);
        const auto warning_path = path + ".warnings";

        result.unacked_warning = readOptionalBool(
            required(warnings, "unacked_records", warning_path),
            warning_path + ".unacked_records"
        );

        result.disk_warning = readOptionalBool(
            required(warnings, "disk_available", warning_path),
            warning_path + ".disk_available"
        );

        if (result.outbox.state == SampleState::Valid) {
            const bool expected =
                result.outbox.value->unacked
                >= result.unacked_warn_records;

            require(
                result.unacked_warning.has_value()
                    && *result.unacked_warning == expected,
                warning_path + ".unacked_records",
                "Outbox warning disagrees with the valid sample"
            );
        } else {
            require(
                !result.unacked_warning.has_value(),
                warning_path + ".unacked_records",
                "Nonvalid Outbox sample requires an unknown warning"
            );
        }

        if (result.disk_available_bytes.state == SampleState::Valid) {
            const bool expected =
                *result.disk_available_bytes.value
                <= result.disk_available_warn_bytes;

            require(
                result.disk_warning.has_value()
                    && *result.disk_warning == expected,
                warning_path + ".disk_available",
                "Disk warning disagrees with the valid sample"
            );
        } else {
            require(
                !result.disk_warning.has_value(),
                warning_path + ".disk_available",
                "Nonvalid disk sample requires an unknown warning"
            );
        }

        if (result.exit_code.has_value()) {
            const auto expected = *result.exit_code == 0
                ? ProcessState::Stopped
                : ProcessState::Failed;

            require(
                result.process_state == expected,
                path + ".process_state",
                "Exit code and final process state disagree"
            );

            require(
                result.active_connections.has_value()
                    && *result.active_connections == 0,
                path + ".active_connections",
                "Stopped snapshot requires a known zero connection count"
            );

            require(
                result.upload.has_value()
                    && result.upload->finished,
                path + ".upload",
                "Stopped snapshot requires a finished upload worker"
            );

            require(
                *result.exit_code != 0 || !result.upload->failed,
                path + ".upload.failed",
                "Successful stop cannot contain upload worker failure"
            );
        }

        return result;
    }
    catch (const StatusDecodeError& error) {
        return error;
    }
}

} // namespace edge::console
