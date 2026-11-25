#include "edge/status/StatusSnapshot.h"

#include <nlohmann/json.hpp>

#include <array>
#include <limits>
#include <stdexcept>
#include <utility>

namespace edge {

namespace {

using Json = nlohmann::json;

const char* processStateName(StatusProcessState state)
{
    switch (state) {
    case StatusProcessState::Unknown:
        return "UNKNOWN";
    case StatusProcessState::Starting:
        return "STARTING";
    case StatusProcessState::Running:
        return "RUNNING";
    case StatusProcessState::Stopping:
        return "STOPPING";
    case StatusProcessState::Stopped:
        return "STOPPED";
    case StatusProcessState::Failed:
        return "FAILED";
    }

    throw std::invalid_argument("Invalid process state");
}

const char* sampleStateName(StatusSampleState state)
{
    switch (state) {
    case StatusSampleState::Unknown:
        return "UNKNOWN";
    case StatusSampleState::Valid:
        return "VALID";
    case StatusSampleState::Stale:
        return "STALE";
    case StatusSampleState::Error:
        return "ERROR";
    }

    throw std::invalid_argument("Invalid sample state");
}

template <typename T, typename Encoder>
Json sampleJson(
    const StatusSample<T>& sample,
    Encoder encode
)
{
    const char* state_name = sampleStateName(sample.state);

    switch (sample.state) {
    case StatusSampleState::Unknown:
        if (sample.value.has_value()
            || sample.checked_at_ms.has_value()
            || !sample.error.empty()) {
            throw std::invalid_argument(
                "Unknown sample must not contain data, time or error"
            );
        }
        break;

    case StatusSampleState::Valid:
    case StatusSampleState::Stale:
        if (!sample.value.has_value()
            || !sample.checked_at_ms.has_value()
            || !sample.error.empty()) {
            throw std::invalid_argument(
                "Valid or stale sample requires data and time only"
            );
        }
        break;

    case StatusSampleState::Error:
        if (sample.value.has_value()
            || !sample.checked_at_ms.has_value()
            || sample.error.empty()) {
            throw std::invalid_argument(
                "Failed sample requires time and error, without data"
            );
        }
        break;
    }

    if (sample.checked_at_ms.has_value()
        && *sample.checked_at_ms <= 0) {
        throw std::invalid_argument(
            "Sample timestamp must be positive"
        );
    }

    Json result = {
        {"state", state_name},
        {"checked_at_ms", nullptr},
        {"value", nullptr},
        {"error", nullptr}
    };

    if (sample.checked_at_ms.has_value()) {
        result["checked_at_ms"] = *sample.checked_at_ms;
    }

    if (sample.value.has_value()) {
        result["value"] = encode(*sample.value);
    }

    if (!sample.error.empty()) {
        result["error"] = sample.error;
    }

    return result;
}

Json outboxCountsJson(const OutboxStateCounts& counts)
{
    if (counts.total < 0) {
        throw std::invalid_argument(
            "Outbox total must not be negative"
        );
    }

    // 与现有 stateCounts() 一样，用逐项减法验证，
    // 避免直接累加损坏输入时发生有符号整数溢出。
    std::int64_t remaining = counts.total;

    const std::array<std::int64_t, 5> states{
        counts.pending,
        counts.sending,
        counts.retry,
        counts.acked,
        counts.dead
    };

    for (const auto count : states) {
        if (count < 0 || count > remaining) {
            throw std::invalid_argument(
                "Outbox state counts are inconsistent"
            );
        }

        remaining -= count;
    }

    if (remaining != 0
        || counts.unacked != counts.total - counts.acked) {
        throw std::invalid_argument(
            "Outbox total or unacked count is inconsistent"
        );
    }

    // 上面的检查已保证减法结果非负且不会溢出。
    const auto backlog = counts.unacked - counts.dead;

    return {
        {"pending", counts.pending},
        {"sending", counts.sending},
        {"retry", counts.retry},
        {"acked", counts.acked},
        {"dead", counts.dead},
        {"total", counts.total},
        {"unacked", counts.unacked},
        {"backlog", backlog}
    };
}

Json uploadJson(
    const std::optional<StatusUploadObservation>& observation
)
{
    Json result = {
        {"state", "UNKNOWN"},
        {"initialized", nullptr},
        {"finished", nullptr},
        {"failed", nullptr},
        {"stop_requested", nullptr},
        {"blocked", nullptr},
        {"blocked_http_status", nullptr}
    };

    if (!observation.has_value()) {
        return result;
    }

    const auto& upload = *observation;

    if ((!upload.blocked && upload.blocked_http_status != 0)
        || (upload.blocked
            && (upload.blocked_http_status < 100
                || upload.blocked_http_status > 599))) {
        throw std::invalid_argument(
            "Upload blocked flag and HTTP status are inconsistent"
        );
    }

    // 当前 UploadWorker 同时发布 failure 和 finished。
    if (upload.failed && !upload.finished) {
        throw std::invalid_argument(
            "Failed upload worker must have finished"
        );
    }

    const char* state = "INITIALIZING";

    if (upload.failed) {
        state = "FAILED";
    } else if (upload.finished) {
        state = "STOPPED";
    } else if (upload.stop_requested) {
        state = "STOPPING";
    } else if (upload.blocked) {
        state = "BLOCKED";
    } else if (upload.initialized) {
        state = "ENABLED";
    }

    result["state"] = state;
    result["initialized"] = upload.initialized;
    result["finished"] = upload.finished;
    result["failed"] = upload.failed;
    result["stop_requested"] = upload.stop_requested;
    result["blocked"] = upload.blocked;

    if (upload.blocked) {
        result["blocked_http_status"] =
            upload.blocked_http_status;
    }

    return result;
}

const char* recordingErrorStageName(
    RawRecorderErrorStage stage
)
{
    switch (stage) {
    case RawRecorderErrorStage::None:
        return "NONE";

    case RawRecorderErrorStage::Startup:
        return "STARTUP";

    case RawRecorderErrorStage::Event:
        return "EVENT";

    case RawRecorderErrorStage::Session:
        return "SESSION";

    case RawRecorderErrorStage::Retention:
        return "RETENTION";
    }

    throw std::invalid_argument(
        "Invalid recording error stage"
    );
}

const char* recordingOperationName(
    RawRecordingOperation operation
)
{
    switch (operation) {
    case RawRecordingOperation::None:
        return "NONE";

    case RawRecordingOperation::Input:
        return "INPUT";

    case RawRecordingOperation::Metadata:
        return "METADATA";

    case RawRecordingOperation::Open:
        return "OPEN";

    case RawRecordingOperation::Write:
        return "WRITE";

    case RawRecordingOperation::Close:
        return "CLOSE";

    case RawRecordingOperation::Notify:
        return "NOTIFY";

    case RawRecordingOperation::Limit:
        return "LIMIT";
    }

    throw std::invalid_argument(
        "Invalid recording operation"
    );
}

Json recordingJson(
    const std::optional<RawRecorderStats>& observation
)
{
    if (!observation.has_value()) {
        return {
            {"state", "UNKNOWN"}
        };
    }

    const auto& recording = *observation;

    // 先比较，再做减法，避免不合法输入发生无符号下溢。
    if (recording.bytes_written > recording.bytes_received ||
        recording.bytes_skipped !=
            recording.bytes_received - recording.bytes_written) {
        throw std::invalid_argument(
            "Inconsistent recording byte counts"
        );
    }

    if (recording.closed_files > recording.opened_files ||
        recording.active_files > recording.active_sessions ||
        (!recording.enabled && recording.operational) ||
        (recording.finished &&
            (recording.operational ||
             recording.active_sessions != 0 ||
             recording.active_files != 0))) {
        throw std::invalid_argument(
            "Inconsistent recording lifecycle"
        );
    }

    const char* stage = recordingErrorStageName(
        recording.last_error_stage
    );

    const char* operation = recordingOperationName(
        recording.last_session_operation
    );

    const char* state = "ACTIVE";

    if (!recording.enabled) {
        state = "DISABLED";
    } else if (
        recording.startup_errors != 0 ||
        (!recording.operational && !recording.finished)
    ) {
        state = "FAILED";
    } else if (recording.finished) {
        state = "STOPPED";
    } else if (
        recording.event_errors != 0 ||
        recording.session_errors != 0 ||
        recording.retention_errors != 0
    ) {
        state = "DEGRADED";
    }

    Json result = {
        {"state", state},

        {"enabled", recording.enabled},
        {"operational", recording.operational},
        {"finished", recording.finished},

        {"connections_seen", recording.connections_seen},

        {"bytes_received", recording.bytes_received},
        {"bytes_written", recording.bytes_written},
        {"bytes_skipped", recording.bytes_skipped},

        {"opened_files", recording.opened_files},
        {"closed_files", recording.closed_files},

        {"active_sessions", recording.active_sessions},
        {"active_files", recording.active_files},

        {"files_deleted", recording.files_deleted},
        {"retained_closed_files", nullptr},

        {"startup_errors", recording.startup_errors},
        {"event_errors", recording.event_errors},
        {"session_errors", recording.session_errors},
        {"retention_errors", recording.retention_errors},

        {"last_error_stage", stage},
        {"last_error_code", recording.last_error_code},
        {"last_error_connection",
            recording.last_error_connection},
        {"last_session_operation", operation}
    };

    if (recording.retained_closed_files.has_value()) {
        result["retained_closed_files"] =
            *recording.retained_closed_files;
    }

    return result;
}

const char* tcpConnectionStateName(TcpConnectionState state)
{
    switch (state) {
    case TcpConnectionState::Waiting:
        return "WAITING";

    case TcpConnectionState::Active:
        return "ACTIVE";

    case TcpConnectionState::Silent:
        return "SILENT";
    }

    throw std::invalid_argument(
        "Invalid TCP connection state"
    );
}

Json tcpActivityJson(
    const std::optional<TcpActivitySnapshot>& observation
)
{
    if (!observation.has_value()) {
        return {
            {"state", "UNKNOWN"}
        };
    }

    const auto& activity = *observation;

    if (
        activity.silent_after_ms <= 0 ||
        activity.connections.size() >
            static_cast<std::size_t>(kMaxTcpConnections) ||
        activity.connected_count != activity.connections.size() ||
        activity.closed_connections > activity.accepted_connections ||
        activity.client_errors > activity.closed_connections ||
        activity.accepted_connections - activity.closed_connections
            != activity.connected_count
    ) {
        throw std::invalid_argument(
            "Inconsistent TCP activity counters"
        );
    }

    Json rows = Json::array();

    std::size_t waiting = 0;
    std::size_t active = 0;
    std::size_t silent = 0;

    std::uint64_t remaining_bytes = activity.bytes_received;

    std::optional<std::size_t> previous_slot;

    for (std::size_t index = 0;
         index < activity.connections.size();
         ++index) {
        const auto& connection = activity.connections[index];

        if (
            connection.slot >=
                static_cast<std::size_t>(kMaxTcpConnections) ||
            (previous_slot.has_value() &&
                connection.slot <= *previous_slot) ||
            connection.connection_id == 0 ||
            connection.connection_id > activity.accepted_connections ||
            connection.bytes_received > remaining_bytes
        ) {
            throw std::invalid_argument(
                "Invalid TCP activity connection"
            );
        }

        previous_slot = connection.slot;

        for (std::size_t earlier = 0;
             earlier < index;
             ++earlier) {
            if (activity.connections[earlier].connection_id
                == connection.connection_id) {
                throw std::invalid_argument(
                    "Duplicate TCP activity connection ID"
                );
            }
        }

        remaining_bytes -= connection.bytes_received;

        const bool has_data =
            connection.last_data_age_ms.has_value();

        if (
            has_data != (connection.bytes_received != 0) ||
            (has_data &&
                *connection.last_data_age_ms >
                    connection.connected_ms)
        ) {
            throw std::invalid_argument(
                "Inconsistent TCP activity receive age"
            );
        }

        const auto idle_ms =
            connection.last_data_age_ms.value_or(
                connection.connected_ms
            );

        TcpConnectionState expected =
            TcpConnectionState::Waiting;

        if (idle_ms >=
            static_cast<std::uint64_t>(
                activity.silent_after_ms
            )) {
            expected = TcpConnectionState::Silent;
        } else if (has_data) {
            expected = TcpConnectionState::Active;
        }

        if (connection.state != expected) {
            throw std::invalid_argument(
                "TCP activity state disagrees with receive age"
            );
        }

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

        Json row = {
            {"slot", connection.slot},
            {"connection_id", connection.connection_id},
            {"state", tcpConnectionStateName(connection.state)},
            {"connected_ms", connection.connected_ms},
            {"last_data_age_ms", nullptr},
            {"bytes_received", connection.bytes_received}
        };

        if (has_data) {
            row["last_data_age_ms"] =
                *connection.last_data_age_ms;
        }

        rows.push_back(std::move(row));
    }

    if (
        waiting != activity.waiting_count ||
        active != activity.active_count ||
        silent != activity.silent_count ||
        (activity.closed_connections == 0 &&
            remaining_bytes != 0)
    ) {
        throw std::invalid_argument(
            "TCP activity distribution is inconsistent"
        );
    }

    return {
        {"state", "VALID"},
        {"silent_after_ms", activity.silent_after_ms},

        {"accepted_connections", activity.accepted_connections},
        {"closed_connections", activity.closed_connections},
        {"rejected_connections", activity.rejected_connections},
        {"client_errors", activity.client_errors},

        {"bytes_received", activity.bytes_received},

        {"connected_count", activity.connected_count},
        {"waiting_count", activity.waiting_count},
        {"active_count", activity.active_count},
        {"silent_count", activity.silent_count},

        {"connections", std::move(rows)}
    };
}

Json countersJson(const StatusCounters& counters)
{
    return {
        {"completed_frames", counters.completed_frames},
        {"valid_events", counters.valid_events},
        {"json_errors", counters.json_errors},
        {"validation_errors", counters.validation_errors},
        {"partial_timeouts", counters.partial_timeouts},

        {"stored_events", counters.stored_events},
        {"duplicate_events", counters.duplicate_events},
        {"identity_conflicts", counters.identity_conflicts},
        {"capacity_rejections", counters.capacity_rejections},
        {"serialization_errors", counters.serialization_errors},
        {"storage_errors", counters.storage_errors},
        {"storage_skipped", counters.storage_skipped},
        {"acked_deleted", counters.acked_deleted},
        {"recovered_sending", counters.recovered_sending},

        {"upload_cycles", counters.upload_cycles},
        {"upload_claimed", counters.upload_claimed},
        {"upload_acked", counters.upload_acked},
        {"upload_retry", counters.upload_retry},
        {"upload_dead", counters.upload_dead},
        {"upload_request_failures", counters.upload_request_failures},
        {"upload_http_failures", counters.upload_http_failures},
        {"upload_protocol_failures", counters.upload_protocol_failures},
        {"upload_errors", counters.upload_errors},
        {"upload_warnings", counters.upload_warnings}
    };
}

} // namespace

std::string serializeStatusSnapshot(
    const StatusSnapshot& snapshot,
    const StatusConfig& config
)
{
    if (snapshot.agent_id.empty()) {
        throw std::invalid_argument(
            "Status agent ID must not be empty"
        );
    }

    if (snapshot.checked_at_ms <= 0) {
        throw std::invalid_argument(
            "Status timestamp must be positive"
        );
    }

    const auto& monitoring = snapshot.monitoring;

    if (monitoring.previous_successful_snapshot_at_ms.has_value()
        && *monitoring.previous_successful_snapshot_at_ms <= 0) {
        throw std::invalid_argument(
            "Previous successful snapshot time must be positive"
        );
    }

    if (config.pending_warn_records <= 0
        || config.disk_free_warn_mb <= 0) {
        throw std::invalid_argument(
            "Status warning thresholds must be positive"
        );
    }

    constexpr std::uint64_t bytes_per_mib = 1024ULL * 1024ULL;

    const auto disk_warn_mib =
        static_cast<std::uint64_t>(config.disk_free_warn_mb);

    if (disk_warn_mib
        > std::numeric_limits<std::uint64_t>::max()
            / bytes_per_mib) {
        throw std::invalid_argument(
            "Disk warning threshold cannot be represented in bytes"
        );
    }

    const auto disk_warn_bytes = disk_warn_mib * bytes_per_mib;

    const auto outbox = sampleJson(
        snapshot.outbox,
        outboxCountsJson
    );

    const auto disk = sampleJson(
        snapshot.disk_available_bytes,
        [](std::uint64_t bytes) -> Json {
            return bytes;
        }
    );

    Json unacked_warning = nullptr;
    Json disk_warning = nullptr;

    // 过期值可以展示，但不能据此宣称当前告警正常或异常。
    if (snapshot.outbox.state == StatusSampleState::Valid) {
        unacked_warning =
            snapshot.outbox.value->unacked
            >= config.pending_warn_records;
    }

    if (snapshot.disk_available_bytes.state
        == StatusSampleState::Valid) {
        disk_warning =
            *snapshot.disk_available_bytes.value
            <= disk_warn_bytes;
    }

    if (
        snapshot.tcp_activity.has_value() &&
        snapshot.active_connections.has_value() &&
        snapshot.tcp_activity->connected_count !=
            *snapshot.active_connections
    ) {
        throw std::invalid_argument(
            "TCP activity and active connection count disagree"
        );
    }

    Json result = {
        {"snapshot_version", kStatusSnapshotVersion},
        {"agent_id", snapshot.agent_id},
        {"run_id", nullptr},
        {"console", nullptr},
        {"checked_at_ms", snapshot.checked_at_ms},
        {"uptime_ms", snapshot.uptime_ms},
        {"process_state", processStateName(snapshot.process_state)},
        {"active_connections", nullptr},
        {"upload", uploadJson(snapshot.upload)},
        {"recording", recordingJson(snapshot.recording)},
        {"tcp_activity", tcpActivityJson(snapshot.tcp_activity)},
        {"counters", countersJson(snapshot.counters)},
        {"display_dropped", snapshot.display_dropped},
        {"monitoring", {
            {"outbox_query_errors",
                monitoring.outbox_query_errors},
            {"disk_query_errors",
                monitoring.disk_query_errors},
            {"output_errors",
                monitoring.output_errors},
            {"previous_successful_writes",
                monitoring.previous_successful_writes},
            {"previous_successful_snapshot_at_ms", nullptr}
        }},
        {"outbox", outbox},
        {"disk_available_bytes", disk},
        {"thresholds", {
            {"unacked_warn_records", config.pending_warn_records},
            {"disk_available_warn_bytes", disk_warn_bytes}
        }},
        {"warnings", {
            {"unacked_records", unacked_warning},
            {"disk_available", disk_warning}
        }}
    };

    if (snapshot.active_connections.has_value()) {
        result["active_connections"] =
            *snapshot.active_connections;
    }

    if (monitoring.previous_successful_snapshot_at_ms.has_value()) {
        result["monitoring"]
              ["previous_successful_snapshot_at_ms"] =
            *monitoring.previous_successful_snapshot_at_ms;
    }

    if (!snapshot.run_id.empty()) {
        result["run_id"] = snapshot.run_id;
    }

    if (snapshot.console.has_value()) {
        const auto& console = *snapshot.console;

        result["console"] = {
            {"mode", console.jsonl ? "jsonl" : "text"},
            {"stdout_enabled", console.stdout_enabled},
            {"stderr_enabled", console.stderr_enabled},
            {"encoding_errors", console.encoding_errors},
            {"stdout_text_dropped", console.stdout_text_dropped},
            {"diagnostic_dropped", console.diagnostic_dropped},
            {"text_truncated", console.text_truncated},
            {"io_errors", console.io_errors},
            {"status_coalesced", console.status_coalesced},
            {"record_queue_drops", console.record_queue_drops},
            {"record_rate_drops", console.record_rate_drops},
            {"abandoned_lines", console.abandoned_lines},
            {"partial_lines_abandoned",
                console.partial_lines_abandoned},
            {"stdout_error", console.stdout_error},
            {"stderr_error", console.stderr_error},
            {"last_diagnostic", console.last_diagnostic}
        };
    }

    return result.dump();
}

} // namespace edge