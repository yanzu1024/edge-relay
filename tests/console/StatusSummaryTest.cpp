#include "protocol/StatusSummary.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace {

using Json = nlohmann::json;

using edge::console::JsonlMessage;
using edge::console::JsonlMessageKind;
using edge::console::ProcessState;
using edge::console::SampleState;
using edge::console::StatusDecodeError;
using edge::console::StatusSummary;
using edge::console::StatusSummaryResult;
using edge::console::decodeStatusSummary;
using edge::console::UploadState;
using edge::console::RecordingState;
using edge::console::RecordingErrorStage;
using edge::console::RecordingOperation;
using edge::console::TcpConnectionState;

const std::string kRunId(32, 'a');

Json unknownSample()
{
    return {
        {"state", "UNKNOWN"},
        {"checked_at_ms", nullptr},
        {"value", nullptr},
        {"error", nullptr}
    };
}

Json enabledUpload()
{
    return {
        {"state", "ENABLED"},
        {"initialized", true},
        {"finished", false},
        {"failed", false},
        {"stop_requested", false},
        {"blocked", false},
        {"blocked_http_status", nullptr}
    };
}

Json finishedUpload(bool failed = false)
{
    auto result = enabledUpload();

    result["state"] = failed ? "FAILED" : "STOPPED";
    result["finished"] = true;
    result["failed"] = failed;
    result["stop_requested"] = true;

    return result;
}

Json activeRecording()
{
    return {
        {"state", "ACTIVE"},
        {"enabled", true},
        {"operational", true},
        {"finished", false},

        {"connections_seen", 0},

        {"bytes_received", 0},
        {"bytes_written", 0},
        {"bytes_skipped", 0},

        {"opened_files", 0},
        {"closed_files", 0},

        {"active_sessions", 0},
        {"active_files", 0},

        {"files_deleted", 0},
        {"retained_closed_files", nullptr},

        {"startup_errors", 0},
        {"event_errors", 0},
        {"session_errors", 0},
        {"retention_errors", 0},

        {"last_error_stage", "NONE"},
        {"last_error_code", 0},
        {"last_error_connection", 0},
        {"last_session_operation", "NONE"}
    };
}

Json sample(
    const std::string& state,
    Json value,
    std::int64_t checked_at_ms = 123
)
{
    return {
        {"state", state},
        {"checked_at_ms", checked_at_ms},
        {"value", std::move(value)},
        {"error", nullptr}
    };
}

Json emptyOutbox()
{
    return {
        {"pending", 0},
        {"sending", 0},
        {"retry", 0},
        {"acked", 0},
        {"dead", 0},
        {"total", 0},
        {"unacked", 0},
        {"backlog", 0}
    };
}

Json snapshot()
{
    Json counters = Json::object();

    for (const char* name : {
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
        }) {
        counters[name] = 0;
    }

    return {
        {"snapshot_version", 1},
        {"agent_id", "demo-agent"},
        {"run_id", kRunId},
        {"checked_at_ms", 1000},
        {"uptime_ms", 0},
        {"process_state", "RUNNING"},
        {"active_connections", nullptr},
        {"upload", {
            {"state", "UNKNOWN"},
            {"initialized", nullptr},
            {"finished", nullptr},
            {"failed", nullptr},
            {"stop_requested", nullptr},
            {"blocked", nullptr},
            {"blocked_http_status", nullptr}
        }},
        {"recording", {
            {"state", "UNKNOWN"}
        }},
        {"tcp_activity", {
            {"state", "UNKNOWN"}
        }},
        {"counters", counters},
        {"display_dropped", 0},
        {"outbox", unknownSample()},
        {"disk_available_bytes", unknownSample()},
        {"thresholds", {
            {"unacked_warn_records", 1000},
            {"disk_available_warn_bytes", 209715200}
        }},
        {"warnings", {
            {"unacked_records", nullptr},
            {"disk_available", nullptr}
        }}
    };
}

Json emptyTcpActivity()
{
    return {
        {"state", "VALID"},
        {"silent_after_ms", 1000},

        {"accepted_connections", 0},
        {"closed_connections", 0},
        {"rejected_connections", 0},
        {"client_errors", 0},
        {"bytes_received", 0},

        {"connected_count", 0},
        {"waiting_count", 0},
        {"active_count", 0},
        {"silent_count", 0},

        {"connections", Json::array()}
    };
}

Json tcpConnection(
    std::uint64_t slot,
    std::uint64_t connection_id,
    const std::string& state,
    std::uint64_t connected_ms,
    Json last_data_age_ms,
    std::uint64_t bytes_received
)
{
    return {
        {"slot", slot},
        {"connection_id", connection_id},
        {"state", state},
        {"connected_ms", connected_ms},
        {"last_data_age_ms", std::move(last_data_age_ms)},
        {"bytes_received", bytes_received}
    };
}

Json twoActiveTcpConnections()
{
    auto result = emptyTcpActivity();

    result["accepted_connections"] = 2;
    result["connected_count"] = 2;
    result["active_count"] = 2;
    result["bytes_received"] = 10;

    result["connections"] = Json::array({
        tcpConnection(0, 1, "ACTIVE", 200, 0, 4),
        tcpConnection(1, 2, "ACTIVE", 100, 0, 6)
    });

    return result;
}

StatusSummaryResult decode(Json data)
{
    return decodeStatusSummary(JsonlMessage{
        JsonlMessageKind::Status,
        kRunId,
        2,
        1000,
        std::move(data)
    });
}

bool accepted(const StatusSummaryResult& result)
{
    return std::holds_alternative<StatusSummary>(result);
}

void expectErrorPath(
    const StatusSummaryResult& result,
    const std::string& expected
)
{
    const auto* error = std::get_if<StatusDecodeError>(&result);

    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->path, expected);
}

TEST(StatusSummaryTest, PreservesUnknownObservations)
{
    const auto result = decode(snapshot());

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_EQ(value.process_state, ProcessState::Running);
    EXPECT_FALSE(value.active_connections.has_value());

    EXPECT_EQ(value.outbox.state, SampleState::Unknown);
    EXPECT_FALSE(value.outbox.value.has_value());
    EXPECT_FALSE(value.outbox.checked_at_ms.has_value());

    EXPECT_FALSE(value.disk_available_bytes.value.has_value());
    EXPECT_FALSE(value.unacked_warning.has_value());
    EXPECT_FALSE(value.disk_warning.has_value());
    EXPECT_FALSE(value.exit_code.has_value());
}

TEST(StatusSummaryTest, PreservesValidZeroAndItsWarning)
{
    auto data = snapshot();

    data["active_connections"] = 0;
    data["outbox"] = sample("VALID", emptyOutbox());
    data["disk_available_bytes"] = sample("VALID", 0);

    data["warnings"]["unacked_records"] = false;
    data["warnings"]["disk_available"] = true;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.active_connections.has_value());
    EXPECT_EQ(*value.active_connections, 0U);

    ASSERT_TRUE(value.outbox.value.has_value());
    EXPECT_EQ(value.outbox.value->total, 0);

    ASSERT_TRUE(value.disk_available_bytes.value.has_value());
    EXPECT_EQ(*value.disk_available_bytes.value, 0U);

    ASSERT_TRUE(value.disk_warning.has_value());
    EXPECT_TRUE(*value.disk_warning);
}

TEST(StatusSummaryTest, PreservesStaleValueAndOriginalTimestamp)
{
    auto data = snapshot();

    auto counts = emptyOutbox();
    counts["pending"] = 3;
    counts["dead"] = 2;
    counts["total"] = 5;
    counts["unacked"] = 5;
    counts["backlog"] = 3;

    data["outbox"] = sample("STALE", counts, 123);

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_EQ(value.checked_at_ms, 1000);
    EXPECT_EQ(value.outbox.state, SampleState::Stale);

    ASSERT_TRUE(value.outbox.checked_at_ms.has_value());
    EXPECT_EQ(*value.outbox.checked_at_ms, 123);

    ASSERT_TRUE(value.outbox.value.has_value());
    EXPECT_EQ(value.outbox.value->unacked, 5);
    EXPECT_EQ(value.outbox.value->backlog, 3);

    EXPECT_FALSE(value.unacked_warning.has_value());
}

TEST(StatusSummaryTest, KeepsQueryErrorSeparateFromZero)
{
    auto data = snapshot();

    data["outbox"] = {
        {"state", "ERROR"},
        {"checked_at_ms", 123},
        {"value", nullptr},
        {"error", "query failed"}
    };

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_EQ(value.outbox.state, SampleState::Error);
    EXPECT_FALSE(value.outbox.value.has_value());
    EXPECT_EQ(value.outbox.error, "query failed");
    EXPECT_FALSE(value.unacked_warning.has_value());
}

TEST(StatusSummaryTest, SeparatesRunCountersFromDatabaseInventory)
{
    auto data = snapshot();

    auto counts = emptyOutbox();
    counts["pending"] = 3;
    counts["dead"] = 2;
    counts["total"] = 5;
    counts["unacked"] = 5;
    counts["backlog"] = 3;

    data["outbox"] = sample("VALID", counts);
    data["warnings"]["unacked_records"] = false;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_EQ(value.counters.at("stored_events"), 0U);

    ASSERT_TRUE(value.outbox.value.has_value());
    EXPECT_EQ(value.outbox.value->total, 5);
    EXPECT_EQ(value.outbox.value->dead, 2);
    EXPECT_EQ(value.outbox.value->backlog, 3);
}

TEST(StatusSummaryTest, PreservesMaximumUint64Values)
{
    auto data = snapshot();

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    data["counters"]["stored_events"] = maximum;
    data["display_dropped"] = maximum;
    data["disk_available_bytes"] = sample("VALID", maximum);
    data["warnings"]["disk_available"] = false;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_EQ(value.counters.at("stored_events"), maximum);
    EXPECT_EQ(value.display_dropped, maximum);

    ASSERT_TRUE(value.disk_available_bytes.value.has_value());
    EXPECT_EQ(*value.disk_available_bytes.value, maximum);
}

TEST(StatusSummaryTest, RejectsInvalidSampleCombinations)
{
    std::vector<Json> invalid_samples;

    auto bad = unknownSample();
    bad["value"] = 0;
    invalid_samples.push_back(bad);

    bad = sample("VALID", 0);
    bad["checked_at_ms"] = nullptr;
    invalid_samples.push_back(bad);

    bad = sample("ERROR", 0);
    bad["error"] = "failed";
    invalid_samples.push_back(bad);

    bad = sample("STALE", 0, 0);
    invalid_samples.push_back(bad);

    bad = sample("ERROR", nullptr);
    bad["error"] = "";
    invalid_samples.push_back(bad);

    for (const auto& invalid : invalid_samples) {
        auto data = snapshot();
        data["disk_available_bytes"] = invalid;

        EXPECT_FALSE(accepted(decode(data)));
    }
}

TEST(StatusSummaryTest, RejectsMissingAndNonintegerCounters)
{
    auto data = snapshot();
    data["counters"].erase("stored_events");

    expectErrorPath(
        decode(data),
        "data.counters.stored_events"
    );

    for (const Json& invalid : {
            Json(-1),
            Json(true),
            Json(0.5)
        }) {
        data = snapshot();
        data["counters"]["stored_events"] = invalid;

        expectErrorPath(
            decode(data),
            "data.counters.stored_events"
        );
    }
}

TEST(StatusSummaryTest, RejectsInconsistentOutboxCounts)
{
    auto data = snapshot();

    auto counts = emptyOutbox();
    counts["total"] = 1;

    data["outbox"] = sample("VALID", counts);
    data["warnings"]["unacked_records"] = false;

    expectErrorPath(
        decode(data),
        "data.outbox.value"
    );
}

TEST(StatusSummaryTest, RejectsOutboxSumWithoutOverflowing)
{
    auto data = snapshot();

    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    auto counts = emptyOutbox();
    counts["pending"] = maximum;
    counts["sending"] = maximum;
    counts["total"] = maximum;
    counts["unacked"] = maximum;
    counts["backlog"] = maximum;

    data["outbox"] = sample("VALID", counts);
    data["warnings"]["unacked_records"] = true;

    expectErrorPath(
        decode(data),
        "data.outbox.value"
    );
}

TEST(StatusSummaryTest, RejectsWarningsFromUnknownSamples)
{
    auto data = snapshot();
    data["warnings"]["unacked_records"] = false;

    expectErrorPath(
        decode(data),
        "data.warnings.unacked_records"
    );
}

TEST(StatusSummaryTest, RejectsWarningThatDisagreesWithValidSample)
{
    auto data = snapshot();

    data["disk_available_bytes"] = sample("VALID", 0);
    data["warnings"]["disk_available"] = false;

    expectErrorPath(
        decode(data),
        "data.warnings.disk_available"
    );
}

TEST(StatusSummaryTest, RejectsInvalidIdentityAndRequiredFields)
{
    auto data = snapshot();
    data["run_id"] = std::string(32, 'b');

    expectErrorPath(decode(data), "data.run_id");

    data = snapshot();
    data.erase("uptime_ms");

    expectErrorPath(decode(data), "data.uptime_ms");

    data = snapshot();
    data["process_state"] = "UPLOADING";

    expectErrorPath(decode(data), "data.process_state");

    data = snapshot();
    data["snapshot_version"] = 2;

    expectErrorPath(decode(data), "data.snapshot_version");
}

TEST(StatusSummaryTest, DecodesStoppedAndPreservesExitCodeZero)
{
    auto final_snapshot = snapshot();

    final_snapshot["process_state"] = "STOPPED";
    final_snapshot["active_connections"] = 0;
    final_snapshot["upload"] = finishedUpload();

    const auto result = decodeStatusSummary(JsonlMessage{
        JsonlMessageKind::Stopped,
        kRunId,
        3,
        1000,
        {
            {"exit_code", 0},
            {"snapshot", final_snapshot}
        }
    });

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_EQ(value.process_state, ProcessState::Stopped);

    ASSERT_TRUE(value.exit_code.has_value());
    EXPECT_EQ(*value.exit_code, 0);

    ASSERT_TRUE(value.upload.has_value());
    EXPECT_EQ(value.upload->state, UploadState::Stopped);
    EXPECT_TRUE(value.upload->finished);
}

TEST(StatusSummaryTest, RejectsFinalStateThatDisagreesWithExitCode)
{
    auto final_snapshot = snapshot();

    final_snapshot["process_state"] = "FAILED";
    final_snapshot["active_connections"] = 0;
    final_snapshot["upload"] = finishedUpload();

    const auto result = decodeStatusSummary(JsonlMessage{
        JsonlMessageKind::Stopped,
        kRunId,
        3,
        1000,
        {
            {"exit_code", 0},
            {"snapshot", final_snapshot}
        }
    });

    expectErrorPath(
        result,
        "data.snapshot.process_state"
    );
}

TEST(StatusSummaryTest, DistinguishesUnknownUploadFromKnownFalse)
{
    const auto unknown = decode(snapshot());

    ASSERT_TRUE(accepted(unknown));
    EXPECT_FALSE(
        std::get<StatusSummary>(unknown).upload.has_value()
    );

    auto data = snapshot();
    auto upload = enabledUpload();

    upload["state"] = "INITIALIZING";
    upload["initialized"] = false;

    data["upload"] = upload;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.upload.has_value());
    EXPECT_EQ(value.upload->state, UploadState::Initializing);
    EXPECT_FALSE(value.upload->initialized);
    EXPECT_FALSE(value.upload->finished);
    EXPECT_FALSE(value.upload->failed);
}

TEST(StatusSummaryTest, DecodesEnabledUpload)
{
    auto data = snapshot();
    data["upload"] = enabledUpload();

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.upload.has_value());
    EXPECT_EQ(value.upload->state, UploadState::Enabled);
    EXPECT_TRUE(value.upload->initialized);
    EXPECT_FALSE(value.upload->blocked);
    EXPECT_FALSE(value.upload->blocked_http_status.has_value());
}

TEST(StatusSummaryTest, PreservesBlockedHistoryAcrossLifecycleStates)
{
    const auto check = [](const Json& upload, UploadState expected) {
        auto data = snapshot();
        data["upload"] = upload;

        const auto result = decode(data);

        ASSERT_TRUE(accepted(result));

        const auto& value = std::get<StatusSummary>(result);

        ASSERT_TRUE(value.upload.has_value());
        EXPECT_EQ(value.upload->state, expected);
        EXPECT_TRUE(value.upload->blocked);

        ASSERT_TRUE(value.upload->blocked_http_status.has_value());
        EXPECT_EQ(*value.upload->blocked_http_status, 403);
    };

    auto upload = enabledUpload();

    upload["state"] = "BLOCKED";
    upload["blocked"] = true;
    upload["blocked_http_status"] = 403;

    check(upload, UploadState::Blocked);

    upload["state"] = "STOPPING";
    upload["stop_requested"] = true;

    check(upload, UploadState::Stopping);

    upload["state"] = "STOPPED";
    upload["finished"] = true;

    check(upload, UploadState::Stopped);

    upload["state"] = "FAILED";
    upload["failed"] = true;

    check(upload, UploadState::Failed);
}

TEST(StatusSummaryTest, RejectsPartialUnknownUpload)
{
    auto data = snapshot();

    // UNKNOWN 时不能把一个未知字段擅自补成 false。
    data["upload"]["initialized"] = false;

    expectErrorPath(
        decode(data),
        "data.upload.initialized"
    );
}

TEST(StatusSummaryTest, RejectsNonbooleanUploadFields)
{
    for (const Json& invalid : {
            Json(nullptr),
            Json(0),
            Json("false")
        }) {
        auto data = snapshot();
        data["upload"] = enabledUpload();
        data["upload"]["initialized"] = invalid;

        expectErrorPath(
            decode(data),
            "data.upload.initialized"
        );
    }
}

TEST(StatusSummaryTest, RejectsFailedWorkerThatHasNotFinished)
{
    auto data = snapshot();

    data["upload"] = enabledUpload();
    data["upload"]["state"] = "FAILED";
    data["upload"]["failed"] = true;

    expectErrorPath(
        decode(data),
        "data.upload.failed"
    );
}

TEST(StatusSummaryTest, ValidatesBlockedHttpStatus)
{
    for (const Json& invalid : {
            Json(nullptr),
            Json(99),
            Json(600),
            Json(-1),
            Json(true),
            Json(403.0)
        }) {
        auto data = snapshot();

        data["upload"] = enabledUpload();
        data["upload"]["state"] = "BLOCKED";
        data["upload"]["blocked"] = true;
        data["upload"]["blocked_http_status"] = invalid;

        expectErrorPath(
            decode(data),
            "data.upload.blocked_http_status"
        );
    }

    auto data = snapshot();

    data["upload"] = enabledUpload();
    data["upload"]["blocked_http_status"] = 403;

    expectErrorPath(
        decode(data),
        "data.upload.blocked_http_status"
    );
}

TEST(StatusSummaryTest, RejectsUploadStateThatDisagreesWithFlags)
{
    auto data = snapshot();

    data["upload"] = enabledUpload();
    data["upload"]["state"] = "BLOCKED";

    expectErrorPath(
        decode(data),
        "data.upload.state"
    );

    data["upload"]["state"] = "UPLOADING";

    expectErrorPath(
        decode(data),
        "data.upload.state"
    );
}

TEST(StatusSummaryTest, RejectsStoppedWithUnknownUpload)
{
    auto final_snapshot = snapshot();

    final_snapshot["process_state"] = "STOPPED";
    final_snapshot["active_connections"] = 0;

    const auto result = decodeStatusSummary(JsonlMessage{
        JsonlMessageKind::Stopped,
        kRunId,
        3,
        1000,
        {
            {"exit_code", 0},
            {"snapshot", final_snapshot}
        }
    });

    expectErrorPath(
        result,
        "data.snapshot.upload"
    );
}

TEST(StatusSummaryTest, RejectsSuccessfulStopWithUploadFailure)
{
    auto final_snapshot = snapshot();

    final_snapshot["process_state"] = "STOPPED";
    final_snapshot["active_connections"] = 0;
    final_snapshot["upload"] = finishedUpload(true);

    const auto result = decodeStatusSummary(JsonlMessage{
        JsonlMessageKind::Stopped,
        kRunId,
        3,
        1000,
        {
            {"exit_code", 0},
            {"snapshot", final_snapshot}
        }
    });

    expectErrorPath(
        result,
        "data.snapshot.upload.failed"
    );
}

TEST(StatusSummaryTest, ProcessFailureDoesNotRequireUploadFailure)
{
    auto final_snapshot = snapshot();

    final_snapshot["process_state"] = "FAILED";
    final_snapshot["active_connections"] = 0;
    final_snapshot["upload"] = finishedUpload(false);

    const auto result = decodeStatusSummary(JsonlMessage{
        JsonlMessageKind::Stopped,
        kRunId,
        3,
        1000,
        {
            {"exit_code", 1},
            {"snapshot", final_snapshot}
        }
    });

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_EQ(value.process_state, ProcessState::Failed);

    ASSERT_TRUE(value.upload.has_value());
    EXPECT_EQ(value.upload->state, UploadState::Stopped);
    EXPECT_FALSE(value.upload->failed);
}

TEST(StatusSummaryTest, DistinguishesUnknownRecordingFromDisabled)
{
    const auto unknown = decode(snapshot());

    ASSERT_TRUE(accepted(unknown));

    EXPECT_FALSE(
        std::get<StatusSummary>(unknown).recording.has_value()
    );

    auto data = snapshot();
    auto recording = activeRecording();

    recording["state"] = "DISABLED";
    recording["enabled"] = false;
    recording["operational"] = false;

    data["recording"] = recording;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.recording.has_value());
    EXPECT_EQ(value.recording->state, RecordingState::Disabled);
    EXPECT_FALSE(value.recording->enabled);
}

TEST(StatusSummaryTest, PreservesUnknownAndZeroRetainedFileCounts)
{
    auto data = snapshot();
    data["recording"] = activeRecording();

    const auto unknown = decode(data);

    ASSERT_TRUE(accepted(unknown));

    const auto& first = std::get<StatusSummary>(unknown);

    ASSERT_TRUE(first.recording.has_value());
    EXPECT_EQ(first.recording->state, RecordingState::Active);
    EXPECT_FALSE(
        first.recording->retained_closed_files.has_value()
    );

    data["recording"]["retained_closed_files"] = 0;

    const auto zero = decode(data);

    ASSERT_TRUE(accepted(zero));

    const auto& second = std::get<StatusSummary>(zero);

    ASSERT_TRUE(second.recording.has_value());
    ASSERT_TRUE(
        second.recording->retained_closed_files.has_value()
    );

    EXPECT_EQ(*second.recording->retained_closed_files, 0U);
}

TEST(StatusSummaryTest, PreservesPartialRecordingFailure)
{
    auto data = snapshot();
    auto recording = activeRecording();

    recording["state"] = "DEGRADED";
    recording["connections_seen"] = 1;

    recording["bytes_received"] = 10;
    recording["bytes_written"] = 4;
    recording["bytes_skipped"] = 6;

    recording["opened_files"] = 1;
    recording["active_sessions"] = 1;

    recording["session_errors"] = 1;
    recording["last_error_stage"] = "SESSION";
    recording["last_error_code"] = 5;
    recording["last_error_connection"] = 1;
    recording["last_session_operation"] = "WRITE";

    data["recording"] = recording;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_EQ(value.process_state, ProcessState::Running);

    ASSERT_TRUE(value.recording.has_value());

    EXPECT_EQ(value.recording->state, RecordingState::Degraded);
    EXPECT_TRUE(value.recording->operational);
    EXPECT_EQ(value.recording->bytes_written, 4U);
    EXPECT_EQ(value.recording->bytes_skipped, 6U);

    EXPECT_EQ(
        value.recording->last_error_stage,
        RecordingErrorStage::Session
    );

    EXPECT_EQ(
        value.recording->last_session_operation,
        RecordingOperation::Write
    );
}

TEST(StatusSummaryTest, PreservesRecordingErrorsAfterFinish)
{
    auto data = snapshot();
    auto recording = activeRecording();

    recording["state"] = "STOPPED";
    recording["operational"] = false;
    recording["finished"] = true;

    recording["retention_errors"] = 1;
    recording["last_error_stage"] = "RETENTION";
    recording["last_error_code"] = 13;

    data["recording"] = recording;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.recording.has_value());

    EXPECT_EQ(value.recording->state, RecordingState::Stopped);
    EXPECT_EQ(value.recording->retention_errors, 1U);
    EXPECT_EQ(
        value.recording->last_error_stage,
        RecordingErrorStage::Retention
    );
}

TEST(StatusSummaryTest, KeepsStartupFailureVisibleAfterNormalProcessStop)
{
    auto final_snapshot = snapshot();

    final_snapshot["process_state"] = "STOPPED";
    final_snapshot["active_connections"] = 0;
    final_snapshot["upload"] = finishedUpload();

    auto recording = activeRecording();

    recording["state"] = "FAILED";
    recording["operational"] = false;
    recording["finished"] = true;

    recording["startup_errors"] = 1;
    recording["last_error_stage"] = "STARTUP";
    recording["last_error_code"] = 13;

    final_snapshot["recording"] = recording;

    const auto result = decodeStatusSummary(JsonlMessage{
        JsonlMessageKind::Stopped,
        kRunId,
        3,
        1000,
        {
            {"exit_code", 0},
            {"snapshot", final_snapshot}
        }
    });

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_EQ(value.process_state, ProcessState::Stopped);

    ASSERT_TRUE(value.recording.has_value());
    EXPECT_EQ(value.recording->state, RecordingState::Failed);
    EXPECT_TRUE(value.recording->finished);
}

TEST(StatusSummaryTest, DisabledRecordingRemainsDisabledAfterFinish)
{
    auto data = snapshot();
    auto recording = activeRecording();

    recording["state"] = "DISABLED";
    recording["enabled"] = false;
    recording["operational"] = false;
    recording["finished"] = true;

    data["recording"] = recording;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.recording.has_value());
    EXPECT_EQ(value.recording->state, RecordingState::Disabled);
    EXPECT_TRUE(value.recording->finished);
}

TEST(StatusSummaryTest, RetainedFilesCanComeFromEarlierRuns)
{
    auto data = snapshot();

    data["recording"] = activeRecording();
    data["recording"]["retained_closed_files"] = 5;
    data["recording"]["files_deleted"] = 100;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.recording.has_value());
    EXPECT_EQ(value.recording->opened_files, 0U);
    EXPECT_EQ(value.recording->closed_files, 0U);
    EXPECT_EQ(value.recording->files_deleted, 100U);

    ASSERT_TRUE(
        value.recording->retained_closed_files.has_value()
    );

    EXPECT_EQ(*value.recording->retained_closed_files, 5U);
}

TEST(StatusSummaryTest, PreservesMaximumRecordingByteCounts)
{
    auto data = snapshot();

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    data["recording"] = activeRecording();
    data["recording"]["bytes_received"] = maximum;
    data["recording"]["bytes_written"] = maximum;
    data["recording"]["bytes_skipped"] = 0;
    data["recording"]["retained_closed_files"] = maximum;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.recording.has_value());

    EXPECT_EQ(value.recording->bytes_received, maximum);
    EXPECT_EQ(value.recording->bytes_written, maximum);

    ASSERT_TRUE(
        value.recording->retained_closed_files.has_value()
    );

    EXPECT_EQ(*value.recording->retained_closed_files, maximum);
}

TEST(StatusSummaryTest, RejectsInconsistentRecordingBytes)
{
    auto data = snapshot();

    data["recording"] = activeRecording();
    data["recording"]["bytes_received"] = 10;
    data["recording"]["bytes_written"] = 11;

    expectErrorPath(
        decode(data),
        "data.recording.bytes_written"
    );

    data["recording"]["bytes_written"] = 4;
    data["recording"]["bytes_skipped"] = 5;

    expectErrorPath(
        decode(data),
        "data.recording.bytes_skipped"
    );
}

TEST(StatusSummaryTest, RejectsInconsistentRecordingLifecycle)
{
    auto data = snapshot();

    data["recording"] = activeRecording();
    data["recording"]["closed_files"] = 1;

    expectErrorPath(
        decode(data),
        "data.recording.closed_files"
    );

    data["recording"] = activeRecording();
    data["recording"]["active_files"] = 1;

    expectErrorPath(
        decode(data),
        "data.recording.active_files"
    );

    data["recording"] = activeRecording();
    data["recording"]["enabled"] = false;

    expectErrorPath(
        decode(data),
        "data.recording.operational"
    );

    data["recording"] = activeRecording();
    data["recording"]["finished"] = true;

    expectErrorPath(
        decode(data),
        "data.recording.operational"
    );

    data["recording"]["operational"] = false;
    data["recording"]["active_sessions"] = 1;

    expectErrorPath(
        decode(data),
        "data.recording.active_sessions"
    );
}

TEST(StatusSummaryTest, RejectsInvalidRecordingStatesAndEnums)
{
    auto data = snapshot();

    data["recording"] = activeRecording();
    data["recording"]["state"] = "STOPPED";

    expectErrorPath(
        decode(data),
        "data.recording.state"
    );

    data["recording"] = activeRecording();
    data["recording"]["last_error_stage"] = "OTHER";

    expectErrorPath(
        decode(data),
        "data.recording.last_error_stage"
    );

    data["recording"] = activeRecording();
    data["recording"]["last_session_operation"] = "OTHER";

    expectErrorPath(
        decode(data),
        "data.recording.last_session_operation"
    );

    data["recording"] = activeRecording();
    data["recording"]["state"] = "UNKNOWN";

    expectErrorPath(
        decode(data),
        "data.recording"
    );
}

TEST(StatusSummaryTest, RejectsInvalidRecordingNumericTypes)
{
    for (const Json& invalid : {
            Json(-1),
            Json(true),
            Json(1.0),
            Json("1")
        }) {
        auto data = snapshot();

        data["recording"] = activeRecording();
        data["recording"]["bytes_received"] = invalid;

        expectErrorPath(
            decode(data),
            "data.recording.bytes_received"
        );
    }

    auto data = snapshot();

    data["recording"] = activeRecording();
    data["recording"]["last_error_code"] =
        std::numeric_limits<std::uint64_t>::max();

    expectErrorPath(
        decode(data),
        "data.recording.last_error_code"
    );
}

TEST(StatusSummaryTest, PreservesSignedRecordingErrorCode)
{
    auto data = snapshot();

    data["recording"] = activeRecording();
    data["recording"]["state"] = "DEGRADED";
    data["recording"]["event_errors"] = 1;
    data["recording"]["last_error_stage"] = "EVENT";
    data["recording"]["last_error_code"] = -1;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.recording.has_value());
    EXPECT_EQ(value.recording->last_error_code, -1);
}

TEST(StatusSummaryTest, DistinguishesUnknownTcpFromKnownEmpty)
{
    const auto unknown = decode(snapshot());

    ASSERT_TRUE(accepted(unknown));

    EXPECT_FALSE(
        std::get<StatusSummary>(unknown).tcp_activity.has_value()
    );

    auto data = snapshot();

    data["active_connections"] = 0;
    data["tcp_activity"] = emptyTcpActivity();

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.tcp_activity.has_value());
    EXPECT_EQ(value.tcp_activity->connected_count, 0U);
    EXPECT_TRUE(value.tcp_activity->connections.empty());
}

TEST(StatusSummaryTest, DecodesAllThreeTcpConnectionStates)
{
    auto data = snapshot();
    auto activity = emptyTcpActivity();

    activity["accepted_connections"] = 3;
    activity["connected_count"] = 3;

    activity["waiting_count"] = 1;
    activity["active_count"] = 1;
    activity["silent_count"] = 1;

    activity["bytes_received"] = 12;

    activity["connections"] = Json::array({
        tcpConnection(0, 1, "SILENT", 3000, 1000, 7),
        tcpConnection(1, 2, "ACTIVE", 100, 0, 5),
        tcpConnection(2, 3, "WAITING", 10, nullptr, 0)
    });

    data["active_connections"] = 3;
    data["tcp_activity"] = activity;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.tcp_activity.has_value());

    const auto& rows = value.tcp_activity->connections;

    ASSERT_EQ(rows.size(), 3U);

    EXPECT_EQ(rows[0].state, TcpConnectionState::Silent);
    EXPECT_EQ(rows[1].state, TcpConnectionState::Active);
    EXPECT_EQ(rows[2].state, TcpConnectionState::Waiting);

    ASSERT_TRUE(rows[1].last_data_age_ms.has_value());
    EXPECT_EQ(*rows[1].last_data_age_ms, 0U);

    EXPECT_FALSE(rows[2].last_data_age_ms.has_value());
}

TEST(StatusSummaryTest, AppliesSilentThresholdAtItsBoundary)
{
    auto data = snapshot();
    auto activity = emptyTcpActivity();

    activity["accepted_connections"] = 1;
    activity["connected_count"] = 1;
    activity["silent_count"] = 1;

    activity["connections"] = Json::array({
        tcpConnection(0, 1, "SILENT", 1000, nullptr, 0)
    });

    data["active_connections"] = 1;
    data["tcp_activity"] = activity;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.tcp_activity.has_value());

    EXPECT_EQ(
        value.tcp_activity->connections[0].state,
        TcpConnectionState::Silent
    );
}

TEST(StatusSummaryTest, SlotReuseKeepsNewConnectionIdentityAndFreshValues)
{
    auto data = snapshot();
    auto first_activity = emptyTcpActivity();

    first_activity["accepted_connections"] = 1;
    first_activity["connected_count"] = 1;
    first_activity["active_count"] = 1;
    first_activity["bytes_received"] = 10;

    first_activity["connections"] = Json::array({
        tcpConnection(0, 1, "ACTIVE", 20, 0, 10)
    });

    data["active_connections"] = 1;
    data["tcp_activity"] = first_activity;

    const auto first = decode(data);

    ASSERT_TRUE(accepted(first));

    auto next_activity = emptyTcpActivity();

    next_activity["accepted_connections"] = 2;
    next_activity["closed_connections"] = 1;
    next_activity["connected_count"] = 1;
    next_activity["waiting_count"] = 1;

    // 旧连接收到的字节仍在累计总量中。
    next_activity["bytes_received"] = 10;

    next_activity["connections"] = Json::array({
        tcpConnection(0, 2, "WAITING", 0, nullptr, 0)
    });

    data["tcp_activity"] = next_activity;

    const auto next = decode(data);

    ASSERT_TRUE(accepted(next));

    const auto& old_value = std::get<StatusSummary>(first);
    const auto& new_value = std::get<StatusSummary>(next);

    ASSERT_TRUE(old_value.tcp_activity.has_value());
    ASSERT_TRUE(new_value.tcp_activity.has_value());

    const auto& old_row = old_value.tcp_activity->connections[0];
    const auto& new_row = new_value.tcp_activity->connections[0];

    EXPECT_EQ(old_row.connection_id, 1U);

    EXPECT_EQ(new_row.slot, 0U);
    EXPECT_EQ(new_row.connection_id, 2U);
    EXPECT_EQ(new_row.bytes_received, 0U);
    EXPECT_EQ(new_row.connected_ms, 0U);
    EXPECT_FALSE(new_row.last_data_age_ms.has_value());
}

TEST(StatusSummaryTest, RejectsInvalidTcpRows)
{
    auto data = snapshot();
    data["active_connections"] = 2;

    auto activity = twoActiveTcpConnections();
    activity["connections"][0]["slot"] = 8;
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.connections[0].slot"
    );

    activity = twoActiveTcpConnections();
    activity["connections"][1]["slot"] = 0;
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.connections[1].slot"
    );

    activity = twoActiveTcpConnections();
    activity["connections"][1]["connection_id"] = 1;
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.connections[1].connection_id"
    );

    activity = twoActiveTcpConnections();
    activity["connections"][0]["connection_id"] = 0;
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.connections[0].connection_id"
    );

    activity = twoActiveTcpConnections();
    activity["connections"][0]["last_data_age_ms"] = nullptr;
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.connections[0].last_data_age_ms"
    );

    activity = twoActiveTcpConnections();
    activity["connections"][0]["last_data_age_ms"] = 201;
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.connections[0].last_data_age_ms"
    );

    activity = twoActiveTcpConnections();
    activity["connections"][0]["state"] = "SILENT";
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.connections[0].state"
    );
}

TEST(StatusSummaryTest, RejectsTcpCountAndDistributionMismatch)
{
    const std::vector<std::pair<const char*, Json>> cases{
        {"connected_count", 3},
        {"closed_connections", 3},
        {"client_errors", 1},
        {"waiting_count", 1},
        {"active_count", 0},
        {"silent_count", 1}
    };

    for (const auto& entry : cases) {
        auto data = snapshot();
        auto activity = twoActiveTcpConnections();

        activity[entry.first] = entry.second;

        data["active_connections"] = 2;
        data["tcp_activity"] = activity;

        expectErrorPath(
            decode(data),
            std::string("data.tcp_activity.") + entry.first
        );
    }
}

TEST(StatusSummaryTest, RejectsOverflowingConnectionByteSum)
{
    auto data = snapshot();
    auto activity = twoActiveTcpConnections();

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    activity["bytes_received"] = maximum;
    activity["connections"][0]["bytes_received"] = maximum;
    activity["connections"][1]["bytes_received"] = 1;

    data["active_connections"] = 2;
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.connections[1].bytes_received"
    );
}

TEST(StatusSummaryTest, AllowsHistoricalBytesOnlyAfterConnectionClosure)
{
    auto data = snapshot();
    auto activity = twoActiveTcpConnections();

    // 当前两条连接共收到 10 字节。
    activity["bytes_received"] = 11;

    data["active_connections"] = 2;
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.bytes_received"
    );

    // 已关闭的一条历史连接可以解释剩余的 1 字节。
    activity["accepted_connections"] = 3;
    activity["closed_connections"] = 1;

    data["tcp_activity"] = activity;

    EXPECT_TRUE(accepted(decode(data)));
}

TEST(StatusSummaryTest, EnforcesTcpCapacityAndThresholdRange)
{
    auto data = snapshot();
    auto activity = emptyTcpActivity();

    // 当前工程上限是 8，构造 9 条连接应被拒绝。
    for (std::uint64_t slot = 0; slot < 9; ++slot) {
        activity["connections"].push_back(
            tcpConnection(
                slot,
                slot + 1,
                "WAITING",
                0,
                nullptr,
                0
            )
        );
    }

    activity["accepted_connections"] = 9;
    activity["connected_count"] = 9;
    activity["waiting_count"] = 9;

    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.connections"
    );

    activity = emptyTcpActivity();
    activity["silent_after_ms"] = 0;
    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.silent_after_ms"
    );

    activity["silent_after_ms"] =
        static_cast<std::uint64_t>(
            std::numeric_limits<std::int32_t>::max()
        ) + 1;

    data["tcp_activity"] = activity;

    expectErrorPath(
        decode(data),
        "data.tcp_activity.silent_after_ms"
    );
}

TEST(StatusSummaryTest, ChecksTopLevelConnectionCountWhenKnown)
{
    auto data = snapshot();

    data["tcp_activity"] = twoActiveTcpConnections();
    data["active_connections"] = 1;

    expectErrorPath(
        decode(data),
        "data.active_connections"
    );

    data["active_connections"] = nullptr;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    EXPECT_FALSE(value.active_connections.has_value());

    ASSERT_TRUE(value.tcp_activity.has_value());
    EXPECT_EQ(value.tcp_activity->connected_count, 2U);
}

TEST(StatusSummaryTest, PreservesMaximumTcpCountersAndIdentity)
{
    auto data = snapshot();
    auto activity = emptyTcpActivity();

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    activity["accepted_connections"] = maximum;
    activity["closed_connections"] = maximum - 1;
    activity["rejected_connections"] = maximum;
    activity["client_errors"] = maximum - 1;
    activity["bytes_received"] = maximum;

    activity["connected_count"] = 1;
    activity["waiting_count"] = 1;

    activity["connections"] = Json::array({
        tcpConnection(0, maximum, "WAITING", 0, nullptr, 0)
    });

    data["active_connections"] = 1;
    data["tcp_activity"] = activity;

    const auto result = decode(data);

    ASSERT_TRUE(accepted(result));

    const auto& value = std::get<StatusSummary>(result);

    ASSERT_TRUE(value.tcp_activity.has_value());

    EXPECT_EQ(
        value.tcp_activity->accepted_connections,
        maximum
    );

    EXPECT_EQ(value.tcp_activity->bytes_received, maximum);
    EXPECT_EQ(
        value.tcp_activity->connections[0].connection_id,
        maximum
    );
}

TEST(StatusSummaryTest, RejectsUnknownTcpWithConnectionDetails)
{
    auto data = snapshot();

    data["tcp_activity"] = {
        {"state", "UNKNOWN"},
        {"connected_count", 0}
    };

    expectErrorPath(
        decode(data),
        "data.tcp_activity"
    );
}

} // namespace
