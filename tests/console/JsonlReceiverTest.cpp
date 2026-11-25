#include "protocol/JsonlReceiver.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {

using Json = nlohmann::json;

using edge::console::JsonlFrameKind;
using edge::console::JsonlLineFramer;
using edge::console::JsonlMessageKind;
using edge::console::JsonlParseCode;
using edge::console::JsonlParseError;
using edge::console::JsonlReceivedMessage;
using edge::console::JsonlReceiveFramingError;
using edge::console::JsonlReceiveResult;
using edge::console::JsonlReceiver;
using edge::console::ProcessState;
using edge::console::StatusDecodeError;

const std::string kRunA(32, 'a');
const std::string kRunB(32, 'b');

Json unknownSample()
{
    return {
        {"state", "UNKNOWN"},
        {"checked_at_ms", nullptr},
        {"value", nullptr},
        {"error", nullptr}
    };
}

// 覆盖当前状态摘要模型要求的字段。
Json statusData(const std::string& run_id)
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
        {"run_id", run_id},
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

Json envelope(
    const std::string& type,
    std::uint64_t sequence,
    Json data,
    const std::string& run_id = kRunA
)
{
    return {
        {"protocol_version", 1},
        {"type", type},
        {"run_id", run_id},
        {"sequence", sequence},
        {"emitted_at_ms", 1000},
        {"data", std::move(data)}
    };
}

Json readyMessage(const std::string& run_id = kRunA)
{
    return envelope(
        "ready",
        1,
        {
            {"agent_id", "demo-agent"},
            {"tcp_bind", "127.0.0.1"},
            {"tcp_port", 19090},
            {"events_jsonl", true},
            {"snapshot_version", 1},
            {"limits", {
                {"line_bytes", 65536},
                {"record_rate_per_second", 100}
            }}
        },
        run_id
    );
}

Json statusMessage(
    std::uint64_t sequence,
    const std::string& run_id = kRunA
)
{
    return envelope(
        "status",
        sequence,
        statusData(run_id),
        run_id
    );
}

Json stoppedMessage(
    std::uint64_t sequence,
    const std::string& run_id = kRunA
)
{
    auto snapshot = statusData(run_id);

    snapshot["process_state"] = "STOPPED";
    snapshot["active_connections"] = 0;

    snapshot["upload"] = {
        {"state", "STOPPED"},
        {"initialized", true},
        {"finished", true},
        {"failed", false},
        {"stop_requested", true},
        {"blocked", false},
        {"blocked_http_status", nullptr}
    };

    return envelope(
        "stopped",
        sequence,
        {
            {"exit_code", 0},
            {"snapshot", std::move(snapshot)}
        },
        run_id
    );
}

Json recordMessage(std::uint64_t sequence)
{
    // 此测试只检查 record 载荷交付和数值精度，
    // 不替代业务事件模型的完整字段校验。
    return envelope(
        "record",
        sequence,
        {
            {"event_id", "example-event"},
            {"disposition", "stored"},
            {"event", {
                {"probe", std::numeric_limits<std::uint64_t>::max()},
                {"value", 1.25}
            }}
        }
    );
}

bool accepted(const JsonlReceiveResult& result)
{
    return std::holds_alternative<JsonlReceivedMessage>(result);
}

class JsonlReceiverTest : public ::testing::Test {
protected:
    void feed(std::string_view bytes)
    {
        receiver_.feed(bytes, [this](JsonlReceiveResult result) {
            results_.push_back(std::move(result));
        });
    }

    void pushMessage(const Json& message)
    {
        const std::string wire = message.dump() + "\n";
        feed(wire);
    }

    void finishInput()
    {
        receiver_.finish([this](JsonlReceiveResult result) {
            results_.push_back(std::move(result));
        });
    }

    JsonlReceiver receiver_;
    std::vector<JsonlReceiveResult> results_;
};

TEST_F(JsonlReceiverTest, ReceivesFragmentedLifecycle)
{
    auto status = statusMessage(10);
    status["data"]["note"] = u8"设备状态";

    const std::string wire =
        readyMessage().dump() + "\n"
        + status.dump() + "\n"
        + recordMessage(20).dump() + "\n"
        + stoppedMessage(30).dump() + "\n";

    const std::string_view view(wire);

    // 逐字节输入，包含 UTF-8 字符内部的分片。
    for (std::size_t offset = 0; offset < view.size(); ++offset) {
        feed(view.substr(offset, 1));
    }

    finishInput();

    ASSERT_EQ(results_.size(), 4U);

    for (const auto& result : results_) {
        ASSERT_TRUE(accepted(result));
    }

    const auto& ready =
        std::get<JsonlReceivedMessage>(results_[0]);

    const auto& received_status =
        std::get<JsonlReceivedMessage>(results_[1]);

    const auto& record =
        std::get<JsonlReceivedMessage>(results_[2]);

    const auto& stopped =
        std::get<JsonlReceivedMessage>(results_[3]);

    EXPECT_EQ(ready.message.kind, JsonlMessageKind::Ready);
    EXPECT_FALSE(ready.status.has_value());

    ASSERT_TRUE(received_status.status.has_value());

    EXPECT_EQ(received_status.status->sequence, 10U);
    EXPECT_EQ(
        received_status.message.data["note"].get<std::string>(),
        u8"设备状态"
    );

    EXPECT_EQ(record.message.kind, JsonlMessageKind::Record);
    EXPECT_FALSE(record.status.has_value());

    ASSERT_TRUE(stopped.status.has_value());

    EXPECT_EQ(
        stopped.status->process_state,
        ProcessState::Stopped
    );

    ASSERT_TRUE(stopped.status->exit_code.has_value());
    EXPECT_EQ(*stopped.status->exit_code, 0);
}

TEST_F(JsonlReceiverTest, InvalidStatusDoesNotCommitSequence)
{
    pushMessage(readyMessage());

    auto invalid = statusMessage(100);
    invalid["data"].erase("uptime_ms");

    pushMessage(invalid);
    pushMessage(statusMessage(2));

    ASSERT_EQ(results_.size(), 3U);

    const auto* error =
        std::get_if<StatusDecodeError>(&results_[1]);

    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->path, "data.uptime_ms");

    ASSERT_TRUE(accepted(results_[2]));

    EXPECT_EQ(
        std::get<JsonlReceivedMessage>(results_[2]).message.sequence,
        2U
    );
}

TEST_F(JsonlReceiverTest, InvalidStoppedDoesNotCloseTheContext)
{
    pushMessage(readyMessage());

    auto invalid = stoppedMessage(100);

    // 磁盘样本未知时，告警不能被伪造为 false。
    invalid["data"]["snapshot"]["warnings"]["disk_available"] =
        false;

    pushMessage(invalid);

    pushMessage(statusMessage(2));
    pushMessage(stoppedMessage(3));

    ASSERT_EQ(results_.size(), 4U);

    EXPECT_TRUE(
        std::holds_alternative<StatusDecodeError>(results_[1])
    );

    ASSERT_TRUE(accepted(results_[2]));
    ASSERT_TRUE(accepted(results_[3]));

    EXPECT_EQ(
        std::get<JsonlReceivedMessage>(results_[2]).message.kind,
        JsonlMessageKind::Status
    );

    EXPECT_EQ(
        std::get<JsonlReceivedMessage>(results_[3]).message.kind,
        JsonlMessageKind::Stopped
    );

    // 合法 stopped 已提交，此后才拒绝新的状态消息。
    pushMessage(statusMessage(4));

    ASSERT_EQ(results_.size(), 5U);

    const auto* error =
        std::get_if<JsonlParseError>(&results_[4]);

    ASSERT_NE(error, nullptr);

    EXPECT_EQ(
        error->code,
        JsonlParseCode::UnexpectedMessage
    );
}

TEST_F(JsonlReceiverTest, InvalidReadyDoesNotLatchRunIdentity)
{
    auto invalid = readyMessage(kRunA);
    invalid["protocol_version"] = 2;

    pushMessage(invalid);
    pushMessage(readyMessage(kRunB));
    pushMessage(statusMessage(2, kRunB));

    ASSERT_EQ(results_.size(), 3U);

    EXPECT_TRUE(
        std::holds_alternative<JsonlParseError>(results_[0])
    );

    ASSERT_TRUE(accepted(results_[1]));
    ASSERT_TRUE(accepted(results_[2]));

    EXPECT_EQ(
        std::get<JsonlReceivedMessage>(results_[2]).message.run_id,
        kRunB
    );
}

TEST_F(JsonlReceiverTest, WrongRunDoesNotAdvanceTheCurrentRun)
{
    pushMessage(readyMessage());

    pushMessage(statusMessage(100, kRunB));
    pushMessage(statusMessage(2));

    ASSERT_EQ(results_.size(), 3U);

    const auto* error =
        std::get_if<JsonlParseError>(&results_[1]);

    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, JsonlParseCode::RunMismatch);

    ASSERT_TRUE(accepted(results_[2]));

    EXPECT_EQ(
        std::get<JsonlReceivedMessage>(results_[2]).message.sequence,
        2U
    );
}

TEST_F(JsonlReceiverTest, RecoversAfterOversizedLine)
{
    std::string wire(
        JsonlLineFramer::kMaxLineBytes,
        'x'
    );

    wire += "\n";
    wire += readyMessage().dump();
    wire += "\n";

    feed(wire);

    ASSERT_EQ(results_.size(), 2U);

    const auto* error =
        std::get_if<JsonlReceiveFramingError>(&results_[0]);

    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->kind, JsonlFrameKind::LineTooLong);

    EXPECT_TRUE(accepted(results_[1]));
}

TEST_F(JsonlReceiverTest, ReportsIncompleteTailOnlyOnce)
{
    pushMessage(readyMessage());

    feed("{\"protocol_version\":");

    finishInput();
    finishInput();

    ASSERT_EQ(results_.size(), 2U);

    const auto* error =
        std::get_if<JsonlReceiveFramingError>(&results_[1]);

    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->kind, JsonlFrameKind::IncompleteTail);
}

TEST_F(JsonlReceiverTest, EndOfInputDoesNotInventStopped)
{
    pushMessage(readyMessage());
    pushMessage(statusMessage(2));

    finishInput();

    ASSERT_EQ(results_.size(), 2U);
    ASSERT_TRUE(accepted(results_[1]));

    const auto& last =
        std::get<JsonlReceivedMessage>(results_[1]);

    EXPECT_EQ(last.message.kind, JsonlMessageKind::Status);

    ASSERT_TRUE(last.status.has_value());
    EXPECT_EQ(last.status->process_state, ProcessState::Running);

    EXPECT_THROW(
        feed(readyMessage().dump() + "\n"),
        std::logic_error
    );
}

TEST_F(JsonlReceiverTest, ResetClearsPartialInputAndOldIdentity)
{
    pushMessage(readyMessage(kRunA));

    feed("{\"type\":\"status\"");

    receiver_.reset();

    pushMessage(readyMessage(kRunB));
    pushMessage(statusMessage(2, kRunB));

    ASSERT_EQ(results_.size(), 3U);

    for (const auto& result : results_) {
        ASSERT_TRUE(accepted(result));
    }

    EXPECT_EQ(
        std::get<JsonlReceivedMessage>(results_[0]).message.run_id,
        kRunA
    );

    EXPECT_EQ(
        std::get<JsonlReceivedMessage>(results_[2]).message.run_id,
        kRunB
    );
}

TEST_F(JsonlReceiverTest, PreservesRecordIntegerPrecision)
{
    pushMessage(readyMessage());
    pushMessage(recordMessage(4));

    ASSERT_EQ(results_.size(), 2U);
    ASSERT_TRUE(accepted(results_[1]));

    const auto& received =
        std::get<JsonlReceivedMessage>(results_[1]);

    EXPECT_FALSE(received.status.has_value());

    const auto& probe = received.message.data["event"]["probe"];

    EXPECT_TRUE(probe.is_number_unsigned());

    EXPECT_EQ(
        probe.get<std::uint64_t>(),
        std::numeric_limits<std::uint64_t>::max()
    );
}

TEST_F(JsonlReceiverTest, RejectsReentrantResetWithoutLosingIdentity)
{
    const auto wire = readyMessage().dump() + "\n";

    receiver_.feed(wire, [this](JsonlReceiveResult result) {
        ASSERT_TRUE(accepted(result));

        EXPECT_THROW(
            receiver_.reset(),
            std::logic_error
        );
    });

    pushMessage(statusMessage(2));

    ASSERT_EQ(results_.size(), 1U);
    ASSERT_TRUE(accepted(results_[0]));

    EXPECT_EQ(
        std::get<JsonlReceivedMessage>(results_[0]).message.kind,
        JsonlMessageKind::Status
    );
}

TEST_F(JsonlReceiverTest, RejectsEmptyHandlerBeforeConsumingInput)
{
    const auto wire = readyMessage().dump() + "\n";

    EXPECT_THROW(
        receiver_.feed(wire, JsonlReceiver::Sink{}),
        std::invalid_argument
    );

    EXPECT_THROW(
        receiver_.finish(JsonlReceiver::Sink{}),
        std::invalid_argument
    );

    pushMessage(readyMessage());

    ASSERT_EQ(results_.size(), 1U);
    EXPECT_TRUE(accepted(results_[0]));
}

} // namespace