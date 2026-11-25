#include "edge/status/JsonlProtocol.h"
#include "edge/domain/EventId.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

namespace {

using Json = nlohmann::json;

edge::JsonlMessageMeta metadata(std::uint64_t sequence)
{
    return {
        "0123456789abcdef0123456789abcdef",
        sequence,
        1700000000200LL
    };
}

edge::JsonlReadyData readyData()
{
    return {
        "demo-agent",
        "127.0.0.1",
        19090,
        false
    };
}

edge::StatusSnapshot statusSnapshot()
{
    edge::StatusSnapshot snapshot;

    snapshot.agent_id = "demo-agent";
    snapshot.checked_at_ms = 1700000000100LL;
    snapshot.uptime_ms = 100;
    snapshot.process_state = edge::StatusProcessState::Running;
    snapshot.active_connections = 0;

    edge::StatusUploadObservation upload;
    upload.initialized = true;
    snapshot.upload = upload;

    return snapshot;
}

edge::JsonlRecordData recordData()
{
    edge::JsonlRecordData record;

    record.event.device_id = "sensor-01";
    record.event.session_id =
        "550e8400-e29b-41d4-a716-446655440000";
    record.event.sequence = 1;
    record.event.observed_at_ms = 1700000000000LL;
    record.event.metric = "temperature";
    record.event.value = 21.5;
    record.event.unit = "degC";

    record.disposition = edge::JsonlRecordDisposition::Stored;

    return record;
}

std::string requireLine(edge::JsonlEncodeResult result)
{
    if (auto* line = std::get_if<std::string>(&result)) {
        return std::move(*line);
    }

    throw std::runtime_error(
        std::get<edge::JsonlEncodeError>(result).message
    );
}

Json parseLine(edge::JsonlEncodeResult result)
{
    return Json::parse(requireLine(std::move(result)));
}

void expectError(
    const edge::JsonlEncodeResult& result,
    edge::JsonlEncodeCode expected
)
{
    const auto* error =
        std::get_if<edge::JsonlEncodeError>(&result);

    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, expected);
    EXPECT_FALSE(error->message.empty());
}

TEST(JsonlProtocolTest, ReadyHasVersionedEnvelope)
{
    const auto line = requireLine(
        edge::encodeJsonlReady(metadata(1), readyData())
    );

    ASSERT_FALSE(line.empty());
    EXPECT_EQ(line.back(), '\n');
    EXPECT_EQ(std::count(line.begin(), line.end(), '\n'), 1);

    const auto json = Json::parse(line);

    EXPECT_EQ(json["protocol_version"], 1);
    EXPECT_EQ(json["type"], "ready");
    EXPECT_EQ(
        json["run_id"],
        "0123456789abcdef0123456789abcdef"
    );
    EXPECT_EQ(json["sequence"], 1);
    EXPECT_EQ(json["emitted_at_ms"], 1700000000200LL);
    EXPECT_EQ(json["data"]["agent_id"], "demo-agent");
    EXPECT_EQ(json["data"]["tcp_port"], 19090);
}

TEST(JsonlProtocolTest, ReadyCarriesEventsSwitchAndLimits)
{
    auto ready = readyData();
    ready.events_jsonl = true;

    const auto json = parseLine(
        edge::encodeJsonlReady(metadata(1), ready)
    );

    EXPECT_EQ(json["data"]["events_jsonl"], true);
    EXPECT_EQ(json["data"]["snapshot_version"], 1);

    EXPECT_EQ(
        json["data"]["limits"]["line_bytes"],
        65536
    );

    EXPECT_EQ(
        json["data"]["limits"]["record_rate_per_second"],
        100
    );
}

TEST(JsonlProtocolTest, StatusContainsSnapshotAndUnknownValues)
{
    auto snapshot = statusSnapshot();
    snapshot.counters.stored_events = 3;

    const auto json = parseLine(
        edge::encodeJsonlStatus(metadata(2), snapshot, {})
    );

    EXPECT_EQ(json["type"], "status");
    EXPECT_EQ(json["data"]["process_state"], "RUNNING");
    EXPECT_EQ(json["data"]["counters"]["stored_events"], 3);

    EXPECT_EQ(json["data"]["outbox"]["state"], "UNKNOWN");
    EXPECT_TRUE(json["data"]["outbox"]["value"].is_null());

    EXPECT_TRUE(
        json["data"]["warnings"]["unacked_records"].is_null()
    );

    EXPECT_EQ(
        json["data"]["monitoring"]["output_errors"],
        0
    );
}

TEST(JsonlProtocolTest, RecordPreservesEventAndIdentity)
{
    const auto record = recordData();

    const auto json = parseLine(
        edge::encodeJsonlRecord(metadata(2), record)
    );

    EXPECT_EQ(json["type"], "record");

    EXPECT_EQ(
        json["data"]["event_id"],
        edge::makeEventId(record.event)
    );

    EXPECT_EQ(json["data"]["disposition"], "stored");
    EXPECT_EQ(json["data"]["event"]["device_id"], "sensor-01");
    EXPECT_EQ(json["data"]["event"]["sequence"], 1);
    EXPECT_EQ(json["data"]["event"]["value"], 21.5);
}

TEST(JsonlProtocolTest, RecordDispositionNamesAreStable)
{
    using Disposition = edge::JsonlRecordDisposition;

    const std::array<std::pair<Disposition, const char*>, 6> cases{{
        {Disposition::Stored, "stored"},
        {Disposition::Duplicate, "duplicate"},
        {Disposition::IdentityConflict, "identity_conflict"},
        {Disposition::CapacityRejected, "capacity_rejected"},
        {Disposition::StorageSkipped, "storage_skipped"},
        {Disposition::StorageError, "storage_error"}
    }};

    for (const auto& item : cases) {
        auto record = recordData();
        record.disposition = item.first;

        const auto json = parseLine(
            edge::encodeJsonlRecord(metadata(2), record)
        );

        EXPECT_EQ(
            json["data"]["disposition"],
            item.second
        );
    }
}

TEST(JsonlProtocolTest, RejectsNonFiniteRecordValue)
{
    auto record = recordData();
    record.event.value = std::numeric_limits<double>::infinity();

    expectError(
        edge::encodeJsonlRecord(metadata(2), record),
        edge::JsonlEncodeCode::InvalidPayload
    );
}

TEST(JsonlProtocolTest, RejectsUnknownRecordDisposition)
{
    auto record = recordData();
    record.disposition = edge::JsonlRecordDisposition::Unknown;

    expectError(
        edge::encodeJsonlRecord(metadata(2), record),
        edge::JsonlEncodeCode::InvalidPayload
    );

    record.disposition =
        static_cast<edge::JsonlRecordDisposition>(999);

    expectError(
        edge::encodeJsonlRecord(metadata(2), record),
        edge::JsonlEncodeCode::InvalidPayload
    );
}

TEST(JsonlProtocolTest, EncodesSuccessfulStoppedSnapshot)
{
    auto snapshot = statusSnapshot();

    snapshot.process_state = edge::StatusProcessState::Stopped;
    snapshot.upload->finished = true;
    snapshot.upload->stop_requested = true;
    snapshot.counters.upload_acked = 2;

    const auto json = parseLine(
        edge::encodeJsonlStopped(
            metadata(3),
            0,
            snapshot,
            {}
        )
    );

    EXPECT_EQ(json["type"], "stopped");
    EXPECT_EQ(json["data"]["exit_code"], 0);

    EXPECT_EQ(
        json["data"]["snapshot"]["process_state"],
        "STOPPED"
    );

    EXPECT_EQ(
        json["data"]["snapshot"]["counters"]["upload_acked"],
        2
    );
}

TEST(JsonlProtocolTest, EncodesFailedStoppedSnapshot)
{
    auto snapshot = statusSnapshot();

    snapshot.process_state = edge::StatusProcessState::Failed;
    snapshot.upload->finished = true;
    snapshot.upload->failed = true;

    const auto json = parseLine(
        edge::encodeJsonlStopped(
            metadata(3),
            1,
            snapshot,
            {}
        )
    );

    EXPECT_EQ(json["data"]["exit_code"], 1);
    EXPECT_EQ(
        json["data"]["snapshot"]["process_state"],
        "FAILED"
    );
}

TEST(JsonlProtocolTest, RejectsIncompleteOrConflictingFinalState)
{
    auto snapshot = statusSnapshot();

    expectError(
        edge::encodeJsonlStopped(metadata(3), 0, snapshot, {}),
        edge::JsonlEncodeCode::InvalidPayload
    );

    snapshot.process_state = edge::StatusProcessState::Stopped;

    // 线程尚未结束。
    expectError(
        edge::encodeJsonlStopped(metadata(3), 0, snapshot, {}),
        edge::JsonlEncodeCode::InvalidPayload
    );

    snapshot.upload->finished = true;
    snapshot.active_connections = 1;

    expectError(
        edge::encodeJsonlStopped(metadata(3), 0, snapshot, {}),
        edge::JsonlEncodeCode::InvalidPayload
    );

    snapshot.active_connections = 0;

    // 退出码 1 与 STOPPED 不匹配。
    expectError(
        edge::encodeJsonlStopped(metadata(3), 1, snapshot, {}),
        edge::JsonlEncodeCode::InvalidPayload
    );

    snapshot.upload->failed = true;

    expectError(
        edge::encodeJsonlStopped(metadata(3), 0, snapshot, {}),
        edge::JsonlEncodeCode::InvalidPayload
    );
}

TEST(JsonlProtocolTest, RejectsMalformedMetadata)
{
    auto meta = metadata(1);
    meta.run_id = "short";

    expectError(
        edge::encodeJsonlReady(meta, readyData()),
        edge::JsonlEncodeCode::InvalidMetadata
    );

    meta = metadata(1);
    meta.run_id[0] = 'A';

    expectError(
        edge::encodeJsonlReady(meta, readyData()),
        edge::JsonlEncodeCode::InvalidMetadata
    );

    meta = metadata(1);
    meta.sequence = 0;

    expectError(
        edge::encodeJsonlReady(meta, readyData()),
        edge::JsonlEncodeCode::InvalidMetadata
    );

    meta = metadata(1);
    meta.emitted_at_ms = 0;

    expectError(
        edge::encodeJsonlReady(meta, readyData()),
        edge::JsonlEncodeCode::InvalidMetadata
    );
}

TEST(JsonlProtocolTest, ReadyMustBeFirstAndOtherMessagesFollow)
{
    expectError(
        edge::encodeJsonlReady(metadata(2), readyData()),
        edge::JsonlEncodeCode::InvalidMetadata
    );

    expectError(
        edge::encodeJsonlStatus(
            metadata(1),
            statusSnapshot(),
            {}
        ),
        edge::JsonlEncodeCode::InvalidMetadata
    );

    expectError(
        edge::encodeJsonlRecord(metadata(1), recordData()),
        edge::JsonlEncodeCode::InvalidMetadata
    );
}

TEST(JsonlProtocolTest, UnicodeAndEscapedTextStayOnOneLine)
{
    auto snapshot = statusSnapshot();
    snapshot.agent_id = "设备\"A\n第二行";

    const auto line = requireLine(
        edge::encodeJsonlStatus(metadata(2), snapshot, {})
    );

    EXPECT_EQ(std::count(line.begin(), line.end(), '\n'), 1);
    EXPECT_EQ(line.back(), '\n');

    const auto json = Json::parse(line);

    EXPECT_EQ(
        json["data"]["agent_id"].get<std::string>(),
        snapshot.agent_id
    );
}

TEST(JsonlProtocolTest, ReportsInvalidUtf8)
{
    auto ready = readyData();
    ready.agent_id = std::string(
        1,
        static_cast<char>(0xff)
    );

    expectError(
        edge::encodeJsonlReady(metadata(1), ready),
        edge::JsonlEncodeCode::InvalidTextEncoding
    );
}

TEST(JsonlProtocolTest, LimitIncludesNewlineAndRejectsOversize)
{
    auto snapshot = statusSnapshot();

    snapshot.outbox.state = edge::StatusSampleState::Error;
    snapshot.outbox.checked_at_ms = snapshot.checked_at_ms;
    snapshot.outbox.error = "x";

    const auto initial_line = requireLine(
        edge::encodeJsonlStatus(metadata(2), snapshot, {})
    );

    ASSERT_LT(
        initial_line.size(),
        edge::kMaxJsonlLineBytes
    );

    // ASCII x 不需要额外转义，增加多少字符就增加多少字节。
    snapshot.outbox.error.append(
        edge::kMaxJsonlLineBytes - initial_line.size(),
        'x'
    );

    const auto boundary_line = requireLine(
        edge::encodeJsonlStatus(metadata(2), snapshot, {})
    );

    EXPECT_EQ(
        boundary_line.size(),
        edge::kMaxJsonlLineBytes
    );
    EXPECT_EQ(boundary_line.back(), '\n');

    snapshot.outbox.error.push_back('x');

    expectError(
        edge::encodeJsonlStatus(metadata(2), snapshot, {}),
        edge::JsonlEncodeCode::LineTooLarge
    );
}

TEST(JsonlProtocolTest, PreservesLargeUnsignedSequence)
{
    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    const auto json = parseLine(
        edge::encodeJsonlStatus(
            metadata(maximum),
            statusSnapshot(),
            {}
        )
    );

    EXPECT_TRUE(json["sequence"].is_number_unsigned());

    EXPECT_EQ(
        json["sequence"].get<std::uint64_t>(),
        maximum
    );
}

TEST(JsonlProtocolTest, ReportsInvalidSnapshotAsPayloadError)
{
    auto snapshot = statusSnapshot();
    snapshot.checked_at_ms = 0;

    expectError(
        edge::encodeJsonlStatus(metadata(2), snapshot, {}),
        edge::JsonlEncodeCode::InvalidPayload
    );
}

} // namespace