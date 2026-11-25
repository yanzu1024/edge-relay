#include "edge/status/JsonlProtocol.h"
#include "edge/status/StatusSnapshot.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <cerrno>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <variant>

namespace {

using Json = nlohmann::json;

constexpr char kRun[] =
    "0123456789abcdef0123456789abcdef";

edge::StatusSnapshot makeSnapshot()
{
    edge::StatusSnapshot snapshot;

    snapshot.agent_id = "recording-test";
    snapshot.run_id = kRun;
    snapshot.checked_at_ms = 1700000000000LL;

    snapshot.process_state =
        edge::StatusProcessState::Running;

    snapshot.active_connections = 0;

    return snapshot;
}

Json encode(const edge::StatusSnapshot& snapshot)
{
    return Json::parse(
        edge::serializeStatusSnapshot(
            snapshot,
            edge::StatusConfig{}
        )
    );
}

TEST(RecordingStatusTest, MissingObservationIsUnknown)
{
    const auto json = encode(makeSnapshot());

    EXPECT_EQ(
        json.at("recording").at("state"),
        "UNKNOWN"
    );
}

TEST(RecordingStatusTest, DisabledIsExplicit)
{
    auto snapshot = makeSnapshot();

    edge::RawRecorderStats recording;
    recording.finished = true;

    snapshot.recording = recording;

    const auto json = encode(snapshot);
    const auto& value = json.at("recording");

    EXPECT_EQ(value.at("state"), "DISABLED");
    EXPECT_EQ(value.at("enabled"), false);
    EXPECT_EQ(value.at("finished"), true);
    EXPECT_EQ(value.at("bytes_received"), 0U);
}

TEST(RecordingStatusTest, PreservesUnsignedByteCounts)
{
    auto snapshot = makeSnapshot();

    edge::RawRecorderStats recording;

    recording.enabled = true;
    recording.operational = true;

    recording.bytes_received =
        std::numeric_limits<std::uint64_t>::max();

    recording.bytes_written = recording.bytes_received;

    recording.opened_files = 1;
    recording.active_sessions = 1;
    recording.active_files = 1;

    snapshot.recording = recording;

    const auto json = encode(snapshot);
    const auto& value = json.at("recording");

    EXPECT_EQ(value.at("state"), "ACTIVE");

    EXPECT_EQ(
        value.at("bytes_received").get<std::uint64_t>(),
        std::numeric_limits<std::uint64_t>::max()
    );

    EXPECT_EQ(
        value.at("bytes_written").get<std::uint64_t>(),
        std::numeric_limits<std::uint64_t>::max()
    );

    EXPECT_EQ(value.at("bytes_skipped"), 0U);
}

TEST(RecordingStatusTest, StartupFailureIsSeparateFromBusiness)
{
    auto snapshot = makeSnapshot();

    edge::RawRecorderStats recording;

    recording.enabled = true;
    recording.startup_errors = 1;

    recording.bytes_received = 12;
    recording.bytes_skipped = 12;

    recording.last_error_stage =
        edge::RawRecorderErrorStage::Startup;

    recording.last_error_code = EACCES;

    snapshot.recording = recording;

    const auto json = encode(snapshot);

    EXPECT_EQ(
        json.at("recording").at("state"),
        "FAILED"
    );

    EXPECT_EQ(
        json.at("recording").at("last_error_stage"),
        "STARTUP"
    );

    EXPECT_EQ(
        json.at("process_state"),
        "RUNNING"
    );

    EXPECT_EQ(
        json.at("counters").at("storage_errors"),
        0U
    );

    EXPECT_EQ(
        json.at("counters").at("upload_errors"),
        0U
    );
}

TEST(RecordingStatusTest, CleanupErrorDegradesAvailableRecorder)
{
    auto snapshot = makeSnapshot();

    edge::RawRecorderStats recording;

    recording.enabled = true;
    recording.operational = true;
    recording.retention_errors = 1;

    recording.last_error_stage =
        edge::RawRecorderErrorStage::Retention;

    recording.last_error_code = EACCES;

    snapshot.recording = recording;

    const auto json = encode(snapshot);
    const auto& value = json.at("recording");

    EXPECT_EQ(value.at("state"), "DEGRADED");
    EXPECT_EQ(value.at("operational"), true);

    EXPECT_EQ(value.at("session_errors"), 0U);
    EXPECT_EQ(value.at("retention_errors"), 1U);

    EXPECT_TRUE(
        value.at("retained_closed_files").is_null()
    );
}

TEST(RecordingStatusTest, UnknownRetentionDiffersFromZero)
{
    auto snapshot = makeSnapshot();

    edge::RawRecorderStats recording;

    recording.enabled = true;
    recording.operational = true;

    snapshot.recording = recording;

    auto json = encode(snapshot);

    EXPECT_TRUE(
        json.at("recording")
            .at("retained_closed_files")
            .is_null()
    );

    recording.retained_closed_files = 0;
    snapshot.recording = recording;

    json = encode(snapshot);

    EXPECT_EQ(
        json.at("recording").at("retained_closed_files"),
        0U
    );
}

TEST(RecordingStatusTest, StatusAndStoppedReuseSameRecordingJson)
{
    auto snapshot = makeSnapshot();

    snapshot.process_state =
        edge::StatusProcessState::Stopped;

    snapshot.upload = edge::StatusUploadObservation{
        true,
        true,
        false,
        true,
        false,
        0
    };

    edge::RawRecorderStats recording;

    recording.enabled = true;
    recording.finished = true;

    recording.bytes_received = 5;
    recording.bytes_written = 5;

    recording.opened_files = 1;
    recording.closed_files = 1;

    recording.retained_closed_files = 1;

    snapshot.recording = recording;

    const auto expected = encode(snapshot).at("recording");

    EXPECT_EQ(expected.at("state"), "STOPPED");

    const edge::StatusConfig config;

    const auto status_line = edge::encodeJsonlStatus(
        {kRun, 2, snapshot.checked_at_ms},
        snapshot,
        config
    );

    ASSERT_TRUE(
        std::holds_alternative<std::string>(status_line)
    );

    const auto status = Json::parse(
        std::get<std::string>(status_line)
    );

    EXPECT_EQ(
        status.at("data").at("recording"),
        expected
    );

    const auto stopped_line = edge::encodeJsonlStopped(
        {kRun, 3, snapshot.checked_at_ms},
        0,
        snapshot,
        config
    );

    ASSERT_TRUE(
        std::holds_alternative<std::string>(stopped_line)
    );

    const auto stopped = Json::parse(
        std::get<std::string>(stopped_line)
    );

    EXPECT_EQ(
        stopped.at("data")
            .at("snapshot")
            .at("recording"),
        expected
    );
}

TEST(RecordingStatusTest, RejectsInconsistentByteCounts)
{
    auto snapshot = makeSnapshot();

    edge::RawRecorderStats recording;

    recording.enabled = true;
    recording.operational = true;

    recording.bytes_received = 3;
    recording.bytes_written = 4;

    snapshot.recording = recording;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(
            snapshot,
            edge::StatusConfig{}
        ),
        std::invalid_argument
    );

    recording.bytes_written = 2;
    recording.bytes_skipped = 0;

    snapshot.recording = recording;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(
            snapshot,
            edge::StatusConfig{}
        ),
        std::invalid_argument
    );
}

} // namespace