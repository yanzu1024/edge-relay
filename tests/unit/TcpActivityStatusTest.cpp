#include "edge/status/JsonlProtocol.h"
#include "edge/status/StatusSnapshot.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <variant>

namespace {

using Json = nlohmann::json;
using Tracker = edge::TcpActivityTracker;

constexpr char kRun[] =
    "0123456789abcdef0123456789abcdef";

Tracker::TimePoint at(std::int64_t milliseconds)
{
    return Tracker::TimePoint{} +
        std::chrono::milliseconds{milliseconds};
}

edge::TcpConfig config()
{
    edge::TcpConfig value;

    value.max_connections = 2;
    value.silent_after_ms = 100;

    return value;
}

edge::StatusSnapshot baseSnapshot()
{
    edge::StatusSnapshot snapshot;

    snapshot.agent_id = "tcp-activity-test";
    snapshot.run_id = kRun;

    snapshot.checked_at_ms = 1700000000000LL;
    snapshot.process_state = edge::StatusProcessState::Running;

    snapshot.active_connections = 0;

    return snapshot;
}

edge::TcpActivitySnapshot activeObservation()
{
    Tracker tracker{config()};

    edge::TcpPollBatch batch;

    batch.events = {
        {edge::TcpIoKind::Accepted, 0, {}, 0},
        {edge::TcpIoKind::Data, 0, {'a'}, 0}
    };

    tracker.consume(batch, at(0));

    return tracker.snapshot(at(0));
}

edge::StatusSnapshot withActivity(
    const edge::TcpActivitySnapshot& activity
)
{
    auto snapshot = baseSnapshot();

    snapshot.active_connections = activity.connected_count;
    snapshot.tcp_activity = activity;

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

TEST(TcpActivityStatusTest, MissingObservationIsUnknown)
{
    const auto json = encode(baseSnapshot());

    EXPECT_EQ(
        json.at("tcp_activity").at("state"),
        "UNKNOWN"
    );
}

TEST(TcpActivityStatusTest, KnownEmptyHasValidEmptyList)
{
    Tracker tracker{config()};

    const auto json = encode(
        withActivity(tracker.snapshot(at(0)))
    );

    const auto& activity = json.at("tcp_activity");

    EXPECT_EQ(activity.at("state"), "VALID");
    EXPECT_EQ(activity.at("connected_count"), 0U);

    EXPECT_TRUE(activity.at("connections").is_array());
    EXPECT_TRUE(activity.at("connections").empty());
}

TEST(TcpActivityStatusTest, NoDataDiffersFromJustReceived)
{
    Tracker tracker{config()};

    edge::TcpPollBatch batch;

    batch.events = {
        {edge::TcpIoKind::Accepted, 0, {}, 0},
        {edge::TcpIoKind::Accepted, 1, {}, 0},
        {edge::TcpIoKind::Data, 1, {'x'}, 0}
    };

    tracker.consume(batch, at(0));

    const auto json = encode(
        withActivity(tracker.snapshot(at(0)))
    );

    const auto& rows =
        json.at("tcp_activity").at("connections");

    ASSERT_EQ(rows.size(), 2U);

    EXPECT_EQ(rows[0].at("state"), "WAITING");
    EXPECT_TRUE(rows[0].at("last_data_age_ms").is_null());

    EXPECT_EQ(rows[1].at("state"), "ACTIVE");
    EXPECT_EQ(rows[1].at("last_data_age_ms"), 0U);
}

TEST(TcpActivityStatusTest, SilentMayHaveNeverReceivedData)
{
    Tracker tracker{config()};

    edge::TcpPollBatch batch;

    batch.events = {
        {edge::TcpIoKind::Accepted, 0, {}, 0}
    };

    tracker.consume(batch, at(0));

    const auto json = encode(
        withActivity(tracker.snapshot(at(100)))
    );

    const auto& row =
        json.at("tcp_activity").at("connections").at(0);

    EXPECT_EQ(row.at("state"), "SILENT");
    EXPECT_EQ(row.at("connected_ms"), 100U);
    EXPECT_EQ(row.at("bytes_received"), 0U);

    EXPECT_TRUE(row.at("last_data_age_ms").is_null());
}

TEST(TcpActivityStatusTest, PreservesUnsignedCounters)
{
    auto activity = activeObservation();

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    activity.accepted_connections = maximum;
    activity.closed_connections = maximum - 1;

    activity.bytes_received = maximum;
    activity.connections[0].bytes_received = maximum;

    const auto json = encode(withActivity(activity));
    const auto& value = json.at("tcp_activity");

    EXPECT_EQ(
        value.at("accepted_connections").get<std::uint64_t>(),
        maximum
    );

    EXPECT_EQ(
        value.at("bytes_received").get<std::uint64_t>(),
        maximum
    );
}

TEST(TcpActivityStatusTest, RejectsInconsistentCounts)
{
    auto activity = activeObservation();

    activity.connected_count = 2;

    EXPECT_THROW(
        encode(withActivity(activity)),
        std::invalid_argument
    );

    activity = activeObservation();

    auto snapshot = withActivity(activity);
    snapshot.active_connections = 0;

    EXPECT_THROW(
        encode(snapshot),
        std::invalid_argument
    );

    activity.closed_connections = 2;

    EXPECT_THROW(
        encode(withActivity(activity)),
        std::invalid_argument
    );
}

TEST(TcpActivityStatusTest, RejectsInconsistentRows)
{
    auto activity = activeObservation();

    activity.connections[0].state =
        edge::TcpConnectionState::Silent;

    EXPECT_THROW(
        encode(withActivity(activity)),
        std::invalid_argument
    );

    activity = activeObservation();

    activity.connections[0].last_data_age_ms = 1;

    EXPECT_THROW(
        encode(withActivity(activity)),
        std::invalid_argument
    );

    activity = activeObservation();

    activity.connections[0].bytes_received = 0;

    EXPECT_THROW(
        encode(withActivity(activity)),
        std::invalid_argument
    );
}

TEST(TcpActivityStatusTest, JsonlReusesFinalActivitySnapshot)
{
    Tracker tracker{config()};

    edge::TcpPollBatch opened;
    opened.events = {
        {edge::TcpIoKind::Accepted, 0, {}, 0},
        {edge::TcpIoKind::Data, 0, {'a', 'b', 'c'}, 0}
    };

    tracker.consume(opened, at(0));

    edge::TcpPollBatch closed;
    closed.events = {
        {edge::TcpIoKind::Closed, 0, {}, 0}
    };

    tracker.consume(closed, at(1));

    auto snapshot = withActivity(tracker.snapshot(at(1)));

    snapshot.process_state = edge::StatusProcessState::Stopped;

    snapshot.upload = edge::StatusUploadObservation{
        true, true, false, true, false, 0
    };

    const auto expected = encode(snapshot).at("tcp_activity");

    EXPECT_EQ(expected.at("connected_count"), 0U);
    EXPECT_EQ(expected.at("closed_connections"), 1U);
    EXPECT_EQ(expected.at("bytes_received"), 3U);

    const edge::StatusConfig status_config;

    const auto status_line = edge::encodeJsonlStatus(
        {kRun, 2, snapshot.checked_at_ms},
        snapshot,
        status_config
    );

    ASSERT_TRUE(
        std::holds_alternative<std::string>(status_line)
    );

    EXPECT_EQ(
        Json::parse(std::get<std::string>(status_line))
            .at("data").at("tcp_activity"),
        expected
    );

    const auto stopped_line = edge::encodeJsonlStopped(
        {kRun, 3, snapshot.checked_at_ms},
        0,
        snapshot,
        status_config
    );

    ASSERT_TRUE(
        std::holds_alternative<std::string>(stopped_line)
    );

    EXPECT_EQ(
        Json::parse(std::get<std::string>(stopped_line))
            .at("data").at("snapshot").at("tcp_activity"),
        expected
    );
}

} // namespace