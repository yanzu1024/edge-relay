#include "edge/status/StatusSnapshot.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace {

using Json = nlohmann::json;

edge::StatusSnapshot baseSnapshot()
{
    edge::StatusSnapshot snapshot;

    snapshot.agent_id = "test-agent";
    snapshot.checked_at_ms = 1700000000100LL;
    snapshot.uptime_ms = 100;
    snapshot.process_state = edge::StatusProcessState::Running;
    snapshot.active_connections = 0;

    edge::StatusUploadObservation upload;
    upload.initialized = true;
    snapshot.upload = upload;

    return snapshot;
}

template <typename T>
edge::StatusSample<T> validSample(T value)
{
    edge::StatusSample<T> sample;

    sample.state = edge::StatusSampleState::Valid;
    sample.value = std::move(value);
    sample.checked_at_ms = 1700000000000LL;

    return sample;
}

Json encode(
    const edge::StatusSnapshot& snapshot,
    const edge::StatusConfig& config = edge::StatusConfig{}
)
{
    return Json::parse(
        edge::serializeStatusSnapshot(snapshot, config)
    );
}

TEST(StatusSnapshotTest, SeparatesRuntimeCountersFromDatabaseStock)
{
    auto snapshot = baseSnapshot();

    snapshot.counters.stored_events = 1;
    snapshot.counters.upload_acked = 100;

    snapshot.outbox = validSample(
        edge::OutboxStateCounts{2, 3, 4, 5, 6, 20, 15}
    );

    edge::StatusConfig config;
    config.pending_warn_records = 15;

    const auto json = encode(snapshot, config);

    EXPECT_EQ(json["counters"]["stored_events"], 1);
    EXPECT_EQ(json["counters"]["upload_acked"], 100);

    EXPECT_EQ(json["outbox"]["value"]["total"], 20);
    EXPECT_EQ(json["outbox"]["value"]["acked"], 5);
    EXPECT_EQ(json["outbox"]["value"]["unacked"], 15);
    EXPECT_EQ(json["outbox"]["value"]["backlog"], 9);

    EXPECT_EQ(json["warnings"]["unacked_records"], true);
}

TEST(StatusSnapshotTest, DeadStillOccupiesUnackedCapacity)
{
    auto snapshot = baseSnapshot();

    snapshot.outbox = validSample(
        edge::OutboxStateCounts{0, 0, 0, 0, 4, 4, 4}
    );

    edge::StatusConfig config;
    config.pending_warn_records = 4;

    const auto json = encode(snapshot, config);

    EXPECT_EQ(json["outbox"]["value"]["unacked"], 4);
    EXPECT_EQ(json["outbox"]["value"]["backlog"], 0);
    EXPECT_EQ(json["warnings"]["unacked_records"], true);
}

TEST(StatusSnapshotTest, UnknownValuesRemainNull)
{
    auto snapshot = baseSnapshot();

    snapshot.active_connections.reset();
    snapshot.upload.reset();

    const auto json = encode(snapshot);

    EXPECT_TRUE(json["active_connections"].is_null());

    EXPECT_EQ(json["upload"]["state"], "UNKNOWN");
    EXPECT_TRUE(json["upload"]["blocked"].is_null());

    EXPECT_EQ(json["outbox"]["state"], "UNKNOWN");
    EXPECT_TRUE(json["outbox"]["value"].is_null());
    EXPECT_TRUE(json["outbox"]["checked_at_ms"].is_null());

    EXPECT_TRUE(json["disk_available_bytes"]["value"].is_null());

    EXPECT_TRUE(json["warnings"]["unacked_records"].is_null());
    EXPECT_TRUE(json["warnings"]["disk_available"].is_null());
}

TEST(StatusSnapshotTest, ValidZeroIsNotUnknown)
{
    auto snapshot = baseSnapshot();

    snapshot.outbox = validSample(edge::OutboxStateCounts{});
    snapshot.disk_available_bytes =
        validSample(std::uint64_t{0});

    const auto json = encode(snapshot);

    EXPECT_EQ(json["active_connections"], 0);

    EXPECT_EQ(json["outbox"]["state"], "VALID");
    EXPECT_EQ(json["outbox"]["value"]["total"], 0);
    EXPECT_EQ(json["warnings"]["unacked_records"], false);

    EXPECT_EQ(json["disk_available_bytes"]["state"], "VALID");
    EXPECT_EQ(json["disk_available_bytes"]["value"], 0);
    EXPECT_EQ(json["warnings"]["disk_available"], true);
}

TEST(StatusSnapshotTest, QueryFailuresDoNotBecomeZero)
{
    auto snapshot = baseSnapshot();

    snapshot.outbox.state = edge::StatusSampleState::Error;
    snapshot.outbox.checked_at_ms = snapshot.checked_at_ms;
    snapshot.outbox.error = "database busy";

    snapshot.disk_available_bytes.state =
        edge::StatusSampleState::Error;
    snapshot.disk_available_bytes.checked_at_ms =
        snapshot.checked_at_ms;
    snapshot.disk_available_bytes.error = "space query failed";

    const auto json = encode(snapshot);

    EXPECT_EQ(json["outbox"]["state"], "ERROR");
    EXPECT_EQ(json["outbox"]["error"], "database busy");
    EXPECT_TRUE(json["outbox"]["value"].is_null());

    EXPECT_EQ(json["disk_available_bytes"]["state"], "ERROR");
    EXPECT_TRUE(json["disk_available_bytes"]["value"].is_null());

    EXPECT_TRUE(json["warnings"]["unacked_records"].is_null());
    EXPECT_TRUE(json["warnings"]["disk_available"].is_null());
}

TEST(StatusSnapshotTest, StaleValuesKeepTheirOriginalTime)
{
    auto snapshot = baseSnapshot();

    snapshot.outbox = validSample(
        edge::OutboxStateCounts{3, 0, 0, 0, 0, 3, 3}
    );
    snapshot.outbox.state = edge::StatusSampleState::Stale;

    snapshot.disk_available_bytes =
        validSample(std::uint64_t{0});
    snapshot.disk_available_bytes.state =
        edge::StatusSampleState::Stale;

    const auto json = encode(snapshot);

    EXPECT_EQ(json["outbox"]["state"], "STALE");
    EXPECT_EQ(json["outbox"]["value"]["total"], 3);
    EXPECT_EQ(
        json["outbox"]["checked_at_ms"],
        1700000000000LL
    );

    EXPECT_EQ(json["disk_available_bytes"]["value"], 0);

    EXPECT_TRUE(json["warnings"]["unacked_records"].is_null());
    EXPECT_TRUE(json["warnings"]["disk_available"].is_null());
}

TEST(StatusSnapshotTest, RunningProcessCanHaveBlockedUpload)
{
    auto snapshot = baseSnapshot();

    snapshot.upload->blocked = true;
    snapshot.upload->blocked_http_status = 401;

    const auto json = encode(snapshot);

    EXPECT_EQ(json["process_state"], "RUNNING");
    EXPECT_EQ(json["upload"]["state"], "BLOCKED");
    EXPECT_EQ(json["upload"]["blocked"], true);
    EXPECT_EQ(json["upload"]["blocked_http_status"], 401);
}

TEST(StatusSnapshotTest, FinishedWorkerIsNotReportedAsEnabled)
{
    auto snapshot = baseSnapshot();

    snapshot.upload->finished = true;
    snapshot.upload->blocked = true;
    snapshot.upload->blocked_http_status = 409;

    const auto json = encode(snapshot);

    EXPECT_EQ(json["upload"]["initialized"], true);
    EXPECT_EQ(json["upload"]["state"], "STOPPED");

    // 结束不抹去本进程曾经进入阻塞的事实。
    EXPECT_EQ(json["upload"]["blocked"], true);
    EXPECT_EQ(json["upload"]["blocked_http_status"], 409);
}

TEST(StatusSnapshotTest, FailureTakesPrecedenceOverFinished)
{
    auto snapshot = baseSnapshot();

    snapshot.upload->finished = true;
    snapshot.upload->failed = true;

    const auto json = encode(snapshot);

    EXPECT_EQ(json["upload"]["state"], "FAILED");
}

TEST(StatusSnapshotTest, WorkerLifecycleDoesNotInventUploadingState)
{
    auto snapshot = baseSnapshot();

    snapshot.upload->initialized = false;
    EXPECT_EQ(encode(snapshot)["upload"]["state"], "INITIALIZING");

    snapshot.upload->initialized = true;
    EXPECT_EQ(encode(snapshot)["upload"]["state"], "ENABLED");

    snapshot.upload->stop_requested = true;
    EXPECT_EQ(encode(snapshot)["upload"]["state"], "STOPPING");
}

TEST(StatusSnapshotTest, RejectsMalformedSamples)
{
    auto snapshot = baseSnapshot();

    snapshot.outbox.state = edge::StatusSampleState::Valid;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, {}),
        std::invalid_argument
    );

    snapshot.outbox = validSample(edge::OutboxStateCounts{});
    snapshot.outbox.state = edge::StatusSampleState::Error;
    snapshot.outbox.error = "query failed";

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, {}),
        std::invalid_argument
    );

    snapshot.outbox = validSample(edge::OutboxStateCounts{});
    snapshot.outbox.state = edge::StatusSampleState::Unknown;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, {}),
        std::invalid_argument
    );

    snapshot.outbox = validSample(edge::OutboxStateCounts{});
    snapshot.outbox.checked_at_ms = 0;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, {}),
        std::invalid_argument
    );
}

TEST(StatusSnapshotTest, RejectsInconsistentDatabaseCounts)
{
    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    const std::vector<edge::OutboxStateCounts> invalid_counts{
        {-1, 0, 0, 0, 0, 0, 0},
        {1, 0, 0, 0, 0, 2, 2},
        {1, 0, 0, 0, 0, 1, 0},
        {maximum, 1, 0, 0, 0, maximum, maximum}
    };

    for (const auto& counts : invalid_counts) {
        auto snapshot = baseSnapshot();
        snapshot.outbox = validSample(counts);

        EXPECT_THROW(
            edge::serializeStatusSnapshot(snapshot, {}),
            std::invalid_argument
        );
    }
}

TEST(StatusSnapshotTest, HandlesLargeCountsWithoutSignedOverflow)
{
    auto snapshot = baseSnapshot();

    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    snapshot.outbox = validSample(
        edge::OutboxStateCounts{
            maximum - 1,
            0,
            0,
            1,
            0,
            maximum,
            maximum - 1
        }
    );

    const auto json = encode(snapshot);

    EXPECT_EQ(
        json["outbox"]["value"]["backlog"].get<std::int64_t>(),
        maximum - 1
    );
}

TEST(StatusSnapshotTest, RejectsInvalidThresholdsAndSnapshotTime)
{
    auto snapshot = baseSnapshot();

    edge::StatusConfig config;
    config.pending_warn_records = 0;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, config),
        std::invalid_argument
    );

    config = edge::StatusConfig{};
    config.disk_free_warn_mb =
        std::numeric_limits<std::int64_t>::max();

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, config),
        std::invalid_argument
    );

    snapshot.checked_at_ms = 0;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, {}),
        std::invalid_argument
    );
}

TEST(StatusSnapshotTest, RejectsInconsistentUploadObservation)
{
    auto snapshot = baseSnapshot();

    snapshot.upload->blocked = true;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, {}),
        std::invalid_argument
    );

    snapshot.upload->blocked = false;
    snapshot.upload->blocked_http_status = 401;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, {}),
        std::invalid_argument
    );

    snapshot.upload->blocked_http_status = 0;
    snapshot.upload->failed = true;

    EXPECT_THROW(
        edge::serializeStatusSnapshot(snapshot, {}),
        std::invalid_argument
    );
}

TEST(StatusSnapshotTest, EscapesTextAndPreservesUnsignedCounters)
{
    auto snapshot = baseSnapshot();

    snapshot.agent_id = "agent\"one\nsecond-line";
    snapshot.counters.upload_claimed =
        std::numeric_limits<std::uint64_t>::max();

    const auto text = edge::serializeStatusSnapshot(snapshot, {});
    const auto json = Json::parse(text);

    EXPECT_EQ(text.find('\n'), std::string::npos);
    EXPECT_EQ(json["agent_id"].get<std::string>(), snapshot.agent_id);

    EXPECT_TRUE(
        json["counters"]["upload_claimed"].is_number_unsigned()
    );

    EXPECT_EQ(
        json["counters"]["upload_claimed"].get<std::uint64_t>(),
        std::numeric_limits<std::uint64_t>::max()
    );
}

TEST(StatusSnapshotTest, DiskThresholdUsesMiBAndIncludesBoundary)
{
    auto snapshot = baseSnapshot();

    constexpr std::uint64_t threshold =
        200ULL * 1024ULL * 1024ULL;

    snapshot.disk_available_bytes = validSample(threshold);

    auto json = encode(snapshot);

    EXPECT_EQ(
        json["thresholds"]["disk_available_warn_bytes"],
        threshold
    );
    EXPECT_EQ(json["warnings"]["disk_available"], true);

    snapshot.disk_available_bytes.value = threshold + 1;

    json = encode(snapshot);

    EXPECT_EQ(json["warnings"]["disk_available"], false);
}

} // namespace