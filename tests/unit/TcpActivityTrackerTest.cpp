#include "edge/status/TcpActivityTracker.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using Tracker = edge::TcpActivityTracker;
using Kind = edge::TcpIoKind;
using ConnectionState = edge::TcpConnectionState;

Tracker::TimePoint at(std::int64_t milliseconds)
{
    return Tracker::TimePoint{} +
        std::chrono::milliseconds{milliseconds};
}

edge::TcpIoEvent event(
    Kind kind,
    std::optional<std::size_t> slot,
    const std::string& bytes = {}
)
{
    return {
        kind,
        slot,
        std::vector<std::uint8_t>(
            bytes.begin(),
            bytes.end()
        ),
        0
    };
}

edge::TcpPollBatch batch(
    std::initializer_list<edge::TcpIoEvent> events
)
{
    edge::TcpPollBatch result;
    result.events = events;

    return result;
}

edge::TcpConfig config()
{
    edge::TcpConfig result;

    result.max_connections = 2;
    result.silent_after_ms = 100;

    return result;
}

TEST(TcpActivityTrackerTest, StartsEmpty)
{
    Tracker tracker{config()};

    const auto snapshot = tracker.snapshot(at(0));

    EXPECT_EQ(snapshot.silent_after_ms, 100);

    EXPECT_EQ(snapshot.accepted_connections, 0U);
    EXPECT_EQ(snapshot.closed_connections, 0U);
    EXPECT_EQ(snapshot.rejected_connections, 0U);
    EXPECT_EQ(snapshot.client_errors, 0U);

    EXPECT_EQ(snapshot.bytes_received, 0U);

    EXPECT_EQ(snapshot.connected_count, 0U);
    EXPECT_EQ(snapshot.waiting_count, 0U);
    EXPECT_EQ(snapshot.active_count, 0U);
    EXPECT_EQ(snapshot.silent_count, 0U);

    EXPECT_TRUE(snapshot.connections.empty());
}

TEST(TcpActivityTrackerTest, WaitingBecomesSilentAtThreshold)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0)
        }),
        at(0)
    );

    auto snapshot = tracker.snapshot(at(99));

    ASSERT_EQ(snapshot.connections.size(), 1U);

    EXPECT_EQ(snapshot.waiting_count, 1U);
    EXPECT_EQ(snapshot.silent_count, 0U);

    EXPECT_EQ(
        snapshot.connections[0].state,
        ConnectionState::Waiting
    );

    EXPECT_EQ(
        snapshot.connections[0].connected_ms,
        99U
    );

    EXPECT_FALSE(
        snapshot.connections[0]
            .last_data_age_ms.has_value()
    );

    snapshot = tracker.snapshot(at(100));

    EXPECT_EQ(snapshot.waiting_count, 0U);
    EXPECT_EQ(snapshot.silent_count, 1U);

    EXPECT_EQ(
        snapshot.connections[0].state,
        ConnectionState::Silent
    );

    // 从未收到数据，不能伪造最近接收时间。
    EXPECT_FALSE(
        snapshot.connections[0]
            .last_data_age_ms.has_value()
    );
}

TEST(TcpActivityTrackerTest, ReceivedBytesUseLastDataDeadline)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0)
        }),
        at(0)
    );

    tracker.consume(
        batch({
            event(Kind::Data, 0, "abc")
        }),
        at(20)
    );

    auto snapshot = tracker.snapshot(
        at(119) + std::chrono::microseconds{999}
    );

    ASSERT_EQ(snapshot.connections.size(), 1U);

    EXPECT_EQ(snapshot.active_count, 1U);

    EXPECT_EQ(
        snapshot.connections[0].state,
        ConnectionState::Active
    );

    ASSERT_TRUE(
        snapshot.connections[0]
            .last_data_age_ms.has_value()
    );

    EXPECT_EQ(
        *snapshot.connections[0].last_data_age_ms,
        99U
    );

    snapshot = tracker.snapshot(at(120));

    EXPECT_EQ(snapshot.silent_count, 1U);

    EXPECT_EQ(
        snapshot.connections[0].state,
        ConnectionState::Silent
    );

    EXPECT_EQ(
        *snapshot.connections[0].last_data_age_ms,
        100U
    );

    EXPECT_EQ(snapshot.bytes_received, 3U);
}

TEST(
    TcpActivityTrackerTest,
    AnyNonemptyDataRecoversSilentConnection
)
{
    Tracker tracker{config()};

    const std::string noise("\0\xff", 2);

    tracker.consume(
        batch({
            event(Kind::Accepted, 0),
            event(Kind::Data, 0, noise)
        }),
        at(0)
    );

    EXPECT_EQ(
        tracker.snapshot(at(100)).silent_count,
        1U
    );

    tracker.consume(
        batch({
            event(
                Kind::Data,
                0,
                std::string(1, '\0')
            )
        }),
        at(101)
    );

    const auto snapshot = tracker.snapshot(at(101));

    ASSERT_EQ(snapshot.connections.size(), 1U);

    EXPECT_EQ(snapshot.active_count, 1U);
    EXPECT_EQ(snapshot.silent_count, 0U);

    EXPECT_EQ(snapshot.bytes_received, 3U);
    EXPECT_EQ(snapshot.connections[0].bytes_received, 3U);

    ASSERT_TRUE(
        snapshot.connections[0]
            .last_data_age_ms.has_value()
    );

    EXPECT_EQ(
        *snapshot.connections[0].last_data_age_ms,
        0U
    );
}

TEST(TcpActivityTrackerTest, EmptyDataDoesNotRefreshDeadline)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0),
            event(Kind::Data, 0, "x")
        }),
        at(20)
    );

    tracker.consume(
        batch({
            event(Kind::Data, 0)
        }),
        at(119)
    );

    const auto snapshot = tracker.snapshot(at(120));

    ASSERT_EQ(snapshot.connections.size(), 1U);

    EXPECT_EQ(snapshot.silent_count, 1U);
    EXPECT_EQ(snapshot.bytes_received, 1U);

    ASSERT_TRUE(
        snapshot.connections[0]
            .last_data_age_ms.has_value()
    );

    EXPECT_EQ(
        *snapshot.connections[0].last_data_age_ms,
        100U
    );
}

TEST(TcpActivityTrackerTest, SlotsHaveIndependentDeadlines)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0),
            event(Kind::Accepted, 1)
        }),
        at(0)
    );

    tracker.consume(
        batch({
            event(Kind::Data, 0, "x")
        }),
        at(50)
    );

    const auto snapshot = tracker.snapshot(at(100));

    ASSERT_EQ(snapshot.connections.size(), 2U);

    EXPECT_EQ(snapshot.connected_count, 2U);
    EXPECT_EQ(snapshot.active_count, 1U);
    EXPECT_EQ(snapshot.silent_count, 1U);
    EXPECT_EQ(snapshot.waiting_count, 0U);

    EXPECT_EQ(snapshot.connections[0].slot, 0U);

    EXPECT_EQ(
        snapshot.connections[0].state,
        ConnectionState::Active
    );

    EXPECT_EQ(snapshot.connections[1].slot, 1U);

    EXPECT_EQ(
        snapshot.connections[1].state,
        ConnectionState::Silent
    );

    EXPECT_FALSE(
        snapshot.connections[1]
            .last_data_age_ms.has_value()
    );
}

TEST(
    TcpActivityTrackerTest,
    SlotReuseResetsConnectionObservation
)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0),
            event(Kind::Data, 0, "a")
        }),
        at(10)
    );

    // 同一批内：旧连接尾部数据、关闭、槽位复用。
    tracker.consume(
        batch({
            event(Kind::Data, 0, "tail"),
            event(Kind::Closed, 0),
            event(Kind::Accepted, 0)
        }),
        at(20)
    );

    auto snapshot = tracker.snapshot(at(20));

    ASSERT_EQ(snapshot.connections.size(), 1U);

    EXPECT_EQ(snapshot.accepted_connections, 2U);
    EXPECT_EQ(snapshot.closed_connections, 1U);
    EXPECT_EQ(snapshot.bytes_received, 5U);

    const auto& current = snapshot.connections[0];

    EXPECT_EQ(current.connection_id, 2U);
    EXPECT_EQ(current.bytes_received, 0U);
    EXPECT_EQ(current.connected_ms, 0U);

    EXPECT_FALSE(current.last_data_age_ms.has_value());

    EXPECT_EQ(
        current.state,
        ConnectionState::Waiting
    );

    tracker.consume(
        batch({
            event(Kind::Data, 0, "bc")
        }),
        at(21)
    );

    snapshot = tracker.snapshot(at(21));

    EXPECT_EQ(snapshot.connections[0].connection_id, 2U);
    EXPECT_EQ(snapshot.connections[0].bytes_received, 2U);

    EXPECT_EQ(snapshot.bytes_received, 7U);
}

TEST(TcpActivityTrackerTest, CloseAndClientErrorRetireRows)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0),
            event(Kind::Accepted, 1),
            event(Kind::Data, 0, "a"),
            event(Kind::Data, 1, "bc")
        }),
        at(0)
    );

    auto client_error = event(Kind::ClientError, 0);
    client_error.error_code = EIO;

    tracker.consume(
        batch({
            client_error,
            event(Kind::Closed, 1)
        }),
        at(10)
    );

    const auto snapshot = tracker.snapshot(at(10));

    EXPECT_TRUE(snapshot.connections.empty());

    EXPECT_EQ(snapshot.connected_count, 0U);
    EXPECT_EQ(snapshot.waiting_count, 0U);
    EXPECT_EQ(snapshot.active_count, 0U);
    EXPECT_EQ(snapshot.silent_count, 0U);

    EXPECT_EQ(snapshot.accepted_connections, 2U);
    EXPECT_EQ(snapshot.closed_connections, 2U);
    EXPECT_EQ(snapshot.client_errors, 1U);

    EXPECT_EQ(snapshot.bytes_received, 3U);
}

TEST(
    TcpActivityTrackerTest,
    CapacityRejectionDoesNotConsumeConnectionId
)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0),
            event(
                Kind::RejectedAtCapacity,
                std::nullopt
            ),
            event(Kind::Accepted, 1)
        }),
        at(0)
    );

    const auto snapshot = tracker.snapshot(at(0));

    ASSERT_EQ(snapshot.connections.size(), 2U);

    EXPECT_EQ(snapshot.accepted_connections, 2U);
    EXPECT_EQ(snapshot.rejected_connections, 1U);

    EXPECT_EQ(snapshot.connections[0].connection_id, 1U);
    EXPECT_EQ(snapshot.connections[1].connection_id, 2U);
}

TEST(
    TcpActivityTrackerTest,
    ConsumesEventsEvenWhenBatchHasError
)
{
    Tracker tracker{config()};

    auto input = batch({
        event(Kind::Accepted, 0),
        event(Kind::Data, 0, "abc")
    });

    input.error = edge::TcpPollError{
        "poll",
        EIO,
        "simulated error"
    };

    tracker.consume(input, at(0));

    const auto snapshot = tracker.snapshot(at(0));

    EXPECT_EQ(snapshot.accepted_connections, 1U);
    EXPECT_EQ(snapshot.connected_count, 1U);
    EXPECT_EQ(snapshot.bytes_received, 3U);
    EXPECT_EQ(snapshot.active_count, 1U);
}

TEST(TcpActivityTrackerTest, InvalidBatchDoesNotCommitPrefix)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0),
            event(Kind::Data, 0, "a")
        }),
        at(10)
    );

    const auto invalid = batch({
        event(Kind::Data, 0, "bc"),
        event(
            Kind::RejectedAtCapacity,
            std::nullopt
        ),

        // 槽位 1 尚未接入。
        event(Kind::Data, 1, "orphan")
    });

    EXPECT_THROW(
        tracker.consume(invalid, at(20)),
        std::logic_error
    );

    auto snapshot = tracker.snapshot(at(20));

    EXPECT_EQ(snapshot.bytes_received, 1U);
    EXPECT_EQ(snapshot.rejected_connections, 0U);

    ASSERT_EQ(snapshot.connections.size(), 1U);

    EXPECT_EQ(snapshot.connections[0].bytes_received, 1U);

    ASSERT_TRUE(
        snapshot.connections[0]
            .last_data_age_ms.has_value()
    );

    EXPECT_EQ(
        *snapshot.connections[0].last_data_age_ms,
        10U
    );

    // 失败批次的时间也未提交，最近成功时间仍是 10。
    EXPECT_NO_THROW(
        tracker.consume(
            batch({
                event(Kind::Data, 0, "d")
            }),
            at(15)
        )
    );

    snapshot = tracker.snapshot(at(15));

    EXPECT_EQ(snapshot.bytes_received, 2U);

    EXPECT_THROW(
        tracker.consume(
            batch({
                event(Kind::Accepted, 0)
            }),
            at(16)
        ),
        std::logic_error
    );

    EXPECT_THROW(
        tracker.consume(
            batch({
                event(Kind::Data, 2, "outside")
            }),
            at(16)
        ),
        std::logic_error
    );

    EXPECT_THROW(
        tracker.consume(
            batch({
                event(Kind::Data, std::nullopt, "missing")
            }),
            at(16)
        ),
        std::logic_error
    );
}

TEST(TcpActivityTrackerTest, RejectsBackwardTime)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0)
        }),
        at(10)
    );

    EXPECT_THROW(
        tracker.consume({}, at(9)),
        std::invalid_argument
    );

    EXPECT_THROW(
        tracker.snapshot(at(9)),
        std::invalid_argument
    );

    tracker.consume({}, at(11));

    EXPECT_THROW(
        tracker.snapshot(at(10)),
        std::invalid_argument
    );

    EXPECT_EQ(
        tracker.snapshot(at(11)).waiting_count,
        1U
    );
}

TEST(TcpActivityTrackerTest, RejectsInvalidConfiguration)
{
    for (const auto capacity :
         {0, -1, edge::kMaxTcpConnections + 1}) {
        auto value = config();
        value.max_connections = capacity;

        EXPECT_THROW(
            (Tracker{value}),
            std::invalid_argument
        );
    }

    for (const auto threshold : {0, -1}) {
        auto value = config();
        value.silent_after_ms = threshold;

        EXPECT_THROW(
            (Tracker{value}),
            std::invalid_argument
        );
    }
}

TEST(TcpActivityTrackerTest, SnapshotsOwnTheirRows)
{
    Tracker tracker{config()};

    tracker.consume(
        batch({
            event(Kind::Accepted, 0),
            event(Kind::Data, 0, "a")
        }),
        at(0)
    );

    const auto previous = tracker.snapshot(at(0));

    tracker.consume(
        batch({
            event(Kind::Closed, 0)
        }),
        at(1)
    );

    const auto current = tracker.snapshot(at(1));

    EXPECT_TRUE(current.connections.empty());

    ASSERT_EQ(previous.connections.size(), 1U);

    EXPECT_EQ(previous.connections[0].connection_id, 1U);
    EXPECT_EQ(previous.connections[0].bytes_received, 1U);
    EXPECT_EQ(previous.active_count, 1U);
}

} // namespace