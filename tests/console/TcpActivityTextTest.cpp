#include "presentation/TcpActivityText.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <string>

namespace {

using namespace edge::console;

StatusSummary tcpSnapshot()
{
    StatusSummary snapshot;

    snapshot.agent_id = "tcp-text-test";
    snapshot.run_id = std::string(32, 'a');

    snapshot.sequence = 2;
    snapshot.checked_at_ms = 1700000000000LL;

    snapshot.process_state = ProcessState::Running;
    snapshot.active_connections = 0;

    snapshot.tcp_activity = TcpActivityObservation{};
    snapshot.tcp_activity->silent_after_ms = 1000;

    return snapshot;
}

TcpConnectionObservation waitingConnection(
    std::uint64_t slot = 0,
    std::uint64_t connectionId = 1
)
{
    TcpConnectionObservation connection;

    connection.slot = slot;
    connection.connection_id = connectionId;

    connection.state = TcpConnectionState::Waiting;
    connection.connected_ms = 10;

    return connection;
}

void setSingleConnection(
    StatusSummary& snapshot,
    const TcpConnectionObservation& connection
)
{
    auto& activity = *snapshot.tcp_activity;

    activity.connections = {connection};

    activity.connected_count = 1;

    activity.accepted_connections =
        connection.connection_id;

    activity.closed_connections =
        connection.connection_id - 1;

    activity.bytes_received = connection.bytes_received;

    activity.waiting_count =
        connection.state == TcpConnectionState::Waiting ? 1 : 0;

    activity.active_count =
        connection.state == TcpConnectionState::Active ? 1 : 0;

    activity.silent_count =
        connection.state == TcpConnectionState::Silent ? 1 : 0;

    snapshot.active_connections = 1;
}

TEST(TcpActivityTextTest, MissingObservationIsUnknown)
{
    const auto missing = makeTcpActivityText(
        std::nullopt,
        true
    );

    EXPECT_TRUE(missing.rows.empty());
    EXPECT_EQ(missing.totals, QStringLiteral("未知"));

    auto snapshot = tcpSnapshot();
    snapshot.tcp_activity.reset();

    const auto unknown = makeTcpActivityText(snapshot, true);

    EXPECT_TRUE(unknown.rows.empty());

    EXPECT_TRUE(
        unknown.scope.contains(QStringLiteral("TCP 观察未知"))
    );

    EXPECT_EQ(unknown.totals, QStringLiteral("未知"));
}

TEST(TcpActivityTextTest, KnownEmptyObservationShowsZero)
{
    const auto text = makeTcpActivityText(
        tcpSnapshot(),
        true
    );

    EXPECT_TRUE(text.rows.empty());

    EXPECT_TRUE(
        text.scope.contains(QStringLiteral("没有连接"))
    );

    EXPECT_TRUE(
        text.totals.contains(QStringLiteral("当前连接=0"))
    );

    EXPECT_TRUE(
        text.totals.contains(QStringLiteral("收到字节累计=0 B"))
    );
}

TEST(TcpActivityTextTest, NeverReceivedDiffersFromZeroAge)
{
    auto snapshot = tcpSnapshot();

    auto connection = waitingConnection();

    setSingleConnection(snapshot, connection);

    const auto waiting = makeTcpActivityText(snapshot, true);

    ASSERT_EQ(waiting.rows.size(), 1U);

    EXPECT_EQ(
        waiting.rows[0][4],
        QStringLiteral("尚未收到数据")
    );

    connection.state = TcpConnectionState::Active;
    connection.last_data_age_ms = 0;
    connection.bytes_received = 1;

    setSingleConnection(snapshot, connection);

    const auto active = makeTcpActivityText(snapshot, true);

    ASSERT_EQ(active.rows.size(), 1U);

    EXPECT_EQ(active.rows[0][2], QStringLiteral("ACTIVE"));
    EXPECT_EQ(active.rows[0][4], QStringLiteral("0"));
    EXPECT_EQ(active.rows[0][5], QStringLiteral("1"));
}

TEST(TcpActivityTextTest, SilentConnectionMayHaveNeverReceivedData)
{
    auto snapshot = tcpSnapshot();

    auto connection = waitingConnection();

    connection.state = TcpConnectionState::Silent;
    connection.connected_ms = 1000;

    setSingleConnection(snapshot, connection);

    const auto text = makeTcpActivityText(snapshot, true);

    ASSERT_EQ(text.rows.size(), 1U);

    EXPECT_EQ(text.rows[0][2], QStringLiteral("SILENT"));

    EXPECT_EQ(
        text.rows[0][4],
        QStringLiteral("尚未收到数据")
    );

    EXPECT_TRUE(
        text.totals.contains(QStringLiteral("当前连接=1"))
    );
}

TEST(TcpActivityTextTest, PreservesUint64Precision)
{
    auto snapshot = tcpSnapshot();

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    auto connection = waitingConnection(0, maximum);

    connection.state = TcpConnectionState::Silent;
    connection.connected_ms = maximum;
    connection.last_data_age_ms = maximum;
    connection.bytes_received = maximum;

    setSingleConnection(snapshot, connection);

    const auto text = makeTcpActivityText(snapshot, true);

    ASSERT_EQ(text.rows.size(), 1U);

    const QString expected =
        QStringLiteral("18446744073709551615");

    EXPECT_EQ(text.rows[0][1], expected);
    EXPECT_EQ(text.rows[0][3], expected);
    EXPECT_EQ(text.rows[0][4], expected);
    EXPECT_EQ(text.rows[0][5], expected);
}

TEST(TcpActivityTextTest, ReusedSlotShowsNewConnectionIdentity)
{
    auto snapshot = tcpSnapshot();

    setSingleConnection(
        snapshot,
        waitingConnection(0, 1)
    );

    const auto previous = makeTcpActivityText(snapshot, true);

    ASSERT_EQ(previous.rows.size(), 1U);
    EXPECT_EQ(previous.rows[0][1], QStringLiteral("1"));

    setSingleConnection(
        snapshot,
        waitingConnection(0, 2)
    );

    const auto current = makeTcpActivityText(snapshot, true);

    ASSERT_EQ(current.rows.size(), 1U);

    EXPECT_EQ(current.rows[0][0], QStringLiteral("0"));
    EXPECT_EQ(current.rows[0][1], QStringLiteral("2"));
}

TEST(TcpActivityTextTest, ClosedRowsDisappearButTotalsRemain)
{
    auto snapshot = tcpSnapshot();

    auto connection = waitingConnection();

    connection.state = TcpConnectionState::Active;
    connection.last_data_age_ms = 0;
    connection.bytes_received = 32;

    setSingleConnection(snapshot, connection);

    const auto connected = makeTcpActivityText(snapshot, true);

    ASSERT_EQ(connected.rows.size(), 1U);

    auto& activity = *snapshot.tcp_activity;

    activity.connections.clear();
    activity.connected_count = 0;

    activity.waiting_count = 0;
    activity.active_count = 0;
    activity.silent_count = 0;

    activity.closed_connections = 1;
    snapshot.active_connections = 0;

    const auto closed = makeTcpActivityText(snapshot, true);

    EXPECT_TRUE(closed.rows.empty());

    EXPECT_TRUE(
        closed.totals.contains(QStringLiteral("关闭累计=1"))
    );

    EXPECT_TRUE(
        closed.totals.contains(QStringLiteral("收到字节累计=32 B"))
    );
}

TEST(TcpActivityTextTest, DisplaysAllEightSlotsInOrder)
{
    auto snapshot = tcpSnapshot();

    auto& activity = *snapshot.tcp_activity;

    for (std::uint64_t slot = 0; slot < 8; ++slot) {
        activity.connections.push_back(
            waitingConnection(slot, slot + 1)
        );
    }

    activity.connected_count = 8;
    activity.waiting_count = 8;
    activity.accepted_connections = 8;

    snapshot.active_connections = 8;

    const auto text = makeTcpActivityText(snapshot, true);

    ASSERT_EQ(text.rows.size(), 8U);

    EXPECT_EQ(text.rows.front()[0], QStringLiteral("0"));
    EXPECT_EQ(text.rows.back()[0], QStringLiteral("7"));
}

TEST(TcpActivityTextTest, RejectsOversizedDisplayInput)
{
    auto snapshot = tcpSnapshot();

    // 正常解码路径会更早拒绝；这里验证显示层防御。
    snapshot.tcp_activity->connections.resize(9);

    const auto text = makeTcpActivityText(snapshot, true);

    EXPECT_TRUE(text.rows.empty());

    EXPECT_TRUE(
        text.totals.contains(QStringLiteral("超出协议上限"))
    );
}

TEST(TcpActivityTextTest, ExitedRowsAreMarkedHistorical)
{
    auto snapshot = tcpSnapshot();

    setSingleConnection(
        snapshot,
        waitingConnection()
    );

    const auto text = makeTcpActivityText(snapshot, false);

    ASSERT_EQ(text.rows.size(), 1U);

    EXPECT_TRUE(
        text.scope.contains(QStringLiteral("进程已退出"))
    );

    EXPECT_TRUE(
        text.scope.contains(QStringLiteral("未确认最终状态"))
    );
}

} // namespace