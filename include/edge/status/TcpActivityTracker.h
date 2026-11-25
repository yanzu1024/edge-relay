#pragma once

#include "edge/config/AgentConfig.h"
#include "edge/net/TcpPoller.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace edge {

enum class TcpConnectionState {
    Waiting,
    Active,
    Silent
};

struct TcpConnectionObservation {
    std::size_t slot = 0;
    std::uint64_t connection_id = 0;

    TcpConnectionState state =
        TcpConnectionState::Waiting;

    // 从本次连接接入开始经过的毫秒数。
    std::uint64_t connected_ms = 0;

    // 尚未收到非空数据时为 nullopt。
    // 收到数据的当下，0 是有效值。
    std::optional<std::uint64_t> last_data_age_ms;

    // 当前连接实际收到的累计字节数。
    std::uint64_t bytes_received = 0;
};

struct TcpActivitySnapshot {
    std::int32_t silent_after_ms = 0;

    // 本次运行累计值，包含已经关闭的连接。
    std::uint64_t accepted_connections = 0;
    std::uint64_t closed_connections = 0;
    std::uint64_t rejected_connections = 0;

    // ClientError 同时计入 closed_connections。
    std::uint64_t client_errors = 0;

    std::uint64_t bytes_received = 0;

    // 当前仍然连接的数量及状态分布。
    std::size_t connected_count = 0;
    std::size_t waiting_count = 0;
    std::size_t active_count = 0;
    std::size_t silent_count = 0;

    // 仅包含当前活动连接，按槽位排序。
    // 关闭后移除，数量最多为配置的连接上限。
    std::vector<TcpConnectionObservation> connections;
};

// 单线程调用，不持有 Socket，不执行 I/O。
//
// 时间由调用方提供，组件内部不读取时钟。
// 同一批事件使用同一个单调时间。
//
// 必须保持 TcpPollBatch 的事件顺序。
// 即使 batch.error 有值，已有 events 仍会消费。
class TcpActivityTracker {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    explicit TcpActivityTracker(const TcpConfig& config);

    TcpActivityTracker(const TcpActivityTracker&) = delete;
    TcpActivityTracker& operator=(
        const TcpActivityTracker&
    ) = delete;

    // 非法槽位、重复 Accepted 或对空槽位操作，
    // 抛 std::logic_error。
    //
    // 时间早于上一批时抛 std::invalid_argument。
    // 计数溢出时抛 std::overflow_error。
    //
    // 发生异常时，整批观察状态保持原样。
    void consume(
        const TcpPollBatch& batch,
        TimePoint now
    );

    // 返回拥有自身数据的快照。
    // now 不能早于最近一次成功 consume 的时间。
    //
    // 状态按 now 计算，因此没有新数据时也能观察到静默。
    [[nodiscard]]
    TcpActivitySnapshot snapshot(TimePoint now) const;

private:
    struct Slot {
        bool active = false;

        std::uint64_t connection_id = 0;

        TimePoint accepted_at{};
        std::optional<TimePoint> last_data_at;

        std::uint64_t bytes_received = 0;
    };

    struct State {
        std::array<
            Slot,
            static_cast<std::size_t>(kMaxTcpConnections)
        > slots{};

        std::uint64_t accepted_connections = 0;
        std::uint64_t closed_connections = 0;
        std::uint64_t rejected_connections = 0;
        std::uint64_t client_errors = 0;

        std::uint64_t bytes_received = 0;

        std::optional<TimePoint> last_consumed_at;
    };

    std::size_t capacity_;
    std::int32_t silent_after_ms_;

    State state_;
};

} // namespace edge