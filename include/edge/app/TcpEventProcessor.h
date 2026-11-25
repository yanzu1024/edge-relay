#pragma once

#include "edge/app/EventStreamProcessor.h"
#include "edge/config/AgentConfig.h"
#include "edge/net/TcpPoller.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace edge {

struct TcpEventResult {
    std::size_t slot = 0;

    // 同一槽位被新连接复用时，连接编号也会改变。
    std::uint64_t connection_id = 0;

    // 沿用 M1 的成功、JSON 错误、业务验证错误分类。
    EventProcessResult result;
};

using TcpEventBatch = std::vector<TcpEventResult>;

class TcpEventProcessor {
public:
    using Clock = EventStreamProcessor::Clock;
    using TimePoint = EventStreamProcessor::TimePoint;

    explicit TcpEventProcessor(const TcpConfig& config);

    TcpEventProcessor(const TcpEventProcessor&) = delete;
    TcpEventProcessor& operator=(const TcpEventProcessor&) = delete;

    // 按顺序处理网络事件，最后检查所有活动连接的超时。
    //
    // now_ms：业务验证使用的墙上时间。
    // steady_now：部分帧超时使用的单调时间。
    //
    // 即使 io.error 有值，也需要消费其中已生成的事件。
    // 网络层错误仍由调用方根据原始 io 处理。
    [[nodiscard]] TcpEventBatch consume(
        const TcpPollBatch& io,
        std::int64_t now_ms,
        TimePoint steady_now = Clock::now()
    );

    // 包含活动连接和已经关闭连接的累计统计。
    [[nodiscard]] EventStreamStats stats() const noexcept;

    [[nodiscard]] std::size_t activeCount() const noexcept;

    // 空槽返回 0；越界抛 std::out_of_range。
    [[nodiscard]] std::size_t bufferedBytes(
        std::size_t slot
    ) const;

private:
    struct SlotState {
        std::optional<EventStreamProcessor> processor;
        std::uint64_t connection_id = 0;
    };

    static void appendResults(
        std::size_t slot,
        const SlotState& state,
        EventProcessBatch results,
        TcpEventBatch& output
    );

    TcpConfig config_;
    std::vector<SlotState> slots_;

    std::uint64_t last_connection_id_ = 0;
    EventStreamStats retired_stats_{};
};

} // namespace edge