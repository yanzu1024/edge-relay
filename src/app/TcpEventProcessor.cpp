#include "edge/app/TcpEventProcessor.h"

#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace edge {

namespace {

std::size_t checkedCapacity(std::int32_t limit)
{
    if (limit < 1 || limit > kMaxTcpConnections) {
        throw std::invalid_argument(
            "max_connections must be between 1 and " +
            std::to_string(kMaxTcpConnections)
        );
    }

    return static_cast<std::size_t>(limit);
}

void addStats(
    EventStreamStats& total,
    const EventStreamStats& value
) noexcept
{
    total.framing.completed_frames +=
        value.framing.completed_frames;

    total.framing.invalid_lengths +=
        value.framing.invalid_lengths;

    total.framing.resync_bytes +=
        value.framing.resync_bytes;

    total.framing.partial_timeouts +=
        value.framing.partial_timeouts;

    total.valid_events += value.valid_events;
    total.json_errors += value.json_errors;
    total.validation_errors += value.validation_errors;
}

} // namespace

TcpEventProcessor::TcpEventProcessor(const TcpConfig& config)
    : config_(config),
      slots_(checkedCapacity(config.max_connections))
{
    // 复用已有处理器的参数检查。
    // 非法超时或载荷限制应在构造时暴露，而非等客户端接入。
    (void)EventStreamProcessor{config_};
}

void TcpEventProcessor::appendResults(
    std::size_t slot,
    const SlotState& state,
    EventProcessBatch results,
    TcpEventBatch& output
)
{
    for (auto& result : results) {
        output.push_back({
            slot,
            state.connection_id,
            std::move(result)
        });
    }
}

TcpEventBatch TcpEventProcessor::consume(
    const TcpPollBatch& io,
    std::int64_t now_ms,
    TimePoint steady_now
)
{
    if (now_ms <= 0) {
        throw std::invalid_argument(
            "reference wall time must be positive"
        );
    }

    TcpEventBatch output;

    for (const auto& event : io.events) {
        if (event.kind == TcpIoKind::RejectedAtCapacity) {
            continue;
        }

        if (!event.slot || *event.slot >= slots_.size()) {
            throw std::logic_error(
                "TCP event contains an invalid slot"
            );
        }

        const std::size_t slot = *event.slot;
        auto& state = slots_[slot];

        if (event.kind == TcpIoKind::Accepted) {
            if (state.processor) {
                throw std::logic_error(
                    "TCP slot accepted before previous connection closed"
                );
            }

            if (last_connection_id_ ==
                std::numeric_limits<std::uint64_t>::max()) {
                throw std::overflow_error(
                    "TCP connection ID exhausted"
                );
            }

            // 重新构造，确保字节缓冲、计时和本连接统计均为初始状态。
            state.processor.emplace(config_);
            state.connection_id = ++last_connection_id_;

            continue;
        }

        if (!state.processor) {
            throw std::logic_error(
                "TCP event refers to an inactive slot"
            );
        }

        if (event.kind == TcpIoKind::Data) {
            appendResults(
                slot,
                state,
                state.processor->feed(
                    event.bytes.data(),
                    event.bytes.size(),
                    now_ms,
                    steady_now
                ),
                output
            );

            continue;
        }

        if (event.kind == TcpIoKind::Closed ||
            event.kind == TcpIoKind::ClientError) {
            // 关闭前仍需检查超时。
            // 已超时的候选帧后面，可能藏有可恢复的完整帧。
            appendResults(
                slot,
                state,
                state.processor->checkTimeout(
                    now_ms,
                    steady_now
                ),
                output
            );

            addStats(
                retired_stats_,
                state.processor->stats()
            );

            state.processor.reset();
            state.connection_id = 0;

            continue;
        }

        throw std::logic_error(
            "unknown TCP event kind"
        );
    }

    // 即使本轮没有收到字节，也必须推进部分帧超时。
    for (std::size_t slot = 0; slot < slots_.size(); ++slot) {
        auto& state = slots_[slot];

        if (state.processor) {
            appendResults(
                slot,
                state,
                state.processor->checkTimeout(
                    now_ms,
                    steady_now
                ),
                output
            );
        }
    }

    return output;
}

EventStreamStats TcpEventProcessor::stats() const noexcept
{
    EventStreamStats total = retired_stats_;

    for (const auto& state : slots_) {
        if (state.processor) {
            addStats(total, state.processor->stats());
        }
    }

    return total;
}

std::size_t TcpEventProcessor::activeCount() const noexcept
{
    std::size_t count = 0;

    for (const auto& state : slots_) {
        if (state.processor) {
            ++count;
        }
    }

    return count;
}

std::size_t TcpEventProcessor::bufferedBytes(
    std::size_t slot
) const
{
    const auto& state = slots_.at(slot);

    return state.processor
        ? state.processor->bufferedBytes()
        : 0;
}

} // namespace edge