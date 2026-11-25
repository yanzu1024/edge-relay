#include "edge/status/TcpActivityTracker.h"

#include <limits>
#include <stdexcept>

namespace edge {
namespace {

std::size_t checkedCapacity(std::int32_t value)
{
    if (value < 1 || value > kMaxTcpConnections) {
        throw std::invalid_argument(
            "Invalid TCP activity connection capacity"
        );
    }

    return static_cast<std::size_t>(value);
}

std::int32_t checkedSilentAfter(std::int32_t value)
{
    if (value <= 0) {
        throw std::invalid_argument(
            "TCP silent threshold must be positive"
        );
    }

    return value;
}

void checkedAdd(
    std::uint64_t& target,
    std::uint64_t amount
)
{
    if (amount >
        std::numeric_limits<std::uint64_t>::max() - target) {
        throw std::overflow_error(
            "TCP activity counter exhausted"
        );
    }

    target += amount;
}

std::uint64_t elapsedMs(
    TcpActivityTracker::TimePoint now,
    TcpActivityTracker::TimePoint earlier
)
{
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<
            std::chrono::milliseconds
        >(now - earlier).count()
    );
}

} // namespace

TcpActivityTracker::TcpActivityTracker(
    const TcpConfig& config
)
    : capacity_(checkedCapacity(config.max_connections)),
      silent_after_ms_(
          checkedSilentAfter(config.silent_after_ms)
      )
{
}

void TcpActivityTracker::consume(
    const TcpPollBatch& batch,
    TimePoint now
)
{
    if (state_.last_consumed_at.has_value() &&
        now < *state_.last_consumed_at) {
        throw std::invalid_argument(
            "TCP activity time moved backwards"
        );
    }

    // 固定上限的值对象副本。
    // 完整处理成功后再提交，避免非法批次留下部分更新。
    auto next = state_;

    for (const auto& event : batch.events) {
        if (event.kind == TcpIoKind::RejectedAtCapacity) {
            checkedAdd(next.rejected_connections, 1);
            continue;
        }

        if (!event.slot || *event.slot >= capacity_) {
            throw std::logic_error(
                "TCP activity event contains an invalid slot"
            );
        }

        auto& slot = next.slots[*event.slot];

        if (event.kind == TcpIoKind::Accepted) {
            if (slot.active) {
                throw std::logic_error(
                    "TCP activity slot accepted before close"
                );
            }

            checkedAdd(next.accepted_connections, 1);

            slot = Slot{};

            slot.active = true;
            slot.connection_id =
                next.accepted_connections;

            slot.accepted_at = now;

            continue;
        }

        if (!slot.active) {
            throw std::logic_error(
                "TCP activity event refers to an empty slot"
            );
        }

        switch (event.kind) {
        case TcpIoKind::Data: {
            // 网络层实际只为正数 recv 生成 Data。
            // 这里仍明确保证空数据不会刷新活动时间。
            if (event.bytes.empty()) {
                break;
            }

            const auto count =
                static_cast<std::uint64_t>(
                    event.bytes.size()
                );

            checkedAdd(slot.bytes_received, count);
            checkedAdd(next.bytes_received, count);

            slot.last_data_at = now;
            break;
        }

        case TcpIoKind::Closed:
        case TcpIoKind::ClientError:
            checkedAdd(next.closed_connections, 1);

            if (event.kind == TcpIoKind::ClientError) {
                checkedAdd(next.client_errors, 1);
            }

            slot = Slot{};
            break;

        case TcpIoKind::Accepted:
        case TcpIoKind::RejectedAtCapacity:
            break;

        default:
            throw std::logic_error(
                "Unknown TCP activity event kind"
            );
        }
    }

    next.last_consumed_at = now;

    state_ = next;
}

TcpActivitySnapshot TcpActivityTracker::snapshot(
    TimePoint now
) const
{
    if (state_.last_consumed_at.has_value() &&
        now < *state_.last_consumed_at) {
        throw std::invalid_argument(
            "TCP activity snapshot time moved backwards"
        );
    }

    TcpActivitySnapshot result;

    result.silent_after_ms = silent_after_ms_;

    result.accepted_connections =
        state_.accepted_connections;

    result.closed_connections =
        state_.closed_connections;

    result.rejected_connections =
        state_.rejected_connections;

    result.client_errors = state_.client_errors;
    result.bytes_received = state_.bytes_received;

    result.connections.reserve(capacity_);

    for (std::size_t index = 0;
         index < capacity_;
         ++index) {
        const auto& slot = state_.slots[index];

        if (!slot.active) {
            continue;
        }

        TcpConnectionObservation observation;

        observation.slot = index;
        observation.connection_id = slot.connection_id;

        observation.connected_ms =
            elapsedMs(now, slot.accepted_at);

        observation.bytes_received =
            slot.bytes_received;

        if (slot.last_data_at.has_value()) {
            observation.last_data_age_ms =
                elapsedMs(now, *slot.last_data_at);
        }

        // 首次数据到达前，从接入时间开始计算静默。
        const auto idle_ms =
            observation.last_data_age_ms.value_or(
                observation.connected_ms
            );

        if (idle_ms >=
            static_cast<std::uint64_t>(
                silent_after_ms_
            )) {
            observation.state =
                TcpConnectionState::Silent;

            ++result.silent_count;
        } else if (slot.last_data_at.has_value()) {
            observation.state =
                TcpConnectionState::Active;

            ++result.active_count;
        } else {
            observation.state =
                TcpConnectionState::Waiting;

            ++result.waiting_count;
        }

        result.connections.push_back(observation);
    }

    result.connected_count = result.connections.size();

    return result;
}

} // namespace edge